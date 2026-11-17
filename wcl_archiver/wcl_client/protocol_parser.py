import re
import json
import asyncio
import math
import time
from collections import OrderedDict
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional
import httpx
from wcl_archiver.wcl_client.auth import WCLAuthManager

WEB_REPORT_FIGHT_RE = re.compile(
    r"/reports/([A-Za-z0-9]{10,20})(?:#|\?[^\"']*?)fight=(\d+)"
)


class WCLProtocolClient:
    BASE_URL = "https://www.warcraftlogs.com"

    def __init__(
        self,
        auth_manager: WCLAuthManager,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        raw_archive=None,
    ):
        self.auth_manager = auth_manager
        self._transport = transport
        # Load HTTPX's normal environment-aware CA trust once, before requests.
        # Request clients retain separate lifetimes without blocking the loop on
        # repeated synchronous trust-store initialization for every page/report.
        self._ssl_context = httpx.create_ssl_context()
        self.raw_archive = raw_archive
        self._report_master_cache = OrderedDict()
        self._quota_until = 0.0
        self._quota_wait_lock = asyncio.Lock()

    async def _capture_response(self, response):
        if self.raw_archive is None:
            return
        await response.aread()
        request_json = None
        if response.request.url.path in ('/api/v2/client', '/api/v2/user') and response.request.content:
            request_json = json.loads(response.request.content)
        self.raw_archive.capture(response.request.method, str(response.request.url),
                                 response.content, status=response.status_code,
                                 request_json=request_json)

    @staticmethod
    def _json_payload(response, context):
        if '_wcl_payload' in response.extensions:
            return response.extensions['_wcl_payload']
        try:
            payload = response.json(parse_float=Decimal)
        except (ValueError, UnicodeError):
            raise RuntimeError(f'Invalid {context} JSON response') from None
        if not isinstance(payload, dict):
            raise RuntimeError(f'Invalid {context} response')
        return payload

    async def _graphql_post(self, client, query, variables):
        # Every query carries application quota data; all workers share one gate.
        if not re.search(r'\brateLimitData\b', query):
            opening = query.index('{') + 1
            query = (query[:opening] + '\n rateLimitData { limitPerHour pointsSpentThisHour pointsResetIn }\n'
                     + query[opening:])
        for attempt in range(3):
            await self._wait_for_quota()
            # Resolve after waiting and for each page/retry: refresh supersedes
            # headers copied when this HTTP client was opened.
            endpoint, token = await self.auth_manager.get_api_auth()
            response = await client.post(
                endpoint, json={'query': query, 'variables': variables},
                headers={'Authorization': f'Bearer {token}' if token else ''})
            if response.status_code == 429:
                delay = self._retry_after(response.headers.get('Retry-After', ''))
                self._quota_until = max(self._quota_until, time.monotonic() + delay)
                if attempt == 2:
                    raise RuntimeError('WarcraftLogs API quota remained exhausted after bounded retries')
                continue
            if response.status_code == 200:
                try:
                    payload = response.json(parse_float=Decimal)
                except (ValueError, UnicodeError):
                    payload = None  # The caller reports its contextual JSON error.
                if isinstance(payload, dict):
                    response.extensions['_wcl_payload'] = payload
                data = payload.get('data') if isinstance(payload, dict) else None
                quota = data.get('rateLimitData') if isinstance(data, dict) else None
                if isinstance(quota, dict):
                    try:
                        limit = float(quota.get('limitPerHour', 0))
                        spent = float(quota.get('pointsSpentThisHour', 0))
                        delay = float(quota.get('pointsResetIn', 0))
                    except (TypeError, ValueError, OverflowError):
                        pass
                    else:
                        if limit > 0 and spent >= limit and math.isfinite(delay) and delay > 0:
                            self._quota_until = max(self._quota_until, time.monotonic() + delay)
            return response

    async def _wait_for_quota(self):
        # One waiter sleeps, other workers wait on the gate rather than spinning
        # independently. Known cooldowns pause work without consuming queued
        # reports' failure attempts; task cancellation interrupts either wait.
        async with self._quota_wait_lock:
            while (delay := self._quota_until - time.monotonic()) > 0:
                await asyncio.sleep(min(delay, 30.0))

    def get_quota_status(self):
        return {'api_quota_wait_seconds': math.ceil(max(0, self._quota_until - time.monotonic()))}

    @staticmethod
    def _retry_after(value):
        try:
            delay = float(value)
        except (TypeError, ValueError):
            try:
                delay = parsedate_to_datetime(value).timestamp() - time.time()
            except (TypeError, ValueError, OverflowError):
                return 1.0
        return max(0.0, delay) if math.isfinite(delay) else 1.0

    def _has_user_auth(self):
        status = self.auth_manager.get_auth_status()
        return bool(status.get('has_user_oauth_token') or status.get('oauth_mode') == 'user')

    def _cached_master(self, report_code):
        cached = self._report_master_cache.get(report_code)
        has_source = getattr(self.raw_archive, 'has_report_metadata', None)
        if cached is not None and callable(has_source) and has_source(report_code):
            self._report_master_cache.move_to_end(report_code)
            return cached
        return None

    @staticmethod
    def _next_cursor(value, cursor, end_time):
        if value is None:
            return None
        integer = type(value) is int or (isinstance(value, Decimal) and value.is_finite()
                                       and value == value.to_integral_value())
        if not integer or not cursor < value <= end_time:
            raise RuntimeError('Invalid event pagination cursor: must advance within fight bounds')
        return int(value)

    def _make_client(self) -> httpx.AsyncClient:
        headers, cookies = self.auth_manager.get_headers_and_cookies()
        return httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers=headers,
            cookies=cookies,
            transport=self._transport,
            verify=self._ssl_context,
            follow_redirects=True,
            timeout=45.0,
            event_hooks={'response':[self._capture_response]},
        )

    async def unarchive_report(self, report_code: str) -> bool:
        if self._has_user_auth():
            # A subscribed user's API access is the archive accessibility check.
            # Denial propagates; never silently downgrade to a browser session.
            await self.fetch_report_master_and_fights(report_code)
            return True
        async with self._make_client() as client:
            resp = await client.post(f"/reports/unarchive/{report_code}")
            return resp.status_code == 200

    async def fetch_report_master_and_fights(self, report_code: str) -> Dict[str, Any]:
        async with self._make_client() as client:
            if self._has_user_auth():
                cached = self._cached_master(report_code)
                if cached is not None:
                    return cached
                metadata = await self._fetch_report_master_graphql(client, report_code)
                self._report_master_cache[report_code] = metadata
                self._report_master_cache.move_to_end(report_code)
                while len(self._report_master_cache) > 128:
                    self._report_master_cache.popitem(last=False)
                return metadata
            resp = await client.get(f"/reports/fights-and-participants/{report_code}/0")
            if resp.status_code == 200:
                payload = self._json_payload(resp, 'report metadata')
                if not isinstance(payload,dict) or not isinstance(payload.get('fights'),list):
                    raise RuntimeError('Invalid report metadata response')
                return payload

            if self.auth_manager.get_auth_status().get("has_oauth_token"):
                return await self._fetch_report_master_graphql(client, report_code)

            resp.raise_for_status()
            return resp.json()

    async def _fetch_report_master_graphql(
        self, client: httpx.AsyncClient, report_code: str
    ) -> Dict[str, Any]:
        query = """
        query($code: String!) {
          reportData {
            report(code: $code) {
              archiveStatus { isArchived isAccessible }
              fights {
                id
                startTime
                endTime
                encounterID
                name
                keystoneLevel
                keystoneAffixes
                difficulty
                maps { id }
              }
              masterData {
                logVersion
                gameVersion
                lang
                abilities {
                  gameID
                  name
                  type
                }
                actors {
                  id
                  name
                  type
                  subType
                  petOwner
                  gameID
                  server
                }
              }
            }
          }
        }
        """
        gql_resp = await self._graphql_post(client, query, {'code': report_code})
        gql_resp.raise_for_status()
        payload = self._json_payload(gql_resp, 'GraphQL report metadata')
        data = payload.get('data')
        report_data = data.get('reportData') if isinstance(data, dict) else None
        report = report_data.get('report') if isinstance(report_data, dict) else None
        if (payload.get('errors') or not isinstance(report,dict)
                or not isinstance(report.get('fights'),list)
                or not isinstance(report.get('masterData'),dict)
                or not isinstance(report['masterData'].get('actors'),list)
                or not isinstance(report['masterData'].get('abilities'),list)):
            raise RuntimeError('Incomplete GraphQL report metadata')
        if any(not isinstance(item, dict) for records in (
                report['fights'], report['masterData']['actors'], report['masterData']['abilities'])
               for item in records):
            raise RuntimeError('Invalid GraphQL report metadata records')
        archive_status = report.get('archiveStatus')
        if (isinstance(archive_status, dict) and archive_status.get('isArchived') is True
                and archive_status.get('isAccessible') is False):
            raise RuntimeError('WarcraftLogs archived report is not accessible to this account')
        fights = [
            {
                "id": f.get("id"),
                "start_time": f.get("startTime", 0),
                "end_time": f.get("endTime", 0),
                "boss": f.get("encounterID", 0),
                "name": f.get("name", ""),
                "keystoneLevel": f.get("keystoneLevel") or 0,
                "affixes": f.get("keystoneAffixes") or [],
                "difficulty": f.get("difficulty") or 0,
                "maps": f.get("maps") or [0],
            }
            for f in (report.get("fights") or [])
        ]
        master_data = report.get("masterData") or {}
        friendlies: List[Dict[str, Any]] = []
        enemies: List[Dict[str, Any]] = []
        for actor in master_data.get("actors") or []:
            if actor.get("type") == "Player":
                friendlies.append(
                    {
                        **actor,
                        "id": actor.get("id"),
                        "name": actor.get("name", ""),
                        "type": actor.get("subType", "Player"),
                        "icon": actor.get("subType", "Player"),
                    }
                )
            else:
                enemies.append(
                    {
                        **actor,
                        "id": actor.get("id"),
                        "name": actor.get("name", ""),
                        "type": actor.get("type", "NPC"),
                    }
                )
        return {
            "fights": fights,
            "friendlies": friendlies,
            "enemies": enemies,
            "abilities": master_data.get("abilities") or [],
        }

    async def fetch_all_fight_events(
        self,
        report_code: str,
        fight_id: int,
        start_time: int,
        end_time: int,
    ) -> List[Dict[str, Any]]:
        if self.raw_archive is None:
            return await self._fetch_all_fight_events_impl(report_code,fight_id,start_time,end_time)
        with self.raw_archive.fight_capture(report_code,fight_id) as fetch_id:
            events = await self._fetch_all_fight_events_impl(report_code,fight_id,start_time,end_time)
            self.raw_archive.complete_fight(report_code,fight_id,fetch_id,start_time,end_time,len(events))
            return events

    async def _fetch_all_fight_events_impl(self, report_code, fight_id, start_time, end_time):
        all_events: List[Dict[str, Any]] = []
        cursor: Optional[int] = int(start_time)

        async with self._make_client() as client:
            if self._has_user_auth():
                return await self._fetch_all_fight_events_graphql(
                    client, report_code, fight_id, start_time, end_time)
            while cursor is not None and cursor <= end_time:
                url = (
                    f"/reports/events/summary/{report_code}/{fight_id}/{cursor}/{end_time}"
                    f"/0/0/Any/0/-1.0.-1.0/0?includeResources=true"
                )
                resp = await client.get(url)
                if resp.status_code != 200 and self.auth_manager.get_auth_status().get("has_oauth_token"):
                    tail = await self._fetch_all_fight_events_graphql(
                        client, report_code, fight_id, cursor, end_time
                    )
                    return all_events + tail
                resp.raise_for_status()
                data = self._json_payload(resp, 'canonical event page')
                if not isinstance(data,dict) or not isinstance(data.get('events'),list):
                    raise RuntimeError('Invalid canonical event page')
                batch = data.get("events", [])
                all_events.extend(batch)
                next_ts = data.get("nextPageTimestamp")
                cursor = self._next_cursor(next_ts, cursor, end_time)

        return all_events

    async def _fetch_all_fight_events_graphql(
        self,
        client: httpx.AsyncClient,
        report_code: str,
        fight_id: int,
        start_time: int,
        end_time: int,
    ) -> List[Dict[str, Any]]:
        query = """
        query($code: String!, $fightId: Int!, $startTime: Float!, $endTime: Float!) {
          reportData {
            report(code: $code) {
              events(fightIDs: [$fightId], startTime: $startTime, endTime: $endTime, includeResources: true, limit: 20000) {
                data
                nextPageTimestamp
              }
            }
          }
        }
        """
        all_events: List[Dict[str, Any]] = []
        cursor: Optional[int] = int(start_time)
        while cursor is not None and cursor <= end_time:
            gql_resp = await self._graphql_post(client, query, {
                        "code": report_code,
                        "fightId": int(fight_id),
                        "startTime": float(cursor),
                        "endTime": float(end_time),
                    })
            gql_resp.raise_for_status()
            payload = self._json_payload(gql_resp, 'GraphQL event page')
            if payload.get('errors'):
                raise RuntimeError('GraphQL event response contains errors')
            data = payload.get('data')
            report_data = data.get('reportData') if isinstance(data, dict) else None
            report = report_data.get('report') if isinstance(report_data, dict) else None
            ev_obj = report.get('events') if isinstance(report, dict) else None
            if not isinstance(ev_obj, dict) or not isinstance(ev_obj.get('data'),list):
                raise RuntimeError('Invalid GraphQL event page')
            batch = ev_obj['data']
            if any(not isinstance(event, dict) for event in batch):
                raise RuntimeError('Invalid GraphQL event records')
            all_events.extend(batch)
            next_ts = ev_obj.get("nextPageTimestamp")
            cursor = self._next_cursor(next_ts, cursor, end_time)
        return all_events

    async def fetch_rankings_fight_refs(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str = "Any",
        spec_name: str = "Any",
        page: int = 1,
        difficulty: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if class_name and class_name != "Any":
            if difficulty and int(difficulty) > 0:
                query = """
                query($encounterId: Int!, $page: Int!, $partition: Int!, $className: String, $specName: String, $difficulty: Int) {
                  worldData {
                    encounter(id: $encounterId) {
                      characterRankings(page: $page, partition: $partition, className: $className, specName: $specName, difficulty: $difficulty)
                    }
                  }
                }
                """
            else:
                query = """
                query($encounterId: Int!, $page: Int!, $partition: Int!, $className: String, $specName: String) {
                  worldData {
                    encounter(id: $encounterId) {
                      characterRankings(page: $page, partition: $partition, className: $className, specName: $specName)
                    }
                  }
                }
                """
            variables: Dict[str, Any] = {
                "encounterId": encounter_id,
                "page": page,
                "partition": partition,
                "className": class_name,
                "specName": spec_name,
            }
            if difficulty and int(difficulty) > 0:
                variables["difficulty"] = int(difficulty)
        else:
            if difficulty and int(difficulty) > 0:
                query = """
                query($encounterId: Int!, $page: Int!, $partition: Int!, $difficulty: Int) {
                  worldData {
                    encounter(id: $encounterId) {
                      fightRankings(page: $page, partition: $partition, difficulty: $difficulty)
                    }
                  }
                }
                """
            else:
                query = """
                query($encounterId: Int!, $page: Int!, $partition: Int!) {
                  worldData {
                    encounter(id: $encounterId) {
                      fightRankings(page: $page, partition: $partition)
                    }
                  }
                }
                """
            variables = {
                "encounterId": encounter_id,
                "page": page,
                "partition": partition,
            }
            if difficulty and int(difficulty) > 0:
                variables["difficulty"] = int(difficulty)

        async with self._make_client() as client:
            resp = await self._graphql_post(client, query, variables)
            if resp.status_code in (401, 403):
                auth_st = self.auth_manager.get_auth_status()
                if (auth_st.get("has_session_cookie") and not auth_st.get("has_oauth_token")
                        and not self._has_user_auth()):
                    web_refs = await self._fetch_rankings_web_table_fallback(
                        client, zone_id, encounter_id, partition, class_name, spec_name, page, difficulty=difficulty
                    )
                    if web_refs is not None:
                        return web_refs
                raise RuntimeError(
                    f"WarcraftLogs API v2 rejected the rankings request (HTTP {resp.status_code}). "
                    "Check your WCL API authorization and account access."
                )
            if resp.status_code != 200:
                resp.raise_for_status()
                raise RuntimeError('Unexpected rankings status')
            payload = self._json_payload(resp, 'GraphQL rankings')
            if payload.get("errors"):
                raise RuntimeError('WarcraftLogs API v2 GraphQL rankings response contains errors')
            data = payload.get("data") or {}
            world_data = data.get("worldData") or {}
            encounter = world_data.get("encounter") or {}
            fight_rankings = (
                encounter.get("characterRankings")
                or encounter.get("fightRankings")
                or {}
            )
            rankings = fight_rankings.get("rankings") or []
            return rankings

    async def _fetch_rankings_web_table_fallback(
        self,
        client: httpx.AsyncClient,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str,
        spec_name: str,
        page: int,
        difficulty: Optional[int] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        metric = "leaderboards" if zone_id == 9 else "dps"
        diff = 0 if zone_id == 9 else (int(difficulty) if difficulty and int(difficulty) > 0 else 5)
        url = (
            f"/zone/rankings-table/{zone_id}/{metric}/0/{diff}/0/{encounter_id}"
            f"/0/{partition}/{class_name}/{spec_name}/0/0/0/?page={page}&search="
        )
        web_resp = await client.get(url)
        if web_resp.status_code in (401, 403) or "just a moment" in web_resp.text.lower():
            return None
        if web_resp.status_code != 200:
            web_resp.raise_for_status()
            raise RuntimeError('Unexpected rankings status')
        refs: List[Dict[str, Any]] = []
        seen = set()
        for code, fid in WEB_REPORT_FIGHT_RE.findall(web_resp.text):
            key = (code, int(fid))
            if key not in seen:
                seen.add(key)
                refs.append({"report": {"code": code, "fightID": int(fid)}})
        return refs
