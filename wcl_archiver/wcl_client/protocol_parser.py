import re
import json
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
        self.raw_archive = raw_archive

    async def _capture_response(self, response):
        if self.raw_archive is None:
            return
        await response.aread()
        request_json = None
        if response.request.url.path == '/api/v2/client' and response.request.content:
            request_json = json.loads(response.request.content)
        self.raw_archive.capture(response.request.method, str(response.request.url),
                                 response.content, status=response.status_code,
                                 request_json=request_json)

    def _make_client(self) -> httpx.AsyncClient:
        headers, cookies = self.auth_manager.get_headers_and_cookies()
        return httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers=headers,
            cookies=cookies,
            transport=self._transport,
            follow_redirects=True,
            timeout=45.0,
            event_hooks={'response':[self._capture_response]},
        )

    async def unarchive_report(self, report_code: str) -> bool:
        async with self._make_client() as client:
            resp = await client.post(f"/reports/unarchive/{report_code}")
            return resp.status_code == 200

    async def fetch_report_master_and_fights(self, report_code: str) -> Dict[str, Any]:
        async with self._make_client() as client:
            resp = await client.get(f"/reports/fights-and-participants/{report_code}/0")
            if resp.status_code == 200:
                payload = resp.json()
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
                }
              }
            }
          }
        }
        """
        gql_resp = await client.post(
            "/api/v2/client",
            json={"query": query, "variables": {"code": report_code}},
        )
        gql_resp.raise_for_status()
        payload = gql_resp.json()
        report = ((payload.get("data") or {}).get("reportData") or {}).get("report")
        if (payload.get('errors') or not isinstance(report,dict)
                or not isinstance(report.get('fights'),list)
                or not isinstance(report.get('masterData'),dict)
                or not isinstance(report['masterData'].get('actors'),list)
                or not isinstance(report['masterData'].get('abilities'),list)):
            raise RuntimeError('Incomplete GraphQL report metadata')
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
                        "id": actor.get("id"),
                        "name": actor.get("name", ""),
                        "type": actor.get("subType", "Player"),
                        "icon": actor.get("subType", "Player"),
                    }
                )
            else:
                enemies.append(
                    {
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
                data = resp.json()
                if not isinstance(data,dict) or not isinstance(data.get('events'),list):
                    raise RuntimeError('Invalid canonical event page')
                batch = data.get("events", [])
                all_events.extend(batch)
                next_ts = data.get("nextPageTimestamp")
                if next_ts is not None:
                    if int(next_ts) <= cursor:
                        raise RuntimeError('Event pagination cursor did not advance')
                    cursor = int(next_ts)
                else:
                    cursor = None

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
              events(fightIDs: [$fightId], startTime: $startTime, endTime: $endTime, includeResources: true, limit: 10000) {
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
            gql_resp = await client.post(
                "/api/v2/client",
                json={
                    "query": query,
                    "variables": {
                        "code": report_code,
                        "fightId": int(fight_id),
                        "startTime": float(cursor),
                        "endTime": float(end_time),
                    },
                },
            )
            gql_resp.raise_for_status()
            payload = gql_resp.json()
            if payload.get('errors'):
                raise RuntimeError('GraphQL event response contains errors')
            ev_obj = (
                (((payload.get("data") or {}).get("reportData") or {}).get("report") or {})
                .get("events")
                or {}
            )
            if not isinstance(ev_obj.get('data'),list):
                raise RuntimeError('Invalid GraphQL event page')
            batch = ev_obj['data']
            all_events.extend(batch)
            next_ts = ev_obj.get("nextPageTimestamp")
            if next_ts is not None:
                if int(next_ts) <= cursor:
                    raise RuntimeError('Event pagination cursor did not advance')
                cursor = int(next_ts)
            else:
                cursor = None
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
            resp = await client.post(
                "/api/v2/client",
                json={"query": query, "variables": variables},
            )
            if resp.status_code in (401, 403):
                auth_st = self.auth_manager.get_auth_status()
                if auth_st.get("has_session_cookie") and not auth_st.get("has_oauth_token"):
                    web_refs = await self._fetch_rankings_web_table_fallback(
                        client, zone_id, encounter_id, partition, class_name, spec_name, page, difficulty=difficulty
                    )
                    if web_refs is not None:
                        return web_refs
                raise RuntimeError(
                    f"WarcraftLogs API v2 rejected the rankings request (HTTP {resp.status_code}). "
                    "To scan rankings, please provide a valid API v2 Client ID and Client Secret "
                    "(create free in 10 seconds at https://www.warcraftlogs.com/api/clients) "
                    "in Section 1 above and click 'Sign In & Save Session'."
                )
            if resp.status_code != 200:
                resp.raise_for_status()
                raise RuntimeError('Unexpected rankings status')
            payload = resp.json()
            if payload.get("errors"):
                err_msg = payload["errors"][0].get("message", "Unknown GraphQL error")
                raise RuntimeError(f"WarcraftLogs API v2 GraphQL error: {err_msg}")
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
