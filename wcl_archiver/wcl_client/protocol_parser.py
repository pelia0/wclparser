from typing import Any, Dict, List, Optional
import httpx
from wcl_archiver.wcl_client.auth import WCLAuthManager


class WCLProtocolClient:
    BASE_URL = "https://www.warcraftlogs.com"

    def __init__(
        self,
        auth_manager: WCLAuthManager,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        self.auth_manager = auth_manager
        self._transport = transport

    def _make_client(self) -> httpx.AsyncClient:
        headers, cookies = self.auth_manager.get_headers_and_cookies()
        return httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers=headers,
            cookies=cookies,
            transport=self._transport,
            follow_redirects=True,
            timeout=45.0,
        )

    async def unarchive_report(self, report_code: str) -> bool:
        async with self._make_client() as client:
            resp = await client.post(f"/reports/unarchive/{report_code}")
            return resp.status_code == 200

    async def fetch_report_master_and_fights(self, report_code: str) -> Dict[str, Any]:
        async with self._make_client() as client:
            resp = await client.get(f"/reports/fights-and-participants/{report_code}/0")
            if resp.status_code == 200:
                return resp.json()

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
        report = ((gql_resp.json().get("data") or {}).get("reportData") or {}).get("report") or {}
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
        all_events: List[Dict[str, Any]] = []
        cursor: Optional[int] = int(start_time)

        async with self._make_client() as client:
            while cursor is not None and cursor < end_time:
                url = (
                    f"/reports/events/summary/{report_code}/{fight_id}/{cursor}/{end_time}"
                    f"/0/0/Any/0/-1.0.-1.0/0?includeResources=true"
                )
                resp = await client.get(url)
                if resp.status_code != 200 and self.auth_manager.get_auth_status().get("has_oauth_token"):
                    return await self._fetch_all_fight_events_graphql(
                        client, report_code, fight_id, start_time, end_time
                    )
                resp.raise_for_status()
                data = resp.json()
                batch = data.get("events", [])
                all_events.extend(batch)
                next_ts = data.get("nextPageTimestamp")
                if next_ts is not None and int(next_ts) > cursor:
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
        while cursor is not None and cursor < end_time:
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
            ev_obj = (
                (((gql_resp.json().get("data") or {}).get("reportData") or {}).get("report") or {})
                .get("events")
                or {}
            )
            batch = ev_obj.get("data") or []
            all_events.extend(batch)
            next_ts = ev_obj.get("nextPageTimestamp")
            if next_ts is not None and int(next_ts) > cursor:
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
    ) -> List[Dict[str, Any]]:
        query = """
        query($encounterId: Int!, $page: Int!, $partition: Int!, $className: String, $specName: String) {
          worldData {
            encounter(id: $encounterId) {
              fightRankings(page: $page, partition: $partition, className: $className, specName: $specName)
            }
          }
        }
        """
        async with self._make_client() as client:
            resp = await client.post(
                "/api/v2/client",
                json={
                    "query": query,
                    "variables": {
                        "encounterId": encounter_id,
                        "page": page,
                        "partition": partition,
                        "className": class_name,
                        "specName": spec_name,
                    },
                },
            )
            if resp.status_code != 200:
                return []
            payload = resp.json()
            data = payload.get("data") or {}
            world_data = data.get("worldData") or {}
            encounter = world_data.get("encounter") or {}
            fight_rankings = encounter.get("fightRankings") or {}
            rankings = fight_rankings.get("rankings") or []
            return rankings
