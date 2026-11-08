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
            resp.raise_for_status()
            return resp.json()

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
