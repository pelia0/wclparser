from collections import defaultdict
from typing import Any, Dict, List, Tuple


class CombatStateMachine:
    @classmethod
    def analyze_fight(
        cls,
        fight_meta: Dict[str, Any],
        actors: List[Dict[str, Any]],
        abilities: List[Dict[str, Any]],
        raw_events: List[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
        start_time = int(fight_meta.get("start_time", 0))
        end_time = int(fight_meta.get("end_time", start_time + 1))
        duration_ms = max(1, end_time - start_time)

        actor_map: Dict[int, Dict[str, Any]] = {}
        pet_owner_map: Dict[int, int] = {}
        for a in actors:
            aid = int(a["id"])
            actor_map[aid] = a
            if int(a.get("petOwner") or 0) > 0:
                pet_owner_map[aid] = int(a["petOwner"])

        ability_map: Dict[int, Dict[str, Any]] = {
            int(ab["gameID"]): ab for ab in abilities if "gameID" in ab
        }

        combatant_builds: Dict[str, Dict[str, Any]] = {}
        active_buffs: Dict[int, Dict[int, int]] = defaultdict(dict)
        buff_intervals: Dict[int, Dict[int, List[Tuple[int, int]]]] = defaultdict(lambda: defaultdict(list))

        damage_by_actor: Dict[int, int] = defaultdict(int)
        damage_by_spell: Dict[int, Dict[int, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: {"total": 0, "hits": 0, "crits": 0})
        )
        healing_by_actor: Dict[int, int] = defaultdict(int)
        healing_by_spell: Dict[int, Dict[int, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: {"total": 0, "overheal": 0, "hits": 0})
        )

        cast_timeline: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        interrupts: List[Dict[str, Any]] = []
        deaths: List[Dict[str, Any]] = []
        incoming_history: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

        normalized_events: List[Dict[str, Any]] = []
        replay_frames: List[Dict[str, int]] = []
        last_pos: Dict[int, Tuple[int, int, int]] = {}

        for ev in raw_events:
            abs_t = int(ev.get("timestamp", start_time))
            rel_t = max(0, min(duration_ms, abs_t - start_time))
            ev_type = str(ev.get("type", "unknown"))
            src_id = int(ev.get("sourceID", 0) or 0)
            tgt_id = int(ev.get("targetID", 0) or 0)
            ability_id = int(ev.get("abilityGameID", 0) or 0)

            if ev_type == "summon" and src_id and tgt_id:
                pet_owner_map[tgt_id] = pet_owner_map.get(src_id, src_id)

            owner_id = pet_owner_map.get(src_id, src_id)

            if ev_type == "combatantinfo" and src_id:
                combatant_builds[str(src_id)] = {
                    "spec_id": ev.get("specID", 0),
                    "talents": ev.get("talents", []),
                    "gear": ev.get("gear", []),
                    "artifact": ev.get("artifact", []),
                }
                for aura in ev.get("auras", []):
                    ab_id = int(aura.get("ability", 0) or 0)
                    if ab_id:
                        active_buffs[src_id][ab_id] = 0

            elif ev_type in ("applybuff", "refreshbuff", "applybuffstack"):
                if tgt_id and ability_id and ability_id not in active_buffs[tgt_id]:
                    active_buffs[tgt_id][ability_id] = rel_t

            elif ev_type == "removebuff":
                if tgt_id and ability_id in active_buffs[tgt_id]:
                    st = active_buffs[tgt_id].pop(ability_id)
                    if rel_t > st:
                        buff_intervals[tgt_id][ability_id].append((st, rel_t))

            elif ev_type == "cast" and src_id and ability_id:
                res_list = ev.get("classResources") or []
                res0 = res_list[0] if res_list else {}
                cast_timeline[str(owner_id)].append(
                    {
                        "t_ms": rel_t,
                        "ability_id": ability_id,
                        "target_id": tgt_id,
                        "active_buffs": sorted(active_buffs[owner_id].keys()),
                        "resource_type": int(res0.get("type", 0) or 0),
                        "resource_amount": int(res0.get("amount", 0) or 0),
                    }
                )

            elif ev_type == "damage" and src_id:
                raw_amt = int(ev.get("amount", 0) or 0)
                overkill = int(ev.get("overkill", 0) or 0)
                eff_dmg = max(0, raw_amt - overkill)
                hit_type = int(ev.get("hitType", 1) or 1)

                damage_by_actor[owner_id] += eff_dmg
                sp = damage_by_spell[owner_id][ability_id]
                sp["total"] += eff_dmg
                sp["hits"] += 1
                if hit_type == 2:
                    sp["crits"] += 1

                if tgt_id:
                    incoming_history[tgt_id].append(
                        {
                            "t_ms": rel_t,
                            "type": "damage",
                            "source_id": src_id,
                            "ability_id": ability_id,
                            "amount": raw_amt,
                        }
                    )

            elif ev_type in ("heal", "absorbed") and src_id:
                raw_heal = int(ev.get("amount", 0) or 0)
                overheal = int(ev.get("overheal", 0) or 0)
                healing_by_actor[owner_id] += raw_heal
                sp = healing_by_spell[owner_id][ability_id]
                sp["total"] += raw_heal
                sp["overheal"] += overheal
                sp["hits"] += 1
                if tgt_id:
                    incoming_history[tgt_id].append(
                        {
                            "t_ms": rel_t,
                            "type": "heal",
                            "source_id": src_id,
                            "ability_id": ability_id,
                            "amount": raw_heal,
                        }
                    )

            elif ev_type == "interrupt":
                interrupts.append(
                    {
                        "t_ms": rel_t,
                        "source_id": owner_id,
                        "target_id": tgt_id,
                        "ability_id": ability_id,
                        "interrupted_ability_id": int(ev.get("extraAbilityGameID", 0) or 0),
                    }
                )

            elif ev_type == "death" and tgt_id:
                for ab_id, st in list(active_buffs[tgt_id].items()):
                    if rel_t > st:
                        buff_intervals[tgt_id][ab_id].append((st, rel_t))
                active_buffs[tgt_id].clear()

                window_start = max(0, rel_t - 15000)
                recent = [
                    h for h in incoming_history[tgt_id] if window_start <= h["t_ms"] <= rel_t
                ]
                deaths.append(
                    {
                        "t_ms": rel_t,
                        "target_id": tgt_id,
                        "killing_ability_id": int(ev.get("killingAbilityGameID", 0) or 0),
                        "recent_events": recent,
                    }
                )

            if ev.get("x") is not None and ev.get("y") is not None and src_id:
                x_val = int(ev["x"])
                y_val = int(ev["y"])
                hp = int(ev.get("hitPoints", 100) or 100)
                max_hp = max(1, int(ev.get("maxHitPoints", 100) or 100))
                hp_pct = int((hp * 10000) // max_hp)
                prev = last_pos.get(src_id)
                if prev != (x_val, y_val, hp_pct):
                    last_pos[src_id] = (x_val, y_val, hp_pct)
                    replay_frames.append(
                        {"t": rel_t, "actor": src_id, "x": x_val, "y": y_val, "hp_pct": hp_pct}
                    )

            res_list = ev.get("classResources") or []
            res0 = res_list[0] if res_list else {}
            normalized_events.append(
                {
                    "timestamp": rel_t,
                    "type": ev_type,
                    "sourceID": src_id,
                    "targetID": tgt_id,
                    "targetInstance": int(ev.get("targetInstance", 0) or 0),
                    "abilityGameID": ability_id,
                    "hitType": int(ev.get("hitType", 0) or 0),
                    "amount": int(ev.get("amount", 0) or 0),
                    "unmitigatedAmount": int(ev.get("unmitigatedAmount", 0) or 0),
                    "absorbed": int(ev.get("absorbed", 0) or 0),
                    "overkill": int(ev.get("overkill", 0) or 0),
                    "hitPoints": int(ev.get("hitPoints", 0) or 0),
                    "maxHitPoints": int(ev.get("maxHitPoints", 0) or 0),
                    "resourceType": int(res0.get("type", 0) or 0),
                    "resourceAmount": int(res0.get("amount", 0) or 0),
                    "resourceMax": int(res0.get("max", 0) or 0),
                    "x": int(ev.get("x", 0) or 0),
                    "y": int(ev.get("y", 0) or 0),
                    "facing": int(ev.get("facing", 0) or 0),
                }
            )

        for tgt_id, ab_dict in active_buffs.items():
            for ab_id, st in ab_dict.items():
                if duration_ms > st:
                    buff_intervals[tgt_id][ab_id].append((st, duration_ms))

        buff_uptimes: Dict[str, List[Dict[str, Any]]] = {}
        for tgt_id, ab_intervals in buff_intervals.items():
            rows = []
            for ab_id, intervals in ab_intervals.items():
                covered = sum(end - st for st, end in intervals)
                uptime_pct = round(min(100.0, (covered * 100.0) / duration_ms), 2)
                rows.append(
                    {
                        "ability_id": ab_id,
                        "uptime_pct": uptime_pct,
                        "intervals": intervals,
                    }
                )
            rows.sort(key=lambda r: r["uptime_pct"], reverse=True)
            buff_uptimes[str(tgt_id)] = rows

        damage_done = []
        for aid, total_dmg in sorted(damage_by_actor.items(), key=lambda kv: kv[1], reverse=True):
            actor_info = actor_map.get(aid, {})
            if actor_info.get("type") in ("Boss", "NPC", "Enemy") or actor_info.get("subType") in ("Boss", "NPC", "Enemy"):
                continue
            spells_breakdown = [
                {"ability_id": ab_id, **stats}
                for ab_id, stats in sorted(
                    damage_by_spell[aid].items(), key=lambda kv: kv[1]["total"], reverse=True
                )
            ]
            damage_done.append(
                {
                    "actor_id": aid,
                    "total": total_dmg,
                    "dps": round((total_dmg * 1000.0) / duration_ms, 1),
                    "spells": spells_breakdown,
                }
            )

        healing_done = []
        for aid, total_heal in sorted(healing_by_actor.items(), key=lambda kv: kv[1], reverse=True):
            actor_info = actor_map.get(aid, {})
            if actor_info.get("type") in ("Boss", "NPC", "Enemy") or actor_info.get("subType") in ("Boss", "NPC", "Enemy"):
                continue
            spells_breakdown = [
                {"ability_id": ab_id, **stats}
                for ab_id, stats in sorted(
                    healing_by_spell[aid].items(), key=lambda kv: kv[1]["total"], reverse=True
                )
            ]
            healing_done.append(
                {
                    "actor_id": aid,
                    "total": total_heal,
                    "hps": round((total_heal * 1000.0) / duration_ms, 1),
                    "spells": spells_breakdown,
                }
            )

        summary_section = {
            "report_code": fight_meta.get("report_code", ""),
            "fight_id": int(fight_meta.get("fight_id", 0)),
            "encounter_id": int(fight_meta.get("encounter_id", 0)),
            "name": fight_meta.get("name", ""),
            "keystone_level": int(fight_meta.get("keystone_level", 0)),
            "affixes": fight_meta.get("affixes", []),
            "duration_ms": duration_ms,
            "actors": actors,
            "abilities": {str(k): v for k, v in ability_map.items()},
            "combatant_builds": combatant_builds,
            "damage_done": damage_done,
            "healing_done": healing_done,
            "buff_uptimes": buff_uptimes,
            "cast_timeline": dict(cast_timeline),
            "interrupts": interrupts,
            "deaths": deaths,
        }

        replay_stream = {
            "map_id": int(fight_meta.get("map_id", 0)),
            "pulls": fight_meta.get("pulls", []),
            "frames": replay_frames,
        }

        return summary_section, normalized_events, replay_stream
