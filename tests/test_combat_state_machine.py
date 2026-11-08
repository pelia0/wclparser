from wcl_archiver.parser.combat_state_machine import CombatStateMachine


def test_combat_state_machine_pet_merging_auras_casts_deaths_and_2d_replay():
    fight_meta = {
        "report_code": "aBcDeFg123",
        "fight_id": 2,
        "start_time": 1000,
        "end_time": 21000,
        "encounter_id": 11492,
        "name": "Maw of Souls",
        "keystone_level": 27,
        "affixes": [10, 6, 3, 13],
        "map_id": 1492,
    }
    actors = [
        {"id": 1, "name": "Pellia", "type": "Player", "subType": "Monk"},
        {"id": 50, "name": "Storm Spirit", "type": "Pet", "petOwner": 1},
        {"id": 99, "name": "Helya", "type": "Boss", "subType": "Boss"},
    ]
    abilities = [
        {"gameID": 137639, "name": "Storm, Earth, and Fire", "type": 1},
        {"gameID": 113656, "name": "Fists of Fury", "type": 1},
        {"gameID": 227255, "name": "Tainted Essence", "type": 32},
        {"gameID": 116705, "name": "Spear Hand Strike", "type": 1},
    ]

    raw_events = [
        {
            "timestamp": 1000,
            "type": "combatantinfo",
            "sourceID": 1,
            "specID": 269,
            "talents": [{"id": 196719}, {"id": 115098}],
            "gear": [{"id": 137057, "itemLevel": 1000}, {"id": 151811, "itemLevel": 1000}],
            "auras": [{"ability": 137639, "source": 1, "stacks": 1}],
        },
        {"timestamp": 2000, "type": "begincast", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "x": -41000, "y": 28000, "hitPoints": 100, "maxHitPoints": 100},
        {"timestamp": 2000, "type": "cast", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "x": -41000, "y": 28000, "hitPoints": 100, "maxHitPoints": 100, "classResources": [{"type": 12, "amount": 2, "max": 5}]},
        {"timestamp": 3000, "type": "damage", "sourceID": 1, "targetID": 99, "abilityGameID": 113656, "amount": 500000, "overkill": 0, "hitType": 2, "x": -41010, "y": 28005, "hitPoints": 100, "maxHitPoints": 100},
        {"timestamp": 3100, "type": "damage", "sourceID": 50, "targetID": 99, "abilityGameID": 113656, "amount": 350000, "overkill": 50000, "hitType": 1},
        {"timestamp": 11000, "type": "removebuff", "sourceID": 1, "targetID": 1, "abilityGameID": 137639},
        {"timestamp": 12000, "type": "interrupt", "sourceID": 1, "targetID": 99, "abilityGameID": 116705, "extraAbilityGameID": 227255},
        {"timestamp": 18500, "type": "damage", "sourceID": 99, "targetID": 1, "abilityGameID": 227255, "amount": 6000000, "x": -41050, "y": 28020, "hitPoints": 0, "maxHitPoints": 100},
        {"timestamp": 18510, "type": "death", "targetID": 1, "killingAbilityGameID": 227255},
    ]

    summary, norm_events, replay = CombatStateMachine.analyze_fight(
        fight_meta, actors, abilities, raw_events
    )

    dmg_row = summary["damage_done"][0]
    assert dmg_row["actor_id"] == 1
    assert dmg_row["total"] == 800000

    auras = summary["buff_uptimes"]["1"]
    sef_aura = next(a for a in auras if a["ability_id"] == 137639)
    assert sef_aura["uptime_pct"] == 50.0

    casts = summary["cast_timeline"]["1"]
    assert len(casts) == 1
    assert casts[0]["ability_id"] == 113656
    assert 137639 in casts[0]["active_buffs"]

    assert len(summary["interrupts"]) == 1
    assert summary["interrupts"][0]["interrupted_ability_id"] == 227255
    assert len(summary["deaths"]) == 1
    assert summary["deaths"][0]["target_id"] == 1
    assert len(summary["deaths"][0]["recent_events"]) >= 1

    assert replay["map_id"] == 1492
    assert len(replay["frames"]) >= 2
    zero_hp_frames = [f for f in replay["frames"] if f["actor"] == 99 and f["x"] == -41050]
    assert len(zero_hp_frames) == 1
    assert zero_hp_frames[0]["hp_pct"] == 0


def test_combat_state_machine_dynamic_summon_healing_and_remaining_buffs():
    fight_meta = {
        "report_code": "xYz987",
        "fight_id": 1,
        "start_time": 5000,
        "end_time": 15000,
        "encounter_id": 2050,
        "name": "Argus the Unmaker",
        "keystone_level": 0,
        "affixes": [],
        "map_id": 1712,
    }
    actors = [
        {"id": 2, "name": "Healer", "type": "Player", "subType": "Priest"},
        {"id": 3, "name": "Tank", "type": "Player", "subType": "Paladin"},
    ]
    abilities = [
        {"gameID": 17, "name": "Power Word: Shield", "type": 2},
        {"gameID": 2061, "name": "Flash Heal", "type": 2},
    ]

    raw_events = [
        {"timestamp": 5000, "type": "summon", "sourceID": 2, "targetID": 88},
        {"timestamp": 6000, "type": "applybuff", "sourceID": 2, "targetID": 3, "abilityGameID": 17},
        {"timestamp": 7000, "type": "heal", "sourceID": 88, "targetID": 3, "abilityGameID": 2061, "amount": 300000, "overheal": 50000},
        {"timestamp": 8000, "type": "absorbed", "sourceID": 2, "targetID": 3, "abilityGameID": 17, "amount": 200000},
    ]

    summary, norm_events, replay = CombatStateMachine.analyze_fight(
        fight_meta, actors, abilities, raw_events
    )

    heal_row = summary["healing_done"][0]
    assert heal_row["actor_id"] == 2
    assert heal_row["total"] == 500000
    assert heal_row["hps"] == 50000.0

    uptimes = summary["buff_uptimes"]["3"]
    pws = next(a for a in uptimes if a["ability_id"] == 17)
    assert pws["uptime_pct"] == 90.0


def test_combat_state_machine_handles_null_coordinates_resources_and_zero_pet_owner():
    fight_meta = {
        "report_code": "nullTest123",
        "fight_id": 1,
        "start_time": 1000,
        "end_time": 5000,
        "encounter_id": 100,
        "name": "Test Nulls",
    }
    actors = [
        {"id": 10, "name": "PlayerOne", "type": "Player", "subType": "Warrior", "petOwner": 0},
    ]
    abilities = [
        {"gameID": 1001, "name": "Mortal Strike", "type": 1},
    ]
    raw_events = [
        {
            "timestamp": 2000,
            "type": "cast",
            "sourceID": 10,
            "targetID": 999,
            "abilityGameID": 1001,
            "x": None,
            "y": None,
            "classResources": [{"type": None, "amount": None}],
        },
        {
            "timestamp": 2500,
            "type": "damage",
            "sourceID": 10,
            "targetID": 999,
            "abilityGameID": 1001,
            "amount": 100000,
            "x": None,
            "y": None,
        },
    ]

    summary, norm_events, replay = CombatStateMachine.analyze_fight(
        fight_meta, actors, abilities, raw_events
    )

    # Actor with petOwner=0 should remain owner 10, not 0
    assert summary["damage_done"][0]["actor_id"] == 10
    casts = summary["cast_timeline"]["10"]
    assert len(casts) == 1
    assert casts[0]["resource_type"] == 0
    assert casts[0]["resource_amount"] == 0

    # Null coordinates should not populate replay frames and normalized x, y should default to 0
    assert len(replay["frames"]) == 0
    assert norm_events[0]["x"] == 0
    assert norm_events[0]["y"] == 0

