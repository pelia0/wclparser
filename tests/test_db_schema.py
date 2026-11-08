from pathlib import Path
from wcl_archiver.config import (
    LEGION_MPLUS_ZONE_ID,
    LEGION_MPLUS_S5_PARTITION,
    LEGION_MPLUS_S5_DUNGEONS,
    LEGION_RAIDS,
    LEGION_SPECS,
)
from wcl_archiver.db.schema import IndexDB, MasterDictDB


def test_legion_constants_cover_season5_and_all_5_raids():
    assert LEGION_MPLUS_ZONE_ID == 9
    assert LEGION_MPLUS_S5_PARTITION == 5
    assert len(LEGION_MPLUS_S5_DUNGEONS) == 13
    assert set(LEGION_RAIDS.keys()) == {10, 11, 12, 13, 17}
    assert len(LEGION_SPECS) == 36


def test_index_db_deduplicates_report_fight_and_tracks_participants(tmp_path: Path):
    db = IndexDB(tmp_path / "index.db")
    inserted1 = db.enqueue_fight(
        report_code="aBcDeFg123",
        fight_id=4,
        content_type="mplus",
        zone_id=9,
        partition=5,
        encounter_id=11492,
        encounter_slug="maw-of-souls",
        keystone_level=27,
        difficulty=10,
        affixes=[10, 6, 3, 13],
        duration_ms=852000,
        participant={"name": "Pellia", "class": "Monk", "spec": "Windwalker", "role": "dps", "ilvl": 972},
    )
    inserted2 = db.enqueue_fight(
        report_code="aBcDeFg123",
        fight_id=4,
        content_type="mplus",
        zone_id=9,
        partition=5,
        encounter_id=11492,
        encounter_slug="maw-of-souls",
        keystone_level=27,
        difficulty=10,
        affixes=[10, 6, 3, 13],
        duration_ms=852000,
        participant={"name": "Tankbro", "class": "DeathKnight", "spec": "Blood", "role": "tank", "ilvl": 975},
    )
    assert inserted1 is True
    assert inserted2 is False

    fights = db.search_fights(content_type="mplus", encounter_id=11492, spec="Windwalker")
    assert len(fights) == 1
    assert fights[0]["report_code"] == "aBcDeFg123"
    assert fights[0]["fight_id"] == 4
    assert len(fights[0]["participants"]) == 2
    assert fights[0]["affixes"] == [10, 6, 3, 13]

    # Test fight with NULL affixes_json
    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO fights (
                fight_uid, report_code, fight_id, content_type, zone_id, partition,
                encounter_id, encounter_slug, keystone_level, difficulty, affixes_json
            ) VALUES ('null_affix_1', 'nullRep', 1, 'raid', 10, 1, 1853, 'nythendra', 0, 15, NULL)
            """
        )
    null_affix_fights = db.search_fights(encounter_id=1853)
    assert len(null_affix_fights) == 1
    assert null_affix_fights[0]["affixes"] == []


def test_master_dict_db_stores_spells(tmp_path: Path):
    md = MasterDictDB(tmp_path / "master_dict.db")
    md.upsert_spell(113656, "Fists of Fury", 4, "monk_fistsoffury.jpg")
    spell = md.get_spell(113656)
    assert spell["name"] == "Fists of Fury"
    assert spell["school"] == 4
