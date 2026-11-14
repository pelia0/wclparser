from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Dict, List, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

LEGION_MPLUS_ZONE_ID = 9
# Partition 5 in WCL Zone 9 = Patch 7.3.2 / 7.3.5 (Season 5, before BfA 8.0 Pre-Patch Partition 6)
LEGION_MPLUS_S5_PARTITION = 5

LEGION_MPLUS_S5_DUNGEONS: Dict[int, str] = {
    11501: "black-rook-hold",
    11677: "cathedral-of-eternal-night",
    11571: "court-of-stars",
    11466: "darkheart-thicket",
    11456: "eye-of-azshara",
    11477: "halls-of-valor",
    11492: "maw-of-souls",
    11458: "neltharions-lair",
    11651: "return-to-karazhan-lower",
    11652: "return-to-karazhan-upper",
    11753: "seat-of-the-triumvirate",
    11516: "the-arcway",
    11493: "vault-of-the-wardens",
}

LEGION_RAIDS: Dict[int, Dict] = {
    10: {
        "slug": "emerald-nightmare",
        "name": "The Emerald Nightmare",
        "partitions": [1, 2],
        "bosses": {
            1853: "nythendra",
            1876: "elerethe-renferal",
            1873: "ilgynoth-heart-of-corruption",
            1841: "ursoc",
            1854: "dragons-of-nightmare",
            1877: "cenarius",
            1864: "xavius",
        },
    },
    12: {
        "slug": "trial-of-valor",
        "name": "Trial of Valor",
        "partitions": [1, 2],
        "bosses": {
            1958: "odyn",
            1962: "guarm",
            2008: "helya",
        },
    },
    11: {
        "slug": "the-nighthold",
        "name": "The Nighthold",
        "partitions": [1, 2],
        "bosses": {
            1849: "skorpyron",
            1865: "chronomatic-anomaly",
            1867: "trilliax",
            1871: "spellblade-aluriel",
            1862: "tichondrius",
            1842: "krosus",
            1886: "high-botanist-telarn",
            1863: "star-augur-etraeus",
            1872: "grand-magistrix-elisande",
            1866: "guldan",
        },
    },
    13: {
        "slug": "tomb-of-sargeras",
        "name": "Tomb of Sargeras",
        "partitions": [1, 2],
        "bosses": {
            2032: "goroth",
            2048: "demonic-inquisition",
            2036: "harjatan",
            2037: "mistress-sasszine",
            2050: "sisters-of-the-moon",
            2054: "the-desolate-host",
            2052: "maiden-of-vigilance",
            2038: "fallen-avatar",
            2051: "kiljaeden",
        },
    },
    17: {
        "slug": "antorus-the-burning-throne",
        "name": "Antorus, the Burning Throne",
        "partitions": [1, 2],
        "bosses": {
            2076: "garothi-worldbreaker",
            2074: "felhounds-of-sargeras",
            2070: "antoran-high-command",
            2064: "portal-keeper-hasabel",
            2075: "eonar-the-life-binder",
            2082: "imonar-the-soulhunter",
            2088: "kin-garoth",
            2069: "varimathras",
            2073: "the-coven-of-shivarra",
            2063: "aggramar",
            2092: "argus-the-unmaker",
        },
    },
}

LEGION_RAID_DIFFICULTIES: Dict[int, str] = {
    5: "Mythic",
    4: "Heroic",
    3: "Normal",
    1: "LFR",
}

LEGION_SPECS: List[Tuple[str, str, str]] = [
    ("DeathKnight", "Blood", "tank"),
    ("DeathKnight", "Frost", "dps"),
    ("DeathKnight", "Unholy", "dps"),
    ("DemonHunter", "Havoc", "dps"),
    ("DemonHunter", "Vengeance", "tank"),
    ("Druid", "Balance", "dps"),
    ("Druid", "Feral", "dps"),
    ("Druid", "Guardian", "tank"),
    ("Druid", "Restoration", "healer"),
    ("Hunter", "BeastMastery", "dps"),
    ("Hunter", "Marksmanship", "dps"),
    ("Hunter", "Survival", "dps"),
    ("Mage", "Arcane", "dps"),
    ("Mage", "Fire", "dps"),
    ("Mage", "Frost", "dps"),
    ("Monk", "Brewmaster", "tank"),
    ("Monk", "Mistweaver", "healer"),
    ("Monk", "Windwalker", "dps"),
    ("Paladin", "Holy", "healer"),
    ("Paladin", "Protection", "tank"),
    ("Paladin", "Retribution", "dps"),
    ("Priest", "Discipline", "healer"),
    ("Priest", "Holy", "healer"),
    ("Priest", "Shadow", "dps"),
    ("Rogue", "Assassination", "dps"),
    ("Rogue", "Outlaw", "dps"),
    ("Rogue", "Subtlety", "dps"),
    ("Shaman", "Elemental", "dps"),
    ("Shaman", "Enhancement", "dps"),
    ("Shaman", "Restoration", "healer"),
    ("Warlock", "Affliction", "dps"),
    ("Warlock", "Demonology", "dps"),
    ("Warlock", "Destruction", "dps"),
    ("Warrior", "Arms", "dps"),
    ("Warrior", "Fury", "dps"),
    ("Warrior", "Protection", "tank"),
]


@dataclass
class AppConfig:
    data_dir: Path = field(default_factory=lambda: DATA_DIR)
    max_unarchive_buffer: int = 30
    top_runs_per_dungeon_spec: int = 100
    top_runs_per_raid_boss_spec: int = 50
    zstd_level: int = 22
    scan_concurrency: int = 3
    download_concurrency: int = 4
    browser_request_concurrency: int = 8
    pack_workers: int = 4
    archive_compression: str = field(default_factory=lambda: os.environ.get("WCL_ARCHIVE_COMPRESSION", "compact"))
