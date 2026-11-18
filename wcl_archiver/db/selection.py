"""The queue and ranking scope of one harvest."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class QueueSelection:
    content_type: Literal['raid', 'mplus']
    order: Literal['fifo', 'random'] = 'fifo'
    zone_id: int | None = None
    encounter_id: int | None = None
    difficulty: int | None = None
    specialization: tuple[str, str] | None = None
    partition: int | None = None

    def ranking_matches(self, zone_id, encounter_id, difficulty, class_name, spec_name):
        return (self.zone_id in (None, zone_id)
                and self.encounter_id in (None, encounter_id)
                and self.difficulty in (None, difficulty)
                and self.specialization in (None, (class_name, spec_name)))

    def ranking_filter(self, alias):
        clauses, values = [], []
        for column, value in (('zone_id', self.zone_id), ('encounter_id', self.encounter_id),
                              ('difficulty', self.difficulty), ('partition', self.partition)):
            if value is not None:
                clauses.append(f'{alias}.{column} = ?')
                values.append(value)
        return clauses, values

    def fight_filter(self):
        clauses, values = self.ranking_filter('f')
        clauses.insert(0, 'f.content_type = ?')
        values.insert(0, self.content_type)
        if self.specialization:
            clauses.append("""EXISTS (SELECT 1 FROM fight_participants p
                WHERE p.fight_uid = f.fight_uid AND p.class_name = ? AND p.spec_name = ?)""")
            values.extend(self.specialization)
        return ' AND '.join(clauses), values
