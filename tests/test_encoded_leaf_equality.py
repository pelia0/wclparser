"""Hybrid comparisons keep archive encoding semantics without packing basic leaves."""
from copy import deepcopy
from decimal import Decimal
import struct

import msgpack
import pytest

from wcl_archiver.codec import fight_archive as codec


def _float(bits):
    return struct.unpack('>d', bytes.fromhex(bits))[0]


@pytest.mark.parametrize(('target', 'source'), [
    (None, None), (None, 0), (False, False), (True, 1), (False, 0),
    (1, 1), (1, 2), (-(2 ** 90), -(2 ** 90)), (2 ** 90, 2 ** 90 + 1),
    ('', ''), ('same', 'same'), ('same', 'different'), ('подія', 'подія'),
    (b'\x00\xff', b'\x00\xff'), (b'text', 'text'), (b'a', bytearray(b'a')),
    (Decimal('1.000000000000000001'), Decimal('1.000000000000000001')),
    (Decimal('1.000000000000000001'), Decimal('1.000000000000000002')),
    (Decimal('1.0'), Decimal('1.00')), (Decimal('0'), Decimal('-0')),
    (Decimal('1E+2'), Decimal('100')), (Decimal('1'), 1),
    (Decimal('NaN'), Decimal('NaN')), (Decimal('NaN1'), Decimal('NaN2')),
    (Decimal('sNaN'), Decimal('sNaN')), (Decimal('Infinity'), Decimal('Infinity')),
    (0.0, -0.0), (0.0, 0.0), (1.0, 1), (float('inf'), float('inf')),
    (_float('7ff8000000000001'), _float('7ff8000000000001')),
    (_float('7ff8000000000001'), _float('7ff8000000000002')),
    ((1, None), [1, None]), ((True,), (1,)),
    (msgpack.ExtType(9, b'a'), msgpack.ExtType(9, b'a')),
    (msgpack.ExtType(9, b'a'), msgpack.ExtType(9, b'b')),
], ids=lambda value: repr(value))
def test_merge_leaf_conflicts_match_encoded_values(target, source):
    # The pre-optimization encoded comparison is the compatibility oracle,
    # including tuple/list equivalence and distinct float/Decimal encodings.
    equal = codec._pack(target) == codec._pack(source)
    primary = {'nested': [{'value': deepcopy(target)}]}
    original_bytes = codec._pack(primary)
    conflicts = []

    codec._merge_missing(primary, {'nested': [{'value': source}]},
                         index=7, capture_id='browser', conflicts=conflicts)

    assert conflicts == ([] if equal else [
        {'index': 7, 'path': ['nested', 0, 'value'], 'source_capture': 'browser'}])
    assert codec._pack(primary) == original_bytes


def test_merge_keeps_null_and_primary_values_and_copies_missing_fields():
    precise = Decimal('0.12345678901234567890123456789')
    primary = {'null': None, 'nested': [{'amount': Decimal('1.0')}]}
    source = {'null': 3, 'nested': [{'amount': Decimal('1.00'), 'missing': precise},
                                  {'new': [None, precise]}], 'missingNull': None}
    conflicts = []

    codec._merge_missing(primary, source, index=2, capture_id='canonical', conflicts=conflicts)

    assert primary == {'null': None, 'nested': [{'amount': Decimal('1.0'), 'missing': precise},
                                               {'new': [None, precise]}], 'missingNull': None}
    assert str(primary['nested'][0]['amount']) == '1.0'
    assert str(primary['nested'][0]['missing']) == '0.12345678901234567890123456789'
    assert conflicts == [
        {'index': 2, 'path': ['null'], 'source_capture': 'canonical'},
        {'index': 2, 'path': ['nested', 0, 'amount'], 'source_capture': 'canonical'}]
    source['nested'][1]['new'].append('source mutation')
    assert primary['nested'][1]['new'] == [None, precise]


@pytest.mark.parametrize('conflicts', [None, []])
def test_merge_preserves_encoding_errors_even_when_conflicts_are_not_collected(conflicts):
    with pytest.raises(UnicodeEncodeError):
        codec._merge_missing({'value': '\ud800'}, {'value': '\ud800'},
                             index=0, capture_id='browser', conflicts=conflicts)


def test_scalar_subclass_equality_cannot_override_encoded_comparison():
    class UnequalInt(int):
        def __eq__(self, other):
            return False

    conflicts = []
    codec._merge_missing({'value': UnequalInt(3)}, {'value': UnequalInt(3)},
                         index=0, capture_id='browser', conflicts=conflicts)
    assert conflicts == []


def test_large_identical_primitive_events_do_not_serialize_each_leaf(monkeypatch):
    primary = [{'timestamp': i, 'type': 'damage', 'critical': bool(i % 2),
                'nullable': None, 'payload': b'abc', 'amount': Decimal('1.2300')}
               for i in range(5000)]
    source = deepcopy(primary)
    conflicts = []
    real_pack = codec._pack
    packed_values = []

    def track_pack(value):
        packed_values.append(value)
        return real_pack(value)

    monkeypatch.setattr(codec, '_pack', track_pack)
    codec._merge_missing(primary, source, index=0, capture_id='browser', conflicts=conflicts)

    assert primary == source
    assert conflicts == []
    assert len(packed_values) == 0, 'Identical scalar leaves still trigger per-value serialization'
