from typing import List


def zigzag_encode(val: int) -> int:
    return (val << 1) ^ (val >> 63) if val < 0 else (val << 1)


def zigzag_decode(val: int) -> int:
    return (val >> 1) ^ -(val & 1)


def encode_delta_ints(values: List[int]) -> bytes:
    out = bytearray()
    prev = 0
    for v in values:
        delta = int(v) - prev
        prev = int(v)
        zz = zigzag_encode(delta)
        while zz >= 0x80:
            out.append((zz & 0x7F) | 0x80)
            zz >>= 7
        out.append(zz & 0x7F)
    return bytes(out)


def decode_delta_ints(buf: bytes, count: int) -> List[int]:
    res: List[int] = []
    idx = 0
    prev = 0
    n = len(buf)
    for _ in range(count):
        zz = 0
        shift = 0
        while idx < n:
            b = buf[idx]
            idx += 1
            zz |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break
            shift += 7
        delta = zigzag_decode(zz)
        prev += delta
        res.append(prev)
    return res
