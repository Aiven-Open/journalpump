"""
Copyright (C) 2009 Hiroaki Kawai <kawai@iij.ad.jp>
"""

_base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_base32_map = {_base32[i]: i for i in range(len(_base32))}
LONG_ZERO = 0


def _float_hex_to_int(f):
    if f < -1.0 or f >= 1.0:
        return None

    if f == 0.0:
        return 1, 1

    h = f.hex()
    x = h.find("0x1.")
    assert x >= 0
    p = h.find("p")
    assert p > 0

    half_len = len(h[x + 4 : p]) * 4 - int(h[p + 1 :])
    if x == 0:
        r = (1 << half_len) + ((1 << (len(h[x + 4 : p]) * 4)) + int(h[x + 4 : p], 16))
    else:
        r = (1 << half_len) - ((1 << (len(h[x + 4 : p]) * 4)) + int(h[x + 4 : p], 16))

    return r, half_len + 1


def _encode_i2c(lat, lon, lat_length, lon_length):
    precision = int((lat_length + lon_length) / 5)
    if lat_length < lon_length:
        a = lon
        b = lat
    else:
        a = lat
        b = lon

    boost = (0, 1, 4, 5, 16, 17, 20, 21)
    ret = ""
    for _ in range(precision):
        ret += _base32[(boost[a & 7] + (boost[b & 3] << 1)) & 0x1F]
        t = a >> 3
        a = b >> 2
        b = t

    return ret[::-1]


def encode(latitude: float, longitude: float, precision: int = 12) -> str:
    if latitude >= 90.0 or latitude < -90.0:
        raise ValueError("invalid latitude.")
    while longitude < -180.0:
        longitude += 360.0
    while longitude >= 180.0:
        longitude -= 360.0

    xprecision = precision + 1
    lat_length = lon_length = int(xprecision * 5 / 2)
    if xprecision % 2 == 1:
        lon_length += 1

    a = _float_hex_to_int(latitude / 90.0)
    o = _float_hex_to_int(longitude / 180.0)
    if a[1] > lat_length:
        ai = a[0] >> (a[1] - lat_length)
    else:
        ai = a[0] << (lat_length - a[1])

    if o[1] > lon_length:
        oi = o[0] >> (o[1] - lon_length)
    else:
        oi = o[0] << (lon_length - o[1])

    return _encode_i2c(ai, oi, lat_length, lon_length)[:precision]
