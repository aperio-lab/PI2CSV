import base64
import logging
import time
import struct

import avro.schema
import io

logger = logging.getLogger(__name__)


def decode_header(value):
    if value is None:
        return None, None
    try:
        decoded = base64.b64decode(value.encode('utf-8'))
        key, value = decoded.decode('utf-8').split(':')
        return key, value
    except TypeError as e:
        logger.exception(e)

"""
Convert timestamp to time delta in form of 123s or -123s (seconds)
"""
def ts_to_time_delta(ts):
    now = int(time.time())
    time_delta = now - int(ts)
    sign = '+' if time_delta < 0 else '-'

    return '{0}{1}{2}'.format(sign, time_delta, 's')


def sec_to_nanosec(ts: str):
    return int(ts + ('0' * 9))


def milisec_to_nanosec(ts: str):
    return int(ts + ('0' * 6))


def microsec_to_nanosec(ts: str):
    return int(ts + ('0' * 3))


def nanosec_to_milisec(ts: str):
    return float('.'.join([ts[0:10], ts[10:13]]))


def ts_to_nanosec(ts: float):

    out_ts = 0
    ts = float(ts)

    if ts > 10**18:
        out_ts = int(str(ts)[0:19])
    elif ts > 10**15:
        out_ts = int(ts*10**3)
    elif ts > 10**12:
        out_ts = int(ts*10**6)
    elif ts > 10**9:
        out_ts = int(ts*10**9)
    elif int(ts) == 0:
        out_ts = 0
    else:
        raise ValueError('Timestamp is not in S, mS, uS or nS')

    return out_ts


def ts_to_milisec(ts):
    ts_str = str(ts).replace('.', '')
    return nanosec_to_milisec(ts_str)


def uint32array_to_uint8_array(n: int):
    return [int(i) for i in n.to_bytes(4, byteorder='little', signed=False)]


conversion = {
    's': str,
    'i': int,
    'h': int,
    'q': int,
    'f': float,
    'd': float
}


def do_avro_pack(datum, data):

    buf = io.BytesIO()
    #  datum._writer_schema
    encoder = avro.io.BinaryEncoder(buf)
    data_scheme = {
        "channel": str(data[0]),
        "sample_time": int(data[1]*1000),
        "sample_value": data[2],
        "reason": data[3]
    }
    # try:
    datum.write(data_scheme, encoder)
    # except Exception as ex:
    #     print(ex)
    return buf.getvalue()


def do_pack(types, data):
    if isinstance(types, str):
        types = types.split(' ')
    if len(types) != len(data):
        raise Exception("wrong lengths")
    packing = ''
    data_iter = []
    for i, struct_type in enumerate(types):
        if struct_type == 's':
            data_str = str(data[i])
            packing += '%ds' % len(data_str)
            data_iter.append(bytes(data_str, encoding='utf-8'))
        else:
            packing += struct_type
            data_iter.append(conversion[struct_type](data[i]))
    return struct.pack(packing, *data_iter)
