import pytest
import struct
import zlib
from datetime import datetime

from src.wal import WALOperationType, WALEntry


def test_serialize_put_entry():
    timestamp = int(datetime.now().timestamp())
    key = "test_key"
    value = b"test_value"
    entry = WALEntry.put(timestamp, key, value)

    serialized = entry.serialize()

    # verify header format
    header = serialized[:WALEntry.HEADER_SIZE]
    src_crc, read_timestamp, op_type_value, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header)

    # verify data
    key_start = WALEntry.HEADER_SIZE
    key_end = key_start + key_len
    value_end = key_end + value_len

    read_key = serialized[key_start:key_end].decode('utf-8')
    read_value = serialized[key_end:value_end]

    # calculate expected CRC
    raw_header = struct.pack(
        WALEntry.HEADER_FORMAT_SANS_CRC,
        timestamp,
        WALOperationType.PUT.value,
        len(key),
        len(value)
    )
    expected_crc = zlib.crc32(raw_header + key.encode('utf-8') + value)

    # all components should match
    assert read_timestamp == timestamp
    assert op_type_value == WALOperationType.PUT.value
    assert key_len == len(key)
    assert value_len == len(value)
    assert read_key == key
    assert read_value == value
    assert src_crc == expected_crc


def test_serialize_delete_entry():
    timestamp = int(datetime.now().timestamp())
    key = "test_key"
    entry = WALEntry.delete(timestamp, key)

    serialized = entry.serialize()

    # verify header format
    header = serialized[:WALEntry.HEADER_SIZE]
    src_crc, read_timestamp, op_type_value, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header)

    # verify data
    key_start = WALEntry.HEADER_SIZE
    key_end = key_start + key_len

    read_key = serialized[key_start:key_end].decode('utf-8')

    # calculate expected CRC
    raw_header = struct.pack(
        WALEntry.HEADER_FORMAT_SANS_CRC,
        timestamp,
        WALOperationType.DELETE.value,
        len(key),
        0  # value length should be 0 for DELETE
    )
    expected_crc = zlib.crc32(raw_header + key.encode('utf-8'))

    # all components should match
    assert read_timestamp == timestamp
    assert op_type_value == WALOperationType.DELETE.value
    assert key_len == len(key)
    assert value_len == 0
    assert read_key == key
    # delete entry should have no value bytes
    assert len(serialized) == WALEntry.HEADER_SIZE + len(key)
    assert src_crc == expected_crc


def test_serialize_with_unicode_key():
    timestamp = int(datetime.now().timestamp())
    key = "𓂀𓃭𓆣"  # eye of Horus, cat, bee
    value = b"test_value"
    entry = WALEntry.put(timestamp, key, value)

    serialized = entry.serialize()

    # verify header format
    header = serialized[:WALEntry.HEADER_SIZE]
    src_crc, read_timestamp, op_type_value, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header)

    # each hieroglyph is 4 bytes in UTF-8
    expected_key_bytes_len = len(key.encode('utf-8'))

    # verify data
    key_start = WALEntry.HEADER_SIZE
    key_end = key_start + key_len
    value_end = key_end + value_len

    read_key = serialized[key_start:key_end].decode('utf-8')
    read_value = serialized[key_end:value_end]

    # all components should match
    assert read_timestamp == timestamp
    assert op_type_value == WALOperationType.PUT.value
    assert key_len == expected_key_bytes_len, f"Expected key length {expected_key_bytes_len} bytes, got {key_len}"
    assert value_len == len(value)
    assert read_key == key
    assert read_value == value


def test_serialize_empty_value():
    timestamp = int(datetime.now().timestamp())
    key = "test_key"
    value = b""
    entry = WALEntry.put(timestamp, key, value)

    serialized = entry.serialize()

    # verify header format
    header = serialized[:WALEntry.HEADER_SIZE]
    _, read_timestamp, op_type_value, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header)

    assert value_len == 0
    assert len(serialized) == WALEntry.HEADER_SIZE + len(key)