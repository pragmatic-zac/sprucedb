import os
import tempfile

import pytest
import struct
import zlib
from datetime import datetime

from src.wal import WALOperationType, WALEntry, WriteAheadLog, MAX_KEY_BYTES


def test_serialize_put_entry() -> None:
    timestamp = int(datetime.now().timestamp())
    key = "test_key"
    value = b"test_value"
    sequence = 42
    entry = WALEntry.put(timestamp, key, value, sequence)

    serialized = entry.serialize()

    # verify header format
    header = serialized[:WALEntry.HEADER_SIZE]
    src_crc, read_sequence, read_timestamp, op_type_value, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header)

    # verify data
    key_start = WALEntry.HEADER_SIZE
    key_end = key_start + key_len
    value_end = key_end + value_len

    read_key = serialized[key_start:key_end].decode('utf-8')
    read_value = serialized[key_end:value_end]

    # calculate expected CRC
    raw_header = struct.pack(
        WALEntry.HEADER_FORMAT_SANS_CRC,
        sequence,
        timestamp,
        WALOperationType.PUT.value,
        len(key),
        len(value)
    )
    expected_crc = zlib.crc32(raw_header + key.encode('utf-8') + value)

    # all components should match
    assert read_sequence == sequence
    assert read_timestamp == timestamp
    assert op_type_value == WALOperationType.PUT.value
    assert key_len == len(key)
    assert value_len == len(value)
    assert read_key == key
    assert read_value == value
    assert src_crc == expected_crc


def test_serialize_delete_entry() -> None:
    timestamp = int(datetime.now().timestamp())
    key = "test_key"
    sequence = 42
    entry = WALEntry.delete(timestamp, key, sequence)

    serialized = entry.serialize()

    # verify header format
    header = serialized[:WALEntry.HEADER_SIZE]
    src_crc, read_sequence, read_timestamp, op_type_value, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header)

    # verify data
    key_start = WALEntry.HEADER_SIZE
    key_end = key_start + key_len

    read_key = serialized[key_start:key_end].decode('utf-8')

    # calculate expected CRC
    raw_header = struct.pack(
        WALEntry.HEADER_FORMAT_SANS_CRC,
        sequence,
        timestamp,
        WALOperationType.DELETE.value,
        len(key),
        0  # value length should be 0 for DELETE
    )
    expected_crc = zlib.crc32(raw_header + key.encode('utf-8'))

    # all components should match
    assert read_sequence == sequence
    assert read_timestamp == timestamp
    assert op_type_value == WALOperationType.DELETE.value
    assert key_len == len(key)
    assert value_len == 0
    assert read_key == key
    # delete entry should have no value bytes
    assert len(serialized) == WALEntry.HEADER_SIZE + len(key)
    assert src_crc == expected_crc


def test_serialize_with_unicode_key() -> None:
    timestamp = int(datetime.now().timestamp())
    key = "𓂀𓃭𓆣"  # eye of Horus, cat, bee
    value = b"test_value"
    sequence = 42
    entry = WALEntry.put(timestamp, key, value, sequence)

    serialized = entry.serialize()

    # verify header format
    header = serialized[:WALEntry.HEADER_SIZE]
    src_crc, read_sequence, read_timestamp, op_type_value, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header)

    # each hieroglyph is 4 bytes in UTF-8
    expected_key_bytes_len = len(key.encode('utf-8'))

    # verify data
    key_start = WALEntry.HEADER_SIZE
    key_end = key_start + key_len
    value_end = key_end + value_len

    read_key = serialized[key_start:key_end].decode('utf-8')
    read_value = serialized[key_end:value_end]

    # all components should match
    assert read_sequence == sequence
    assert read_timestamp == timestamp
    assert op_type_value == WALOperationType.PUT.value
    assert key_len == expected_key_bytes_len, f"Expected key length {expected_key_bytes_len} bytes, got {key_len}"
    assert value_len == len(value)
    assert read_key == key
    assert read_value == value


def test_serialize_empty_value() -> None:
    timestamp = int(datetime.now().timestamp())
    key = "test_key"
    value = b""
    sequence = 42
    entry = WALEntry.put(timestamp, key, value, sequence)

    serialized = entry.serialize()

    # verify header format
    header = serialized[:WALEntry.HEADER_SIZE]
    _, read_sequence, read_timestamp, op_type_value, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header)

    assert read_sequence == sequence
    assert value_len == 0
    assert len(serialized) == WALEntry.HEADER_SIZE + len(key)


def test_basic_write_and_rotation() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")

        # test basic writes
        wal = WriteAheadLog(wal_path)
        pos1 = wal.write_to_log(WALOperationType.PUT, "key1", 0, b"value1")
        pos2 = wal.write_to_log(WALOperationType.PUT, "key2", 1, b"value2")

        # verify positions are sequential
        assert pos2 > pos1

        # rotate the file
        old_path = wal.rotate("sst_001")
        assert os.path.exists(old_path)

        # verify we can still write after rotation
        pos3 = wal.write_to_log(WALOperationType.PUT, "key3", 2, b"value3")
        assert pos3 == 0  # Position should reset after rotation


def test_wal_validations() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)

        # invalid key should raise
        with pytest.raises(ValueError):
            wal.write_to_log(WALOperationType.PUT, "", 0, b"value")

        # huge key should raise
        huge_key = "x" * (MAX_KEY_BYTES + 1)
        with pytest.raises(ValueError):
            wal.write_to_log(WALOperationType.PUT, huge_key, 0, b"value")

        # negative sequence number should raise
        with pytest.raises(ValueError):
            wal.write_to_log(WALOperationType.PUT, "key", -1, b"value")


def test_file_closure_and_sync() -> None:
    # test that file is properly closed and synced when using context manager
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")

        with WriteAheadLog(wal_path) as wal:
            pos = wal.write_to_log(WALOperationType.PUT, "key1", 0, b"value1")
            actual_file_path = wal.current_path

        # verify file is closed
        assert wal.write_file is None

        # verify data was actually written by reading file
        with open(actual_file_path, 'rb') as f:
            data = f.read()
            entry = WALEntry.deserialize(data)
            assert entry.key == "key1"
            assert entry.value == b"value1"
            assert entry.op_type == WALOperationType.PUT
            assert entry.sequence == 0  # First entry should have sequence 0


def test_explicit_close() -> None:
    # test explicit close() call
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)

        wal.write_to_log(WALOperationType.PUT, "key1", 0, b"value1")
        wal.close()

        assert wal.write_file is None

        # verify we can't write after closing
        with pytest.raises(RuntimeError):
            wal.write_to_log(WALOperationType.PUT, "key2", 1, b"value2")


def test_read_log_entry() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)

        expected_key = "key1"
        expected_value = b"value100"

        wal.write_to_log(WALOperationType.PUT, expected_key, 0, expected_value)

        result = wal.read_log_entry(0)
        assert result is not None
        assert expected_key == result.key
        assert expected_value == result.value
        assert result.sequence == 0  # First entry should have sequence 0


def test_sequence_number_increment() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)

        # Write multiple entries with externally managed sequence numbers
        pos1 = wal.write_to_log(WALOperationType.PUT, "key1", 0, b"value1")
        pos2 = wal.write_to_log(WALOperationType.PUT, "key2", 1, b"value2")
        pos3 = wal.write_to_log(WALOperationType.DELETE, "key3", 2)

        # Read back entries and verify sequence numbers
        entry1 = wal.read_log_entry(pos1)
        entry2 = wal.read_log_entry(pos2)
        entry3 = wal.read_log_entry(pos3)

        assert entry1 is not None
        assert entry2 is not None
        assert entry3 is not None
        assert entry1.sequence == 0
        assert entry2.sequence == 1
        assert entry3.sequence == 2


def test_sequence_number_persists_after_rotation() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)

        # Write some entries with externally managed sequence numbers
        wal.write_to_log(WALOperationType.PUT, "key1", 0, b"value1")
        wal.write_to_log(WALOperationType.PUT, "key2", 1, b"value2")

        # Rotate the file
        old_path = wal.rotate("sst_001")

        # Write more entries after rotation with continuing sequence numbers
        pos3 = wal.write_to_log(WALOperationType.PUT, "key3", 2, b"value3")
        pos4 = wal.write_to_log(WALOperationType.PUT, "key4", 3, b"value4")

        # Read back entries and verify sequence numbers continue from previous file
        entry3 = wal.read_log_entry(pos3)
        entry4 = wal.read_log_entry(pos4)

        assert entry3 is not None
        assert entry4 is not None
        assert entry3.sequence == 2
        assert entry4.sequence == 3