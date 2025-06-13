import os
import struct
from pathlib import Path

import pytest
from src.sstable import MAX_KEY_SIZE, MAX_VALUE_SIZE, serialize_entry, deserialize_entry, SSTableFeatureFlags, SSTableWriter
from src.entry import DatabaseEntry


def test_basic_serialization_deserialization() -> None:
    """Test basic serialization and deserialization of a DatabaseEntry."""
    # create an entry
    entry = DatabaseEntry.put("test_key", 42, b"test_value")

    # serialize it
    serialized = serialize_entry(entry)

    # deserialize it
    deserialized_entry, bytes_consumed = deserialize_entry(serialized)

    assert deserialized_entry.key == "test_key"
    assert deserialized_entry.sequence == 42
    assert deserialized_entry.value == b"test_value"
    assert bytes_consumed == len(serialized)


def test_tombstone_serialization() -> None:
    """Test handling of DELETE entries (tombstones)."""
    # test with DELETE entry (tombstone)
    delete_entry = DatabaseEntry.delete("key1", 1)
    serialized = serialize_entry(delete_entry)
    deserialized, _ = deserialize_entry(serialized)
    assert deserialized.value is None  # Tombstones should have None value
    assert deserialized.sequence == 1
    assert deserialized.is_tombstone()


def test_size_validation() -> None:
    """Test that over-sized keys and values raise errors."""
    # test over-sized key
    with pytest.raises(ValueError, match=f"Key size exceeds max of {MAX_KEY_SIZE} bytes"):
        big_key = "x" * (MAX_KEY_SIZE + 1)
        entry = DatabaseEntry.put(big_key, 1, b"small_value")
        serialize_entry(entry)

    # test over-sized value
    with pytest.raises(ValueError, match=f"Value size exceeds max of {MAX_VALUE_SIZE} bytes"):
        big_value = b"x" * (MAX_VALUE_SIZE + 1)
        entry = DatabaseEntry.put("small_key", 1, big_value)
        serialize_entry(entry)

    # test negative sequence number
    with pytest.raises(ValueError, match="sequence number must be non-negative"):
        entry = DatabaseEntry.put("key", -1, b"value")
        serialize_entry(entry)


def test_unicode_keys() -> None:
    """Test handling of Unicode keys."""
    # test with emoji and special characters
    key = "hello_🌲_世界"
    entry = DatabaseEntry.put(key, 123, b"test")
    serialized = serialize_entry(entry)
    deserialized, _ = deserialize_entry(serialized)
    assert deserialized.key == key
    assert deserialized.sequence == 123


def test_sorting() -> None:
    """Test that entries can be sorted by key, then by sequence number."""
    entries = [
        DatabaseEntry.put("zebra", 1, b"1"),
        DatabaseEntry.put("apple", 2, b"2"),
        DatabaseEntry.put("banana", 3, b"3"),
        DatabaseEntry.put("apple", 5, b"newer"),  # same key, higher sequence
    ]

    sorted_entries = sorted(entries)
    assert [e.key for e in sorted_entries] == ["apple", "apple", "banana", "zebra"]
    # For same key, lower sequence number should come first
    apple_entries = [e for e in sorted_entries if e.key == "apple"]
    assert apple_entries[0].sequence == 2
    assert apple_entries[1].sequence == 5


# SSTableWriter tests
@pytest.fixture
def temp_sstable(tmp_path: Path) -> str:
    path = tmp_path / "test.sst"
    return str(path)


def test_add_entry_maintains_sort_order(temp_sstable: str) -> None:
    with SSTableWriter(temp_sstable) as writer:
        writer.add_entry(DatabaseEntry.put("b", 1, b"2"))
        writer.add_entry(DatabaseEntry.put("c", 2, b"3"))
        writer.add_entry(DatabaseEntry.put("d", 3, b"4"))

        with pytest.raises(ValueError):
            writer.add_entry(DatabaseEntry.put("a", 4, b"1"))


def test_writer_tracks_count_and_size(temp_sstable: str) -> None:
    writer = SSTableWriter(temp_sstable)
    entry = DatabaseEntry.put("a", 1, b"1")
    writer.add_entry(entry)
    assert writer.entry_count == 1

    writer.finalize()
    assert writer.data_size == len(serialize_entry(entry))
    if writer._file is not None:
        writer._file.close()


def test_duplicate_keys_not_allowed(temp_sstable: str) -> None:
    with SSTableWriter(temp_sstable) as writer:
        writer.add_entry(DatabaseEntry.put("a", 1, b"1"))
        with pytest.raises(ValueError):
            writer.add_entry(DatabaseEntry.put("a", 2, b"2"))


def test_value_error_triggers_discard(temp_sstable: str) -> None:
    with SSTableWriter(temp_sstable) as writer:
        writer.add_entry(DatabaseEntry.put("b", 1, b"2"))
        try:
            writer.add_entry(DatabaseEntry.put("a", 2, b"1"))  # Out of order - raises ValueError
        except ValueError:
            pass

    # file should be discarded due to ValueError
    assert not os.path.exists(temp_sstable)


def test_writer_sets_feature_flags(temp_sstable: str) -> None:
    features = SSTableFeatureFlags.COMPRESSION | SSTableFeatureFlags.BLOOM_FILTER
    writer = SSTableWriter(temp_sstable, features)
    writer.add_entry(DatabaseEntry.put("a", 1, b"1"))

    if writer._file is not None:
        file_path = writer._file.name
        writer.finalize()

        with open(file_path, "rb") as f:
            f.read(6)  # skip magic + version
            flags = struct.unpack("!I", f.read(4))[0]
            assert flags == features.value


def test_sequence_number_handling() -> None:
    """Test that sequence numbers are properly handled in serialization/deserialization."""
    # Test various sequence numbers
    entries = [
        DatabaseEntry.put("key1", 0, b"value1"),      # sequence 0
        DatabaseEntry.put("key2", 999999, b"value2"), # large sequence
        DatabaseEntry.put("key3", 42, b"value3"),     # arbitrary sequence
    ]
    
    for entry in entries:
        serialized = serialize_entry(entry)
        deserialized, _ = deserialize_entry(serialized)
        assert deserialized.sequence == entry.sequence
        assert deserialized.key == entry.key
        assert deserialized.value == entry.value