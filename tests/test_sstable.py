import os
import struct
from pathlib import Path

import pytest
from src.sstable import MAX_KEY_SIZE, MAX_VALUE_SIZE, SSTableEntry, SSTableFeatureFlags, SSTableWriter

# SSTableEntry tests
def test_basic_serialization_deserialization() -> None:
    """Test basic serialization and deserialization of an entry."""
    # create an entry
    entry = SSTableEntry(key="test_key", value=b"test_value")

    # serialize it
    serialized = entry.serialize()

    # deserialize it
    deserialized_entry, bytes_consumed = SSTableEntry.deserialize(serialized)

    assert deserialized_entry.key == "test_key"
    assert deserialized_entry.value == b"test_value"
    assert bytes_consumed == len(serialized)


def test_empty_value() -> None:
    """Test handling of None/empty values."""
    # test with None value
    entry_none = SSTableEntry(key="key1", value=None)
    serialized = entry_none.serialize()
    deserialized, _ = SSTableEntry.deserialize(serialized)
    assert deserialized.value == b''

    # test with empty bytes
    entry_empty = SSTableEntry(key="key1", value=b'')
    serialized = entry_empty.serialize()
    deserialized, _ = SSTableEntry.deserialize(serialized)
    assert deserialized.value == b''


def test_size_validation() -> None:
    """Test that over-sized keys and values raise errors."""
    # test over-sized key
    with pytest.raises(ValueError, match=f"Key size exceeds max of {MAX_KEY_SIZE} bytes"):
        big_key = "x" * (MAX_KEY_SIZE + 1)
        SSTableEntry(key=big_key, value=b"small_value").serialize()

    # test over-sized value
    with pytest.raises(ValueError, match=f"Value size exceeds max of {MAX_VALUE_SIZE} bytes"):
        big_value = b"x" * (MAX_VALUE_SIZE + 1)
        SSTableEntry(key="small_key", value=big_value).serialize()


def test_unicode_keys() -> None:
    """Test handling of Unicode keys."""
    # test with emoji and special characters
    key = "hello_🌲_世界"
    entry = SSTableEntry(key=key, value=b"test")
    serialized = entry.serialize()
    deserialized, _ = SSTableEntry.deserialize(serialized)
    assert deserialized.key == key


def test_sorting() -> None:
    """Test that entries can be sorted by key."""
    entries = [
        SSTableEntry("zebra", b"1"),
        SSTableEntry("apple", b"2"),
        SSTableEntry("banana", b"3")
    ]

    sorted_entries = sorted(entries)
    assert [e.key for e in sorted_entries] == ["apple", "banana", "zebra"]

# SSTableWriter tests
@pytest.fixture
def temp_sstable(tmp_path: Path) -> str:
    path = tmp_path / "test.sst"
    return str(path)


def test_add_entry_maintains_sort_order(temp_sstable: str) -> None:
    with SSTableWriter(temp_sstable) as writer:
        writer.add_entry(SSTableEntry("b", b"2"))
        writer.add_entry(SSTableEntry("c", b"3"))
        writer.add_entry(SSTableEntry("d", b"4"))

        with pytest.raises(ValueError):
            writer.add_entry(SSTableEntry("a", b"1"))


def test_writer_tracks_count_and_size(temp_sstable: str) -> None:
    writer = SSTableWriter(temp_sstable)
    writer.add_entry(SSTableEntry("a", b"1"))
    assert writer.entry_count == 1

    writer.finalize()
    assert writer.data_size == len(SSTableEntry("a", b"1").serialize())
    if writer._file is not None:
        writer._file.close()


def test_duplicate_keys_not_allowed(temp_sstable: str) -> None:
    with SSTableWriter(temp_sstable) as writer:
        writer.add_entry(SSTableEntry("a", b"1"))
        with pytest.raises(ValueError):
            writer.add_entry(SSTableEntry("a", b"2"))


def test_value_error_triggers_discard(temp_sstable: str) -> None:
    with SSTableWriter(temp_sstable) as writer:
        writer.add_entry(SSTableEntry("b", b"2"))
        try:
            writer.add_entry(SSTableEntry("a", b"1"))  # Out of order - raises ValueError
        except ValueError:
            pass

    # file should be discarded due to ValueError
    assert not os.path.exists(temp_sstable)


def test_writer_sets_feature_flags(temp_sstable: str) -> None:
    features = SSTableFeatureFlags.COMPRESSION | SSTableFeatureFlags.BLOOM_FILTER
    writer = SSTableWriter(temp_sstable, features)
    writer.add_entry(SSTableEntry("a", b"1"))

    if writer._file is not None:
        file_path = writer._file.name
        writer.finalize()

        with open(file_path, "rb") as f:
            f.read(6)  # skip magic + version
            flags = struct.unpack("!I", f.read(4))[0]
            assert flags == features.value