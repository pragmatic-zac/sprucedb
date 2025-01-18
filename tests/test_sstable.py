import pytest
from src.sstable import SSTableEntry, MAX_KEY_SIZE, MAX_VALUE_SIZE, SSTableEntry, SSTableFeatureFlags, SSTableWriter

# SSTableEntry tests
def test_basic_serialization_deserialization():
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


def test_empty_value():
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


def test_size_validation():
    """Test that over-sized keys and values raise errors."""
    # test over-sized key
    with pytest.raises(ValueError, match=f"Key size exceeds max of {MAX_KEY_SIZE} bytes"):
        big_key = "x" * (MAX_KEY_SIZE + 1)
        SSTableEntry(key=big_key, value=b"small_value").serialize()

    # test over-sized value
    with pytest.raises(ValueError, match=f"Value size exceeds max of {MAX_VALUE_SIZE} bytes"):
        big_value = b"x" * (MAX_VALUE_SIZE + 1)
        SSTableEntry(key="small_key", value=big_value).serialize()


def test_unicode_keys():
    """Test handling of Unicode keys."""
    # test with emoji and special characters
    key = "hello_🌲_世界"
    entry = SSTableEntry(key=key, value=b"test")
    serialized = entry.serialize()
    deserialized, _ = SSTableEntry.deserialize(serialized)
    assert deserialized.key == key


def test_sorting():
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
def temp_sstable(tmp_path):
    path = tmp_path / "test.sst"
    return str(path)


def test_add_entry_maintains_sort_order(temp_sstable):
    with SSTableWriter(temp_sstable) as writer:
        writer.add_entry(SSTableEntry("b", b"2"))
        writer.add_entry(SSTableEntry("c", b"3"))
        writer.add_entry(SSTableEntry("d", b"4"))

        with pytest.raises(ValueError):
            writer.add_entry(SSTableEntry("a", b"1"))

def test_writer_tracks_count_and_size(temp_sstable):
    with SSTableWriter(temp_sstable) as writer:
        writer.add_entry(SSTableEntry("a", b"1"))
        assert writer.entry_count == 1

        expected_size = len(SSTableEntry("a", b"1").serialize())

        writer.finalize()
        assert writer.data_size == expected_size

def test_duplicate_keys_not_allowed(temp_sstable):
    with SSTableWriter(temp_sstable) as writer:
        writer.add_entry(SSTableEntry("a", b"1"))
        with pytest.raises(ValueError):
            writer.add_entry(SSTableEntry("a", b"2"))

def test_sstable_writer_writes_valid_file(temp_sstable):
    with SSTableWriter(temp_sstable) as writer:
        # write some entries
        writer.add_entry(SSTableEntry("b", b"2"))

        # validate header

        # validate footer - only written on finalize