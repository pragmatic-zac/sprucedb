import pytest
from src.entry import DatabaseEntry, EntryType
from src.wal import WALEntry, WALOperationType
from src.sstable import serialize_entry, deserialize_entry


def test_database_entry_creation() -> None:
    """Test basic DatabaseEntry creation and validation."""
    # Test PUT entry
    put_entry = DatabaseEntry.put("key1", 42, b"value1", 1234567890)
    assert put_entry.key == "key1"
    assert put_entry.sequence == 42
    assert put_entry.entry_type == EntryType.PUT
    assert put_entry.value == b"value1"
    assert put_entry.timestamp == 1234567890
    assert not put_entry.is_tombstone()

    # Test DELETE entry
    delete_entry = DatabaseEntry.delete("key2", 43, 1234567891)
    assert delete_entry.key == "key2"
    assert delete_entry.sequence == 43
    assert delete_entry.entry_type == EntryType.DELETE
    assert delete_entry.value is None
    assert delete_entry.timestamp == 1234567891
    assert delete_entry.is_tombstone()


def test_database_entry_validation() -> None:
    """Test DatabaseEntry validation constraints."""
    # Empty key should raise
    with pytest.raises(ValueError, match="key cannot be empty"):
        DatabaseEntry("", 0, EntryType.PUT, b"value")

    # Negative sequence should raise
    with pytest.raises(ValueError, match="sequence number must be non-negative"):
        DatabaseEntry("key", -1, EntryType.PUT, b"value")

    # PUT without value should raise
    with pytest.raises(ValueError, match="PUT entries must have a value"):
        DatabaseEntry("key", 0, EntryType.PUT, None)

    # DELETE with value should raise
    with pytest.raises(ValueError, match="DELETE entries cannot have a value"):
        DatabaseEntry("key", 0, EntryType.DELETE, b"value")


def test_database_entry_sorting() -> None:
    """Test DatabaseEntry sorting behavior."""
    entry1 = DatabaseEntry.put("a", 1, b"value1")
    entry2 = DatabaseEntry.put("b", 1, b"value2")
    entry3 = DatabaseEntry.put("a", 2, b"value3")  # Same key, higher sequence

    # Sort by key first
    assert entry1 < entry2
    assert entry2 > entry1

    # For same key, higher sequence number wins
    assert entry1 < entry3
    assert entry3 > entry1

    # Test sorting in list
    entries = [entry2, entry3, entry1]
    sorted_entries = sorted(entries)
    assert sorted_entries == [entry1, entry3, entry2]


def test_wal_entry_conversion() -> None:
    """Test conversion between DatabaseEntry and WALEntry."""
    # Test PUT entry conversion
    db_entry = DatabaseEntry.put("test_key", 42, b"test_value", 1234567890)
    wal_entry = WALEntry.from_database_entry(db_entry)
    
    assert wal_entry.key == "test_key"
    assert wal_entry.sequence == 42
    assert wal_entry.value == b"test_value"
    assert wal_entry.timestamp == 1234567890
    assert wal_entry.op_type == WALOperationType.PUT

    # Convert back to DatabaseEntry
    converted_back = wal_entry.to_database_entry()
    assert converted_back.key == db_entry.key
    assert converted_back.sequence == db_entry.sequence
    assert converted_back.value == db_entry.value
    assert converted_back.entry_type == db_entry.entry_type
    assert converted_back.timestamp == db_entry.timestamp

    # Test DELETE entry conversion
    delete_entry = DatabaseEntry.delete("delete_key", 43, 1234567891)
    wal_delete = WALEntry.from_database_entry(delete_entry)
    
    assert wal_delete.key == "delete_key"
    assert wal_delete.sequence == 43
    assert wal_delete.value == b""  # WAL uses empty bytes for DELETE
    assert wal_delete.timestamp == 1234567891
    assert wal_delete.op_type == WALOperationType.DELETE


def test_sstable_serialization() -> None:
    """Test DatabaseEntry serialization/deserialization for SSTable format."""
    # Test PUT entry serialization
    db_entry = DatabaseEntry.put("test_key", 42, b"test_value")
    serialized = serialize_entry(db_entry)
    
    # Deserialize it back
    deserialized, bytes_consumed = deserialize_entry(serialized)
    assert bytes_consumed == len(serialized)
    assert deserialized.key == "test_key"
    assert deserialized.sequence == 42
    assert deserialized.value == b"test_value"
    assert deserialized.entry_type == EntryType.PUT
    assert deserialized.timestamp is None  # SSTable doesn't preserve timestamp

    # Test DELETE entry serialization (tombstone)
    delete_entry = DatabaseEntry.delete("delete_key", 43)
    serialized_delete = serialize_entry(delete_entry)
    
    # Deserialize it back
    deserialized_delete, bytes_consumed = deserialize_entry(serialized_delete)
    assert bytes_consumed == len(serialized_delete)
    assert deserialized_delete.key == "delete_key"
    assert deserialized_delete.sequence == 43
    assert deserialized_delete.value is None  # SSTable uses None for tombstones
    assert deserialized_delete.entry_type == EntryType.DELETE
    assert deserialized_delete.is_tombstone()


def test_sstable_tombstone_serialization() -> None:
    """Test that SSTable tombstones serialize and deserialize correctly."""
    # Create a tombstone entry
    tombstone = DatabaseEntry.delete("deleted_key", 42)
    assert tombstone.is_tombstone()

    # Serialize it
    serialized = serialize_entry(tombstone)
    assert len(serialized) > 0

    # Deserialize it
    deserialized, bytes_consumed = deserialize_entry(serialized)
    assert bytes_consumed == len(serialized)
    assert deserialized.key == "deleted_key"
    assert deserialized.sequence == 42
    assert deserialized.value is None
    assert deserialized.is_tombstone()


def test_round_trip_conversion() -> None:
    """Test round-trip conversion between all entry types."""
    # Start with DatabaseEntry
    original = DatabaseEntry.put("round_trip", 100, b"round_trip_value", 9999999999)

    # Convert to WAL and back
    wal_entry = WALEntry.from_database_entry(original)
    from_wal = wal_entry.to_database_entry()
    
    assert from_wal.key == original.key
    assert from_wal.sequence == original.sequence
    assert from_wal.value == original.value
    assert from_wal.entry_type == original.entry_type
    assert from_wal.timestamp == original.timestamp

    # Convert to SSTable format and back
    sst_serialized = serialize_entry(original)
    from_sst, _ = deserialize_entry(sst_serialized)
    
    assert from_sst.key == original.key
    assert from_sst.sequence == original.sequence
    assert from_sst.value == original.value
    assert from_sst.entry_type == original.entry_type
    assert from_sst.timestamp is None  # SSTable doesn't preserve timestamp 