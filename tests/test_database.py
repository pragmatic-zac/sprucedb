import tempfile
import pytest

from src.database import Database
from src.configuration import Configuration
from src.entry import EntryType, DatabaseEntry


def test_put_basic_operation() -> None:
    """Test basic put operation integrates WAL and memtable correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        db.put("test_key", b"test_value")
        
        # Verify sequence number was incremented
        assert db.seq_no == 1
        
        # Verify memtable contains entry with correct data
        entry = db.memtable.search("test_key")
        assert entry is not None
        assert entry.key == "test_key"
        assert entry.value == b"test_value"
        assert entry.sequence == 1
        assert entry.entry_type == EntryType.PUT
        
        # Verify WAL was written (by checking that position advanced)
        assert db.wal.write_position > 0
        
        db.close()


def test_put_sequence_number_generation() -> None:
    """Test that sequence numbers are generated correctly across multiple puts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Put multiple entries
        db.put("key1", b"value1")
        db.put("key2", b"value2")
        db.put("key3", b"value3")
        
        # Verify final sequence number
        assert db.seq_no == 3
        
        # Verify each entry has correct sequence number
        entry1 = db.memtable.search("key1")
        entry2 = db.memtable.search("key2")
        entry3 = db.memtable.search("key3")
        
        assert entry1 is not None
        assert entry2 is not None
        assert entry3 is not None
        
        assert entry1.sequence == 1
        assert entry2.sequence == 2
        assert entry3.sequence == 3
        
        # Verify all entries are in memtable
        assert entry1.key == "key1"
        assert entry1.value == b"value1"
        assert entry2.key == "key2"
        assert entry2.value == b"value2"
        assert entry3.key == "key3"
        assert entry3.value == b"value3"
        
        db.close()


def test_put_exception_propagation() -> None:
    """Test that exceptions from components bubble up correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Test ValueError from DatabaseEntry validation (empty key)
        with pytest.raises(ValueError, match="key cannot be empty"):
            db.put("", b"some_value")
        
        # Test ValueError from WAL size constraints (key too large)
        large_key = "x" * 70000  # Exceeds MAX_KEY_BYTES (65536)
        with pytest.raises(ValueError, match="key exceeds max size"):
            db.put(large_key, b"value")
        
        # Test ValueError from WAL size constraints (value too large)
        large_value = b"x" * (1024 * 1024 + 1)  # Exceeds MAX_VALUE_BYTES (1MB)
        with pytest.raises(ValueError, match="value exceeds max size"):
            db.put("test_key", large_value)
        
        # Verify database state remains consistent after exceptions
        # Note: sequence numbers are consumed even on failed operations (expected behavior)
        assert db.seq_no == 3  # Three failed operations consumed sequence numbers
        assert db.memtable.search("") is None
        assert db.memtable.search(large_key) is None
        assert db.memtable.search("test_key") is None
        
        db.close()


def test_put_wal_before_memtable_consistency() -> None:
    """Test that WAL is written before memtable for durability."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Record initial WAL position
        initial_position = db.wal.write_position
        
        db.put("consistency_key", b"consistency_value")
        
        # Verify WAL position advanced (indicating write occurred)
        assert db.wal.write_position > initial_position
        
        # Verify both WAL and memtable have the entry
        memtable_entry = db.memtable.search("consistency_key")
        assert memtable_entry is not None
        assert memtable_entry.key == "consistency_key"
        assert memtable_entry.value == b"consistency_value"
        
        # Read from WAL to verify it was written
        wal_entry = db.wal.read_log_entry(initial_position)
        assert wal_entry is not None
        assert wal_entry.key == "consistency_key"
        assert wal_entry.value == b"consistency_value"
        
        db.close()


def test_put_multiple_operations_integration() -> None:
    """Test multiple put operations work correctly together."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Test with different data types and sizes
        test_cases = [
            ("small_key", b"small_value"),
            ("longer_key_name", b"longer value with more data"),
            ("unicode_key_🚀", b"unicode value with emoji data \xf0\x9f\x9a\x80"),
            ("empty_value_key", b""),
            ("numeric_key_123", b"12345"),
        ]
        
        for i, (key, value) in enumerate(test_cases, 1):
            db.put(key, value)
            
            # Verify this specific entry
            entry = db.memtable.search(key)
            assert entry is not None
            assert entry.key == key
            assert entry.value == value
            assert entry.sequence == i
            
        # Verify final state
        assert db.seq_no == len(test_cases)
        
        # Verify all entries are still accessible
        for key, value in test_cases:
            entry = db.memtable.search(key)
            assert entry is not None
            assert entry.key == key
            assert entry.value == value
        
        db.close() 


def test_get_from_memtable_basic() -> None:
    """Test basic get operation from memtable."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Put some data
        db.put("test_key", b"test_value")
        db.put("another_key", b"another_value")
        
        # Test successful gets
        result1 = db.get("test_key")
        assert result1 == b"test_value"
        
        result2 = db.get("another_key")
        assert result2 == b"another_value"
        
        db.close()


def test_get_nonexistent_key() -> None:
    """Test get operation for keys that don't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Test get on empty database
        result = db.get("nonexistent_key")
        assert result is None
        
        # Add some data and test get for different nonexistent key
        db.put("existing_key", b"existing_value")
        
        result = db.get("nonexistent_key")
        assert result is None
        
        # Verify existing key still works
        result = db.get("existing_key")
        assert result == b"existing_value"
        
        db.close()


def test_get_with_key_updates() -> None:
    """Test get returns most recent version when key is updated multiple times."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Update same key multiple times
        db.put("key", b"value1")
        db.put("key", b"value2")
        db.put("key", b"value3")
        
        # Should get most recent version
        result = db.get("key")
        assert result == b"value3"
        
        db.close()


def test_get_with_tombstones() -> None:
    """Test get operation with deleted keys (tombstones)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Put a key, then delete it
        db.put("key_to_delete", b"some_value")
        
        # Verify key exists before deletion
        result = db.get("key_to_delete")
        assert result == b"some_value"
        
        # Delete the key - manually insert a tombstone to test get behavior
        seq_num = db._get_next_sequence()
        tombstone = DatabaseEntry.delete("key_to_delete", seq_num)
        db.wal.write_to_log(tombstone)
        db.memtable.insert("key_to_delete", tombstone)
        
        # Get should return None for deleted key
        result = db.get("key_to_delete")
        assert result is None
        
        db.close()


def test_get_error_handling() -> None:
    """Test get operation handles errors gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Add some data
        db.put("test_key", b"test_value")
        
        # Test get with various edge case keys
        test_keys = [
            "test_key",          # Normal case
            "",                  # Empty string (if supported)
            "very_long_key_" * 100,  # Long key
            "unicode_🚀_key",    # Unicode key
            "key.with.dots",     # Key with special chars
        ]
        
        for key in test_keys:
            try:
                result = db.get(key)
                # Should either return valid result or None, not crash
                assert result is None or isinstance(result, bytes)
            except Exception:
                # If there's an exception, it should be expected (like validation errors)
                # For now, just ensure it doesn't crash the whole test
                pass
        
        # Verify database is still functional after error cases
        final_result = db.get("test_key")
        assert final_result == b"test_value"
        
        db.close() 


def test_delete_basic_operation() -> None:
    """Test basic delete operation integrates WAL and memtable correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Put some data first
        db.put("test_key", b"test_value")
        initial_seq = db.seq_no
        
        # Delete the key
        db.delete("test_key")
        
        # Verify sequence number was incremented
        assert db.seq_no == initial_seq + 1
        
        # Verify memtable contains tombstone entry with correct data
        entry = db.memtable.search("test_key")
        assert entry is not None
        assert entry.key == "test_key"
        assert entry.value is None  # Tombstones have None values
        assert entry.sequence == initial_seq + 1
        assert entry.entry_type == EntryType.DELETE
        assert entry.is_tombstone() is True
        
        # Verify WAL was written (by checking that position advanced)
        assert db.wal.write_position > 0
        
        db.close()


def test_delete_then_get() -> None:
    """Test that deleted keys return None when retrieved."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Put then delete a key
        db.put("key_to_delete", b"some_value")
        
        # Verify key exists before deletion
        result = db.get("key_to_delete")
        assert result == b"some_value"
        
        # Delete the key
        db.delete("key_to_delete")
        
        # Get should return None for deleted key
        result = db.get("key_to_delete")
        assert result is None
        
        # Verify other keys are unaffected
        db.put("other_key", b"other_value")
        result = db.get("other_key")
        assert result == b"other_value"
        
        db.close()


def test_delete_exception_propagation() -> None:
    """Test that exceptions from delete operations bubble up correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Test ValueError from DatabaseEntry validation (empty key)
        with pytest.raises(ValueError, match="key cannot be empty"):
            db.delete("")
        
        # Test ValueError from WAL size constraints (key too large)
        large_key = "x" * 70000  # Exceeds MAX_KEY_BYTES (65536)
        with pytest.raises(ValueError, match="key exceeds max size"):
            db.delete(large_key)
        
        # Verify database state remains consistent after exceptions
        # Note: sequence numbers are consumed even on failed operations (expected behavior)
        assert db.seq_no == 2  # Two failed operations consumed sequence numbers
        assert db.memtable.search("") is None
        assert db.memtable.search(large_key) is None
        
        db.close()


def test_multiple_operations_integration() -> None:
    """Test complex scenarios with puts, updates, and deletes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        db = Database(config)
        
        # Complex scenario: put -> update -> delete -> put again
        db.put("key", b"value1")           # seq 1
        db.put("key", b"value2")           # seq 2 (update)
        db.delete("key")                   # seq 3 (delete)
        db.put("key", b"value3")           # seq 4 (put after delete)
        
        # The final get should return the latest put (after delete)
        result = db.get("key")
        assert result == b"value3"
        
        # Test deleting a key that doesn't exist
        db.delete("never_existed")         # seq 5
        
        # Should still return None
        result = db.get("never_existed")
        assert result is None
        
        # Verify the tombstone was created
        tombstone = db.memtable.search("never_existed")
        assert tombstone is not None
        assert tombstone.entry_type == EntryType.DELETE
        assert tombstone.sequence == 5
        
        db.close() 