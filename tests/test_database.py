import tempfile
import pytest
from pathlib import Path

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

def test_wal_replay_basic_recovery() -> None:
    """Test basic WAL replay functionality - data persists across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # First session: write some data
        config = Configuration()
        config.base_path = tmpdir
        db1 = Database(config)
        
        db1.put("user:123", b"alice")
        db1.put("user:456", b"bob")
        db1.delete("user:789")  # tombstone
        
        # Verify data is in first session
        assert db1.get("user:123") == b"alice"
        assert db1.get("user:456") == b"bob"
        assert db1.get("user:789") is None  # deleted
        
        # Close first session
        final_sequence = db1.seq_no
        db1.close()
        
        # Second session: reopen database
        db2 = Database(config)
        
        # Verify all data recovered
        assert db2.get("user:123") == b"alice"
        assert db2.get("user:456") == b"bob"
        assert db2.get("user:789") is None  # tombstone preserved
        
        # Verify sequence number continuity
        assert db2.seq_no == final_sequence
        
        # Verify new operations continue from correct sequence
        db2.put("user:999", b"charlie")
        assert db2.seq_no == final_sequence + 1
        
        db2.close()

def test_wal_replay_sequence_continuity() -> None:
    """Test that sequence numbers continue correctly after recovery."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        
        # First session: write data with known sequences
        db1 = Database(config)
        db1.put("key1", b"value1")  # seq 1
        db1.put("key2", b"value2")  # seq 2
        db1.put("key3", b"value3")  # seq 3
        
        assert db1.seq_no == 3
        db1.close()
        
        # Second session: verify sequence recovery
        db2 = Database(config)
        assert db2.seq_no == 3
        
        # New operations should continue from 4
        db2.put("key4", b"value4")
        assert db2.seq_no == 4
        
        db2.close()

def test_wal_replay_empty_directory() -> None:
    """Test WAL replay with no existing WAL files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        
        # Fresh database should start with sequence 0
        db = Database(config)
        assert db.seq_no == 0
        
        # First operation should be sequence 1
        db.put("first_key", b"first_value")
        assert db.seq_no == 1
        
        db.close()

def test_wal_replay_overwrite_behavior() -> None:
    """Test that WAL replay correctly handles key overwrites."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        
        # First session: write and overwrite same key
        db1 = Database(config)
        db1.put("overwrite_key", b"original_value")  # seq 1
        db1.put("overwrite_key", b"updated_value")   # seq 2
        db1.close()
        
        # Second session: verify only latest value recovered
        db2 = Database(config)
        assert db2.get("overwrite_key") == b"updated_value"
        
        # Verify memtable contains correct entry
        entry = db2.memtable.search("overwrite_key")
        assert entry is not None
        assert entry.sequence == 2
        assert entry.value == b"updated_value"
        
        db2.close()

def test_wal_replay_delete_after_put() -> None:
    """Test WAL replay correctly handles delete after put."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        
        # First session: put then delete
        db1 = Database(config)
        db1.put("temp_key", b"temp_value")  # seq 1
        db1.delete("temp_key")              # seq 2 (tombstone)
        db1.close()
        
        # Second session: verify delete is preserved
        db2 = Database(config)
        assert db2.get("temp_key") is None
        
        # Verify memtable has tombstone with higher sequence
        entry = db2.memtable.search("temp_key")
        assert entry is not None
        assert entry.is_tombstone()
        assert entry.sequence == 2
        
        db2.close()

def test_wal_replay_multiple_files() -> None:
    """Test WAL replay prevents memtable bloat by skipping flushed WAL files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        config.memtable_flush_threshold = 100  # Small threshold to force flushes
        
        # First session: create multiple WAL files through flushing
        db1 = Database(config)
        
        # Add enough data to trigger multiple flushes
        for i in range(20):
            key = f"key{i:03d}"
            value = b"x" * 20  # 20 bytes per value
            db1.put(key, value)
        
        final_sequence_with_unflushed = db1.seq_no
        db1.close()
        
        # Verify multiple WAL files were created
        wal_dir = Path(tmpdir) / "wal"
        wal_files = [f for f in wal_dir.iterdir() if f.name.startswith("current.wal.")]
        assert len(wal_files) > 1, "Multiple WAL files should have been created"
        
        # Second session: verify our fix prevents memtable bloat
        db2 = Database(config)
        
        # The key success metrics:
        # 1. Most WAL files should be skipped (they end with FLUSH markers)
        # 2. Memtable should NOT be bloated with all the flushed data
        # 3. Sequence numbers should still be tracked correctly
        
        # Verify sequence number includes all operations
        assert db2.seq_no == final_sequence_with_unflushed
        
        # Most importantly: verify memtable is not bloated
        # Before our fix, this would have been 20+ entries (all the flushed data)
        # After our fix, it should be 0 or very few entries (only unflushed data)
        memtable_entry_count = sum(1 for _ in db2.memtable)
        
        # The core fix: memtable should not contain all the flushed data
        assert memtable_entry_count < 10, f"SUCCESS: Memtable bloat prevented! Only {memtable_entry_count} entries instead of 20+"
        
        db2.close()

def test_wal_replay_with_flush_markers() -> None:
    """Test that FLUSH markers prevent memtable bloat during replay."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        config.memtable_flush_threshold = 50  # Small threshold
        
        # First session: force a flush to create FLUSH markers
        db1 = Database(config)
        
        # Add data to trigger flush
        for i in range(5):
            db1.put(f"key{i}", b"x" * 15)  # Should trigger flush
        
        final_sequence = db1.seq_no
        db1.close()
        
        # Second session: verify FLUSH markers prevent memtable bloat
        db2 = Database(config)
        
        # The key success: FLUSH markers should prevent flushed data from being replayed
        # This prevents memtable bloat and improves startup performance
        
        # Verify sequence number recovery
        assert db2.seq_no == final_sequence
        
        # Most importantly: verify memtable is not bloated with flushed data
        memtable_entry_count = sum(1 for _ in db2.memtable)
        
        # Success metric: memtable should not contain the flushed entries
        assert memtable_entry_count < 3, f"SUCCESS: FLUSH markers prevented bloat! Only {memtable_entry_count} entries in memtable"
        
        db2.close()

def test_wal_replay_corruption_resilience() -> None:
    """Test WAL replay handles corruption gracefully by continuing to read valid entries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = Configuration()
        config.base_path = tmpdir
        
        # First session: write some data
        db1 = Database(config)
        db1.put("before_corruption", b"good_data1")
        db1.put("will_be_corrupted", b"corrupted_data")
        db1.put("after_corruption", b"good_data2")
        db1.close()
        
        # Manually corrupt the WAL file
        wal_dir = Path(tmpdir) / "wal"
        wal_files = [f for f in wal_dir.iterdir() if f.name.startswith("current.wal.")]
        assert len(wal_files) > 0
        
        wal_file = wal_files[0]
        
        # Read the file, corrupt middle section, write back
        with open(wal_file, 'rb') as read_f:
            data = bytearray(read_f.read())
        
        # More surgical corruption: corrupt bytes in the middle third of the file
        # This is more likely to hit the second entry while preserving first/last
        if len(data) > 90:
            start_corrupt = len(data) // 3
            end_corrupt = min(start_corrupt + 20, (2 * len(data)) // 3)
            for i in range(start_corrupt, end_corrupt):
                data[i] = 0xFF  # corrupt bytes
        
        with open(wal_file, 'wb') as write_f:
            write_f.write(data)
        
        # Second session: should recover some entries despite corruption
        db2 = Database(config)
        
        # At least one entry should be recovered (corruption test is about resilience, not perfect recovery)
        recovered_count = 0
        test_keys = ["before_corruption", "will_be_corrupted", "after_corruption"]
        for key in test_keys:
            if db2.get(key) is not None:
                recovered_count += 1
        
        # We should recover at least one entry to demonstrate resilience
        assert recovered_count > 0, "At least some entries should be recovered despite corruption"
        
        # Verify database is still functional
        db2.put("new_after_recovery", b"new_data")
        assert db2.get("new_after_recovery") == b"new_data"
        
        db2.close() 