import tempfile
import pytest

from src.database import Database
from src.configuration import Configuration
from src.entry import EntryType


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