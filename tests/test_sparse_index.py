import os
import tempfile

from src.sstable import SSTableWriter, SSTableReader, IndexEntry
from src.entry import DatabaseEntry


def test_index_entry_serialization() -> None:
    """Test IndexEntry serialization and deserialization."""
    entry = IndexEntry("test_key", 12345)
    
    # Serialize
    serialized = entry.serialize()
    assert len(serialized) > 0
    
    # Deserialize
    deserialized, bytes_consumed = IndexEntry.deserialize(serialized)
    assert deserialized.key == "test_key"
    assert deserialized.file_offset == 12345
    assert bytes_consumed == len(serialized)


def test_sparse_index_creation() -> None:
    """Test that sparse index is created during SSTable writing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sst_path = os.path.join(tmpdir, "test.sst")
        
        # Create SSTable with small index interval for testing
        with SSTableWriter(sst_path, index_interval=2) as writer:
            # Add entries - every 2nd entry should be indexed
            writer.add_entry(DatabaseEntry.put("key001", 1, b"value1"))  # Indexed (entry 0)
            writer.add_entry(DatabaseEntry.put("key002", 2, b"value2"))  # Not indexed
            writer.add_entry(DatabaseEntry.put("key003", 3, b"value3"))  # Indexed (entry 2)
            writer.add_entry(DatabaseEntry.put("key004", 4, b"value4"))  # Not indexed
            writer.add_entry(DatabaseEntry.put("key005", 5, b"value5"))  # Indexed (entry 4)
            actual_filepath = writer.filepath
        
        # Read back with reader using actual file path
        reader = SSTableReader(actual_filepath)
        
        # Should have 3 index entries (entries 0, 2, 4)
        assert len(reader._index_entries) == 3
        assert reader._index_entries[0].key == "key001"
        assert reader._index_entries[1].key == "key003"
        assert reader._index_entries[2].key == "key005"
        
        reader.close()


def test_sparse_index_lookup() -> None:
    """Test key lookup using sparse index."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sst_path = os.path.join(tmpdir, "test.sst")
        
        # Create SSTable with entries
        with SSTableWriter(sst_path, index_interval=3) as writer:
            for i in range(10):
                key = f"key{i:03d}"
                value = f"value{i}".encode()
                writer.add_entry(DatabaseEntry.put(key, i, value))
            actual_filepath = writer.filepath
        
        # Test lookups
        reader = SSTableReader(actual_filepath)
        
        # Test existing keys
        for i in range(10):
            key = f"key{i:03d}"
            entry = reader.get(key)
            assert entry is not None
            assert entry.key == key
            assert entry.sequence == i
            assert entry.value == f"value{i}".encode()
        
        # Test non-existent keys
        assert reader.get("key999") is None
        assert reader.get("nonexistent") is None
        assert reader.get("key000a") is None
        
        reader.close()


def test_sparse_index_binary_search() -> None:
    """Test that sparse index uses binary search correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sst_path = os.path.join(tmpdir, "test.sst")
        
        # Create larger SSTable to test binary search
        with SSTableWriter(sst_path, index_interval=5) as writer:
            for i in range(100):
                key = f"key{i:05d}"  # 5-digit zero-padded keys
                value = f"value{i}".encode()
                writer.add_entry(DatabaseEntry.put(key, i, value))
            actual_filepath = writer.filepath
        
        reader = SSTableReader(actual_filepath)
        
        # Should have 20 index entries (0, 5, 10, 15, ..., 95)
        assert len(reader._index_entries) == 20
        
        # Test lookup that requires binary search
        # key00037 should be found between index entries key00035 and key00040
        entry = reader.get("key00037")
        assert entry is not None
        assert entry.key == "key00037"
        assert entry.sequence == 37
        
        # Test edge cases
        assert reader.get("key00000") is not None  # First key
        assert reader.get("key00099") is not None  # Last key
        assert reader.get("key00050") is not None  # Exactly on index boundary
        
        reader.close()


def test_sparse_index_with_tombstones() -> None:
    """Test sparse index with tombstone entries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sst_path = os.path.join(tmpdir, "test.sst")
        
        with SSTableWriter(sst_path, index_interval=2) as writer:
            writer.add_entry(DatabaseEntry.put("key001", 1, b"value1"))      # Regular entry
            writer.add_entry(DatabaseEntry.delete("key002", 2))              # Tombstone
            writer.add_entry(DatabaseEntry.put("key003", 3, b"value3"))      # Regular entry
            writer.add_entry(DatabaseEntry.delete("key004", 4))              # Tombstone
            actual_filepath = writer.filepath
        
        reader = SSTableReader(actual_filepath)
        
        # Test lookup of regular entries
        entry1 = reader.get("key001")
        assert entry1 is not None
        assert entry1.value == b"value1"
        assert not entry1.is_tombstone()
        
        entry3 = reader.get("key003")
        assert entry3 is not None
        assert entry3.value == b"value3"
        assert not entry3.is_tombstone()
        
        # Test lookup of tombstones
        entry2 = reader.get("key002")
        assert entry2 is not None
        assert entry2.value is None
        assert entry2.is_tombstone()
        
        entry4 = reader.get("key004")
        assert entry4 is not None
        assert entry4.value is None
        assert entry4.is_tombstone()
        
        reader.close()


def test_no_index_fallback() -> None:
    """Test fallback to linear scan when index has minimal entries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sst_path = os.path.join(tmpdir, "test.sst")
        
        # Create SSTable with very large index interval (minimal index entries)
        with SSTableWriter(sst_path, index_interval=1000) as writer:
            for i in range(5):  # Only 5 entries, so only entry 0 gets indexed
                key = f"key{i:03d}"
                value = f"value{i}".encode()
                writer.add_entry(DatabaseEntry.put(key, i, value))
            actual_filepath = writer.filepath
        
        reader = SSTableReader(actual_filepath)
        
        # Should have only 1 index entry (entry 0)
        assert len(reader._index_entries) == 1
        assert reader._index_entries[0].key == "key000"
        
        # Should still be able to find keys via linear scan
        for i in range(5):
            key = f"key{i:03d}"
            entry = reader.get(key)
            assert entry is not None
            assert entry.key == key
            assert entry.sequence == i
        
        # Non-existent key should return None
        assert reader.get("key999") is None
        
        reader.close()


def test_index_performance_benefit() -> None:
    """Test that index provides performance benefit (reduced seeks)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sst_path = os.path.join(tmpdir, "test.sst")
        
        # Create SSTable with many entries
        with SSTableWriter(sst_path, index_interval=10) as writer:
            for i in range(1000):
                key = f"key{i:06d}"  # 6-digit zero-padded
                value = f"value{i}".encode()
                writer.add_entry(DatabaseEntry.put(key, i, value))
            actual_filepath = writer.filepath
        
        reader = SSTableReader(actual_filepath)
        
        # Should have 100 index entries
        assert len(reader._index_entries) == 100
        
        # Test lookup near the end - should start scan from appropriate index point
        entry = reader.get("key000999")  # Last entry
        assert entry is not None
        assert entry.key == "key000999"
        assert entry.sequence == 999
        
        # Test lookup in middle
        entry = reader.get("key000500")
        assert entry is not None
        assert entry.key == "key000500"
        assert entry.sequence == 500
        
        reader.close()


def test_unicode_keys_in_index() -> None:
    """Test sparse index with Unicode keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sst_path = os.path.join(tmpdir, "test.sst")
        
        unicode_keys = ["αα", "ββ", "γγ", "δδ", "εε"]
        
        with SSTableWriter(sst_path, index_interval=2) as writer:
            for i, key in enumerate(unicode_keys):
                value = f"value{i}".encode()
                writer.add_entry(DatabaseEntry.put(key, i, value))
            actual_filepath = writer.filepath
        
        reader = SSTableReader(actual_filepath)
        
        # Test Unicode key lookups
        for i, key in enumerate(unicode_keys):
            entry = reader.get(key)
            assert entry is not None
            assert entry.key == key
            assert entry.sequence == i
        
        reader.close() 