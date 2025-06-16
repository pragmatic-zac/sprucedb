#!/usr/bin/env python3
"""
Example showing how sequence numbers work consistently across WAL and SSTable components
in an LSM tree implementation.
"""

import tempfile
import os
from typing import List, Dict

from src.wal import WriteAheadLog, WALOperationType
from src.sstable import SSTableWriter
from src.entry import DatabaseEntry

class SimpleLSMExample:
    """
    Simple example showing how sequence numbers provide consistent ordering
    across WAL and SSTable components.
    """
    
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.wal = WriteAheadLog(os.path.join(base_path, "wal"))
        self.sequence_counter = 0
        
    def get_next_sequence(self) -> int:
        """Get the next global sequence number."""
        current = self.sequence_counter
        self.sequence_counter += 1
        return current
    
    def put(self, key: str, value: bytes) -> int:
        """Put a key-value pair, returning the sequence number used."""
        sequence = self.get_next_sequence()
        self.wal.write_to_log(WALOperationType.PUT, key, sequence, value)
        print(f"PUT {key}={value.decode('utf-8')} with sequence={sequence}")
        return sequence
    
    def delete(self, key: str) -> int:
        """Delete a key, returning the sequence number used."""
        sequence = self.get_next_sequence()
        self.wal.write_to_log(WALOperationType.DELETE, key, sequence)
        print(f"DELETE {key} with sequence={sequence}")
        return sequence
    
    def flush_to_sstable(self, sstable_id: str) -> str:
        """
        Simulate flushing WAL entries to SSTable.
        Note: This is simplified for demonstration. In a real implementation,
        you would flush from memtable to SSTable.
        """
        print(f"\n--- Flushing to SSTable {sstable_id} ---")
        
        # For demo purposes, create some sample entries with sequence numbers
        # In real implementation, these would come from memtable
        sample_entries = [
            DatabaseEntry.put("user:1", 2, b"alice_updated"),  # Latest version
            DatabaseEntry.put("user:3", 4, b"charlie"),
        ]
        
        # Create SSTable with same sequence numbers
        sstable_path = os.path.join(self.base_path, f"sst_{sstable_id}")
        with SSTableWriter(sstable_path) as writer:
            # Sort by key (maintaining sequence order for same key)
            sample_entries.sort()
            
            for entry in sample_entries:
                writer.add_entry(entry)
                print(f"  Added to SSTable: {entry.key} (seq={entry.sequence})")
        
        # Rotate WAL
        old_wal = self.wal.rotate(sstable_id)
        print(f"Rotated WAL: {old_wal}")
        
        return sstable_path
    
    def demonstrate_compaction_logic(self, entries: List[DatabaseEntry]) -> None:
        """
        Demonstrate how sequence numbers are used during compaction.
        When multiple entries have the same key, the one with highest sequence wins.
        """
        print("\n--- Compaction Logic Demonstration ---")
        print("Entries before compaction:")
        for entry in entries:
            value_str = entry.value.decode('utf-8') if entry.value else "None"
            print(f"  {entry.key} = {value_str} (seq={entry.sequence})")
        
        # Group by key
        key_groups: Dict[str, List[DatabaseEntry]] = {}
        for entry in entries:
            if entry.key not in key_groups:
                key_groups[entry.key] = []
            key_groups[entry.key].append(entry)
        
        # For each key, keep only the entry with highest sequence number
        compacted_entries = []
        for key, group in key_groups.items():
            # Sort by sequence number, take the highest (most recent)
            winner = max(group, key=lambda e: e.sequence)
            compacted_entries.append(winner)
            print(f"  Key '{key}': keeping seq={winner.sequence}, discarding others")
        
        print("\nEntries after compaction:")
        for entry in sorted(compacted_entries):
            value_str = entry.value.decode('utf-8') if entry.value else "None"
            print(f"  {entry.key} = {value_str} (seq={entry.sequence})")
    
    def close(self) -> None:
        """Close the WAL."""
        self.wal.close()


if __name__ == "__main__":
    # Example usage
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Working in: {tmpdir}")
        
        lsm = SimpleLSMExample(tmpdir)
        
        # Insert some data
        lsm.put("user:1", b"alice")
        lsm.put("user:2", b"bob") 
        lsm.put("user:1", b"alice_updated")  # Update user:1
        lsm.delete("user:2")                  # Delete user:2
        lsm.put("user:3", b"charlie")
        
        # Flush to SSTable (in real implementation, this would happen automatically)
        sstable_path = lsm.flush_to_sstable("001")
        
        # More operations in new WAL
        lsm.put("user:4", b"diana")
        lsm.put("user:1", b"alice_final")    # Another update to user:1
        
        # Demonstrate compaction logic with overlapping entries
        # Simulate entries from multiple SSTables with same keys but different sequences
        overlapping_entries = [
            DatabaseEntry.put("user:1", 0, b"alice"),         # Original
            DatabaseEntry.put("user:1", 2, b"alice_updated"), # First update  
            DatabaseEntry.put("user:1", 6, b"alice_final"),   # Latest update
            DatabaseEntry.put("user:2", 1, b"bob"),           # Original
            DatabaseEntry.delete("user:2", 3),       # Deleted (tombstone)
            DatabaseEntry.put("user:3", 4, b"charlie"),       # Only version
        ]
        
        lsm.demonstrate_compaction_logic(overlapping_entries)
        
        lsm.close()
        
        print(f"\nFinal sequence counter: {lsm.sequence_counter}")
        print("\nKey takeaway: Sequence numbers provide total ordering across")
        print("WAL entries, memtable entries, and SSTable entries, making")
        print("compaction decisions deterministic and correct.") 