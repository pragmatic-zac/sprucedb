#!/usr/bin/env python3
"""
Demonstration of the unified entry interface in SpruceDB.

This script shows how the DatabaseEntry provides a common interface
that both WAL and SSTable can use, enabling seamless conversion between formats.
"""

import tempfile
import os
from src.entry import DatabaseEntry
from src.wal import WALEntry, WriteAheadLog, WALOperationType
from src.sstable import SSTableWriter


def main() -> None:
    print("=== SpruceDB Unified Entry Interface Demo ===\n")
    
    # Create some unified database entries
    print("1. Creating unified DatabaseEntry objects:")
    put_entry = DatabaseEntry.put("user:123", 1, b"John Doe", 1234567890)
    delete_entry = DatabaseEntry.delete("user:456", 2, 1234567891)
    
    print(f"   PUT entry: {put_entry}")
    print(f"   DELETE entry: {delete_entry}")
    print(f"   Is tombstone? PUT={put_entry.is_tombstone()}, DELETE={delete_entry.is_tombstone()}")
    print()
    
    # Demonstrate WAL conversion
    print("2. Converting to WAL format:")
    wal_put = WALEntry.from_database_entry(put_entry)
    wal_delete = WALEntry.from_database_entry(delete_entry)
    
    print(f"   WAL PUT: key={wal_put.key}, op={wal_put.op_type}, timestamp={wal_put.timestamp}")
    print(f"   WAL DELETE: key={wal_delete.key}, op={wal_delete.op_type}, timestamp={wal_delete.timestamp}")
    print()
    
    # Demonstrate SSTable conversion
    print("3. Converting to SSTable format:")
    # Create new entries with explicit value handling
    sst_put = DatabaseEntry.put(put_entry.key, put_entry.sequence, put_entry.value or b"")
    sst_delete = DatabaseEntry.delete(delete_entry.key, delete_entry.sequence)
    
    print(f"   SSTable PUT: key={sst_put.key}, value={sst_put.value!r}, tombstone={sst_put.is_tombstone()}")
    print(f"   SSTable DELETE: key={sst_delete.key}, value={sst_delete.value!r}, tombstone={sst_delete.is_tombstone()}")
    print()
    
    # Demonstrate round-trip conversion
    print("4. Round-trip conversion (DatabaseEntry -> WAL -> DatabaseEntry):")
    converted_back = wal_put.to_database_entry()
    print(f"   Original:  {put_entry}")
    print(f"   Converted: {converted_back}")
    print(f"   Data preserved: {put_entry.key == converted_back.key and put_entry.value == converted_back.value}")
    print()
    
    # Demonstrate serialization consistency
    print("5. Serialization and storage:")
    with tempfile.TemporaryDirectory() as tmpdir:
        # WAL storage
        wal_path = os.path.join(tmpdir, "demo.wal")
        with WriteAheadLog(wal_path) as wal:
            pos1 = wal.write_to_log(WALOperationType.PUT, put_entry.key, put_entry.sequence, put_entry.value)
            pos2 = wal.write_to_log(WALOperationType.DELETE, delete_entry.key, delete_entry.sequence)
            print(f"   WAL: Wrote entries at positions {pos1} and {pos2}")
            
            # Read back from WAL
            read_entry1 = wal.read_log_entry(pos1)
            read_entry2 = wal.read_log_entry(pos2)
            if read_entry1 and read_entry2:
                print(f"   WAL: Read back {read_entry1.key} and {read_entry2.key}")
        
        # SSTable storage
        sst_path = os.path.join(tmpdir, "demo.sst")
        with SSTableWriter(sst_path) as writer:
            writer.add_entry(sst_put)
            writer.add_entry(sst_delete)
            print(f"   SSTable: Wrote {writer.entry_count} entries to {writer.filepath}")
    
    print()
    print("6. Key benefits of unified interface:")
    print("   ✓ Consistent data representation across storage layers")
    print("   ✓ Easy conversion between WAL and SSTable formats")
    print("   ✓ Proper tombstone handling for deletions")
    print("   ✓ Type safety and validation")
    print("   ✓ Simplified flush operations (WAL -> SSTable)")
    print()
    print("=== Demo Complete ===")


if __name__ == "__main__":
    main() 