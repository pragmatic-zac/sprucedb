#!/usr/bin/env python3
"""
Sparse Index Demonstration

This example demonstrates the sparse index functionality in SSTables,
showing how it provides O(log n) lookup performance instead of O(n) linear scans.
"""

import os
import tempfile
import time
from typing import List

from src.sstable import SSTableWriter, SSTableReader
from src.entry import DatabaseEntry


def create_large_sstable(path: str, num_entries: int, index_interval: int) -> str:
    """Create a large SSTable with the specified number of entries."""
    print(f"Creating SSTable with {num_entries:,} entries (index interval: {index_interval})...")
    
    with SSTableWriter(path, index_interval=index_interval) as writer:
        for i in range(num_entries):
            key = f"key{i:08d}"  # 8-digit zero-padded keys
            value = f"This is value number {i} with some extra data to make it realistic".encode()
            writer.add_entry(DatabaseEntry.put(key, i, value))
        actual_path = writer.filepath
    
    print(f"✓ SSTable created at: {actual_path}")
    return actual_path


def benchmark_lookups(reader: SSTableReader, keys_to_find: List[str]) -> float:
    """Benchmark lookup performance and return average time per lookup."""
    start_time = time.time()
    
    found_count = 0
    for key in keys_to_find:
        entry = reader.get(key)
        if entry is not None:
            found_count += 1
    
    end_time = time.time()
    total_time = end_time - start_time
    avg_time = total_time / len(keys_to_find)
    
    print(f"  Found {found_count}/{len(keys_to_find)} keys")
    print(f"  Total time: {total_time:.4f}s")
    print(f"  Average time per lookup: {avg_time*1000:.2f}ms")
    
    return avg_time


def demonstrate_sparse_index() -> None:
    """Demonstrate sparse index functionality and performance."""
    print("=" * 60)
    print("SPARSE INDEX DEMONSTRATION")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Configuration
        num_entries = 100_000
        test_keys = [
            "key00000001",  # Near beginning
            "key00025000",  # First quarter
            "key00050000",  # Middle
            "key00075000",  # Third quarter
            "key00099999",  # Near end
            "key00012345",  # Random position
            "key00067890",  # Another random position
        ]
        
        # Test 1: SSTable with sparse index (every 100 entries)
        print("\n1. SPARSE INDEX (interval=100)")
        print("-" * 40)
        
        indexed_path = create_large_sstable(
            os.path.join(tmpdir, "indexed.sst"), 
            num_entries, 
            index_interval=100
        )
        
        indexed_reader = SSTableReader(indexed_path)
        print(f"Index entries created: {len(indexed_reader._index_entries):,}")
        print(f"Index coverage: Every {100} entries")
        print(f"Index size reduction: {100/len(indexed_reader._index_entries):.1f}x smaller than full index")
        
        print("\nPerforming lookups with sparse index:")
        indexed_time = benchmark_lookups(indexed_reader, test_keys)
        
        # Test 2: SSTable with minimal index (every 10,000 entries)
        print("\n2. MINIMAL INDEX (interval=10,000)")
        print("-" * 40)
        
        minimal_path = create_large_sstable(
            os.path.join(tmpdir, "minimal.sst"), 
            num_entries, 
            index_interval=10_000
        )
        
        minimal_reader = SSTableReader(minimal_path)
        print(f"Index entries created: {len(minimal_reader._index_entries):,}")
        print(f"Index coverage: Every {10_000} entries")
        
        print("\nPerforming lookups with minimal index:")
        minimal_time = benchmark_lookups(minimal_reader, test_keys)
        
        # Test 3: SSTable with very sparse index (essentially linear scan)
        print("\n3. VERY SPARSE INDEX (interval=100,000)")
        print("-" * 40)
        
        sparse_path = create_large_sstable(
            os.path.join(tmpdir, "sparse.sst"), 
            num_entries, 
            index_interval=100_000
        )
        
        sparse_reader = SSTableReader(sparse_path)
        print(f"Index entries created: {len(sparse_reader._index_entries):,}")
        print("Index coverage: Only first entry indexed")
        
        print("\nPerforming lookups with very sparse index (mostly linear scan):")
        sparse_time = benchmark_lookups(sparse_reader, test_keys)
        
        # Performance comparison
        print("\n" + "=" * 60)
        print("PERFORMANCE COMPARISON")
        print("=" * 60)
        
        print(f"Sparse index (100):     {indexed_time*1000:.2f}ms avg")
        print(f"Minimal index (10K):    {minimal_time*1000:.2f}ms avg")
        print(f"Very sparse (100K):     {sparse_time*1000:.2f}ms avg")
        
        if sparse_time > indexed_time:
            speedup = sparse_time / indexed_time
            print(f"\nSparse index is {speedup:.1f}x faster than linear scan!")
        
        # Index space analysis
        print("\n" + "=" * 60)
        print("INDEX SPACE ANALYSIS")
        print("=" * 60)
        
        file_size = os.path.getsize(indexed_path)
        print(f"SSTable file size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
        
        # Estimate index size (rough calculation)
        avg_key_size = 12  # "key00000000" = 11 chars + some overhead
        index_entry_size = avg_key_size + 8  # key + 8-byte offset
        
        indexed_index_size = len(indexed_reader._index_entries) * index_entry_size
        minimal_index_size = len(minimal_reader._index_entries) * index_entry_size
        sparse_index_size = len(sparse_reader._index_entries) * index_entry_size
        
        print(f"Sparse index size (100):   {indexed_index_size:,} bytes ({indexed_index_size/file_size*100:.2f}% of file)")
        print(f"Minimal index size (10K):  {minimal_index_size:,} bytes ({minimal_index_size/file_size*100:.2f}% of file)")
        print(f"Very sparse size (100K):   {sparse_index_size:,} bytes ({sparse_index_size/file_size*100:.2f}% of file)")
        
        # Demonstrate index structure
        print("\n" + "=" * 60)
        print("INDEX STRUCTURE EXAMPLE")
        print("=" * 60)
        
        print("First 10 index entries from sparse index (interval=100):")
        for i, entry in enumerate(indexed_reader._index_entries[:10]):
            print(f"  {i:2d}: key='{entry.key}' offset={entry.file_offset}")
        
        if len(indexed_reader._index_entries) > 10:
            print(f"  ... ({len(indexed_reader._index_entries)-10} more entries)")
        
        # Cleanup
        indexed_reader.close()
        minimal_reader.close()
        sparse_reader.close()


def demonstrate_binary_search() -> None:
    """Demonstrate how binary search works in the sparse index."""
    print("\n" + "=" * 60)
    print("BINARY SEARCH DEMONSTRATION")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a smaller SSTable for clearer demonstration
        num_entries = 1000
        index_interval = 50  # Index every 50th entry
        
        path = create_large_sstable(
            os.path.join(tmpdir, "demo.sst"), 
            num_entries, 
            index_interval
        )
        
        reader = SSTableReader(path)
        
        print(f"\nSSTable has {num_entries} entries with index interval {index_interval}")
        print(f"Index contains {len(reader._index_entries)} entries:")
        
        # Show index structure
        for i, entry in enumerate(reader._index_entries):
            print(f"  Index[{i:2d}]: key='{entry.key}' (entry #{i*index_interval})")
        
        # Demonstrate lookup process
        target_key = "key00000275"  # Between index entries
        print(f"\nLooking up key: '{target_key}'")
        print("Binary search process:")
        
        # Simulate the binary search process
        left, right = 0, len(reader._index_entries) - 1
        steps = 0
        
        while left <= right:
            steps += 1
            mid = (left + right) // 2
            mid_key = reader._index_entries[mid].key
            
            print(f"  Step {steps}: Check index[{mid}] = '{mid_key}'")
            
            if mid_key <= target_key:
                print(f"    '{mid_key}' <= '{target_key}' → search right half")
                left = mid + 1
            else:
                print(f"    '{mid_key}' > '{target_key}' → search left half")
                right = mid - 1
        
        # Find the starting position
        start_idx = right if right >= 0 else 0
        start_key = reader._index_entries[start_idx].key
        start_offset = reader._index_entries[start_idx].file_offset
        
        print(f"\nBinary search complete in {steps} steps!")
        print(f"Start linear scan from index[{start_idx}] = '{start_key}' at offset {start_offset}")
        print(f"This avoids scanning {start_idx * index_interval} entries!")
        
        # Verify the lookup works
        found_entry = reader.get(target_key)
        if found_entry:
            print(f"✓ Found: '{found_entry.key}' with sequence {found_entry.sequence}")
        else:
            print("✗ Key not found")
        
        reader.close()


if __name__ == "__main__":
    demonstrate_sparse_index()
    demonstrate_binary_search()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("✓ Sparse indexes provide O(log n) lookup performance")
    print("✓ Index interval controls space/time tradeoff")
    print("✓ Binary search minimizes entries to scan")
    print("✓ Fallback to linear scan when index is insufficient")
    print("✓ Index size is typically <1% of total file size") 