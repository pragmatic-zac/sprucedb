import os
import struct
import zlib
from dataclasses import dataclass
from datetime import datetime
from enum import Flag, auto
from typing import Final, Optional, Tuple, BinaryIO, List

from .entry import DatabaseEntry

"""
SSTable format
[HEADER] (48 bytes total)
- Magic number (4 bytes): "SPDB" in ASCII
- Version number (2 bytes)
- Feature flags (4 bytes):
  * Bit 0: Compression enabled
  * Bit 1: Bloom filter present
  * Bit 2: Block-based format
  * Bits 3-31: Reserved for future use
- Reserved space (16 bytes)
- Creation timestamp (8 bytes)
- Entry count (4 bytes)
- Data section size (8 bytes)
- Header checksum (4 bytes) - CRC32 of all previous header fields

[DATA SECTION]
Sorted sequence of entries:
- Sequence number (8 bytes)
- Key length (4 bytes)
- Key (UTF-8 encoded)
- Value length (4 bytes)
- Value (bytes)
... (repeating for each entry)

[FOOTER] (16 bytes)
- Data checksum (4 bytes) - CRC32 of entire data section
- Index offset (8 bytes) - for future optimizations
- Footer checksum (4 bytes) - CRC32 of previous footer fields
"""

# file identification
SSTABLE_MAGIC: Final[bytes] = b"SPDB"
SSTABLE_VERSION: Final[int] = 1

# maximum sizes
MAX_KEY_SIZE: Final[int] = 65536  # same as WAL
MAX_VALUE_SIZE: Final[int] = 1024 * 1024  # 1MB max value size, consistent with WAL

# Struct formats using network byte order (!)
# Header format (50 bytes total):
# - Magic number (4s = 4 bytes string)
# - Version (H = 2 bytes unsigned short)
# - Feature flags (I = 4 bytes unsigned int)
# - Reserved space (16s = 16 bytes)
# - Timestamp (Q = 8 bytes unsigned long)
# - Entry count (I = 4 bytes unsigned int)
# - Data section size (Q = 8 bytes unsigned long)
# - Header checksum (I = 4 bytes unsigned int)
HEADER_FORMAT: Final[str] = "!4sHI16sQIQI"

# Footer format (16 bytes total):
# - Data checksum (I = 4 bytes unsigned int)
# - Index offset (Q = 8 bytes unsigned long)
# - Footer checksum (I = 4 bytes unsigned int)
FOOTER_FORMAT: Final[str] = "!IQI"

# Index format:
# - Index entry count (I = 4 bytes unsigned int)
# - For each index entry:
#   - Key length (I = 4 bytes unsigned int)
#   - Key (UTF-8 encoded, variable)
#   - File offset (Q = 8 bytes unsigned long)
INDEX_HEADER_FORMAT: Final[str] = "!I"  # entry count
INDEX_ENTRY_KEY_LEN_FORMAT: Final[str] = "!I"  # key length
INDEX_ENTRY_OFFSET_FORMAT: Final[str] = "!Q"  # file offset

# Entry format:
# - Sequence number (Q = 8 bytes unsigned long)
# - Key length (I = 4 bytes unsigned int)
# - Key (variable)
# - Value length (I = 4 bytes unsigned int)
# - Value (variable)
SEQUENCE_FORMAT: Final[str] = "!Q"       # format for sequence number
KEY_LENGTH_FORMAT: Final[str] = "!I"     # format for key length
VALUE_LENGTH_FORMAT: Final[str] = "!I"   # format for value length

# header/footer sizes
HEADER_SIZE: Final[int] = struct.calcsize(HEADER_FORMAT)
FOOTER_SIZE: Final[int] = struct.calcsize(FOOTER_FORMAT)
SEQUENCE_SIZE: Final[int] = struct.calcsize(SEQUENCE_FORMAT)
KEY_LEN_SIZE: Final[int] = struct.calcsize(KEY_LENGTH_FORMAT)
VALUE_LEN_SIZE: Final[int] = struct.calcsize(VALUE_LENGTH_FORMAT)
INDEX_HEADER_SIZE: Final[int] = struct.calcsize(INDEX_HEADER_FORMAT)
INDEX_KEY_LEN_SIZE: Final[int] = struct.calcsize(INDEX_ENTRY_KEY_LEN_FORMAT)
INDEX_OFFSET_SIZE: Final[int] = struct.calcsize(INDEX_ENTRY_OFFSET_FORMAT)

# Sparse index configuration
DEFAULT_INDEX_INTERVAL: Final[int] = 1000  # Index every Nth entry

class SSTableFeatureFlags(Flag):
    """Feature flags for SSTable format"""
    NONE = 0
    COMPRESSION = auto()      # uses compression
    BLOOM_FILTER = auto()     # has bloom filter
    BLOCK_BASED = auto()      # uses block-based format


@dataclass(frozen=True)
class IndexEntry:
    """Represents an entry in the sparse index."""
    key: str
    file_offset: int
    
    def serialize(self) -> bytes:
        """Serialize index entry to bytes."""
        key_bytes = self.key.encode("utf-8")
        key_len_bytes = struct.pack(INDEX_ENTRY_KEY_LEN_FORMAT, len(key_bytes))
        offset_bytes = struct.pack(INDEX_ENTRY_OFFSET_FORMAT, self.file_offset)
        return key_len_bytes + key_bytes + offset_bytes
    
    @classmethod
    def deserialize(cls, data: bytes) -> Tuple['IndexEntry', int]:
        """Deserialize bytes into IndexEntry. Returns (entry, bytes_consumed)."""
        if len(data) < INDEX_KEY_LEN_SIZE:
            raise ValueError("Data too short for key length")
            
        key_length = struct.unpack(INDEX_ENTRY_KEY_LEN_FORMAT, data[:INDEX_KEY_LEN_SIZE])[0]
        key_start = INDEX_KEY_LEN_SIZE
        key_end = key_start + key_length
        
        if len(data) < key_end + INDEX_OFFSET_SIZE:
            raise ValueError("Data too short for index entry")
            
        key = data[key_start:key_end].decode('utf-8')
        file_offset = struct.unpack(INDEX_ENTRY_OFFSET_FORMAT, data[key_end:key_end + INDEX_OFFSET_SIZE])[0]
        
        bytes_consumed = key_end + INDEX_OFFSET_SIZE
        return cls(key, file_offset), bytes_consumed


def serialize_entry(entry: DatabaseEntry) -> bytes:
    """
    Serialize DatabaseEntry to SSTable format bytes:
    [sequence][key_length][key][value_length][value]
    """
    key_bytes = entry.key.encode("utf-8")
    # Use empty bytes for DELETE entries (tombstones)
    value_bytes = entry.value if entry.value is not None else b''

    if len(key_bytes) > MAX_KEY_SIZE:
        raise ValueError(f"Key size exceeds max of {MAX_KEY_SIZE} bytes")

    if len(value_bytes) > MAX_VALUE_SIZE:
        raise ValueError(f"Value size exceeds max of {MAX_VALUE_SIZE} bytes")

    if entry.sequence < 0:
        raise ValueError("Sequence number must be non-negative")

    sequence_bytes = struct.pack(SEQUENCE_FORMAT, entry.sequence)
    key_len_bytes = struct.pack(KEY_LENGTH_FORMAT, len(key_bytes))
    value_len_bytes = struct.pack(VALUE_LENGTH_FORMAT, len(value_bytes))

    return sequence_bytes + key_len_bytes + key_bytes + value_len_bytes + value_bytes


def deserialize_entry(data: bytes) -> Tuple[DatabaseEntry, int]:
    """
    Deserialize bytes into DatabaseEntry.
    Returns tuple of (entry, bytes_consumed)
    """
    if len(data) < SEQUENCE_SIZE:
        raise ValueError("Data too short for sequence number")

    sequence = struct.unpack(SEQUENCE_FORMAT, data[:SEQUENCE_SIZE])[0]

    if len(data) < SEQUENCE_SIZE + KEY_LEN_SIZE:
        raise ValueError("Data too short for key length")

    key_length = struct.unpack(KEY_LENGTH_FORMAT, data[SEQUENCE_SIZE:SEQUENCE_SIZE + KEY_LEN_SIZE])[0]
    key_start = SEQUENCE_SIZE + KEY_LEN_SIZE
    key_end = key_start + key_length
    if len(data) < key_end:
        raise ValueError("Data too short for key")

    try:
        key = data[key_start:key_end].decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError("Invalid UTF-8 encoding in key")

    value_length_offset = key_end
    if len(data) < value_length_offset + VALUE_LEN_SIZE:
        raise ValueError("Data too short for value length")

    value_length = struct.unpack(VALUE_LENGTH_FORMAT,
                                 data[value_length_offset:value_length_offset + VALUE_LEN_SIZE])[0]

    value_offset = value_length_offset + VALUE_LEN_SIZE
    if len(data) < value_offset + value_length:
        raise ValueError("Data too short for value")

    value_bytes = data[value_offset:value_offset + value_length]
    
    # Convert empty values to None to represent tombstones (DELETE entries)
    if value_length == 0:
        entry = DatabaseEntry.delete(key, sequence)
    else:
        entry = DatabaseEntry.put(key, sequence, value_bytes)

    bytes_consumed = value_offset + value_length
    return entry, bytes_consumed


class SSTableWriter:
    def __init__(self, base_path: str, features: SSTableFeatureFlags = SSTableFeatureFlags.NONE, 
                 index_interval: int = DEFAULT_INDEX_INTERVAL):
        """Create new SSTable file with header"""
        self.base_path = base_path
        self.features = features
        self.entry_count = 0
        self.data_size = 0
        self._last_key: Optional[str] = None  # for enforcing sorted order
        self._file: Optional[BinaryIO] = None
        self._data_start_pos = 0
        self._data_crc = 0
        self._index_interval = index_interval
        self._index_entries: List[IndexEntry] = []

        os.makedirs(os.path.dirname(base_path), exist_ok=True)
        self.filepath = self._get_timestamped_path()
        self._file = open(self.filepath, 'wb')

        self.timestamp = int(datetime.utcnow().timestamp())

        header = struct.pack(
            HEADER_FORMAT,
            SSTABLE_MAGIC,
            SSTABLE_VERSION,
            features.value,
            b'\x00' * 16, # empty bytes for the placeholder
            self.timestamp,
            0, # entry count placeholder
            0, # data size placeholder
            0, # checksum placeholder
        )
        self._file.write(header)
        self._data_start_pos = self._file.tell()

    def _get_timestamped_path(self) -> str:
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        return f"{self.base_path}.{timestamp}"

    def add_entry(self, entry: DatabaseEntry) -> None:
        """ Add entry, enforcing sort order """
        if self._file is None:
            raise RuntimeError("File not initialized")
            
        if self._last_key is not None and self._last_key > entry.key:
            raise ValueError('Entries are not in sorted order')

        if self._last_key is not None and self._last_key == entry.key:
            raise ValueError(f'Duplicate key: {entry.key}')

        # Record position before writing for index
        current_position = self._file.tell()
        
        # Add to sparse index if this is an indexed entry
        if self.entry_count % self._index_interval == 0:
            index_entry = IndexEntry(entry.key, current_position)
            self._index_entries.append(index_entry)

        entry_bytes = serialize_entry(entry)
        self._file.write(entry_bytes)
        self._last_key = entry.key

        self._data_crc = zlib.crc32(entry_bytes, self._data_crc)
        self.entry_count += 1


    def finalize(self) -> None:
        """ Write header/footer, sync to disk, close file """
        if self._file is None:
            raise RuntimeError("File not initialized")
        
        # grab current position (end of data section)
        end_data_pos = self._file.tell()
        self.data_size = end_data_pos - self._data_start_pos

        # Write sparse index section
        index_offset = self._write_index()
        
        # calculate, pack, crc footer with index offset
        footer = struct.pack(FOOTER_FORMAT, self._data_crc, index_offset, 0)
        footer_crc = zlib.crc32(footer[:-4])
        footer = footer[:-4] + struct.pack("!I", footer_crc)

        self._file.write(footer)

        # recalculate, pack, crc header with final data size
        header = struct.pack(
            HEADER_FORMAT,
            SSTABLE_MAGIC,
            SSTABLE_VERSION,
            self.features.value,
            b'\x00' * 16,
            self.timestamp,
            self.entry_count,
            self.data_size,
            0
        )
        header_crc = zlib.crc32(header[:-4])
        header = header[:-4] + struct.pack("!I", header_crc)

        # seek back to beginning and rewrite header, now with complete info
        self._file.seek(0)
        self._file.write(header)

        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
    
    def _write_index(self) -> int:
        """Write the sparse index section and return its offset."""
        if self._file is None:
            raise RuntimeError("File not initialized")
            
        index_start_offset = self._file.tell()
        
        # Write index header (entry count)
        index_header = struct.pack(INDEX_HEADER_FORMAT, len(self._index_entries))
        self._file.write(index_header)
        
        # Write each index entry
        for index_entry in self._index_entries:
            self._file.write(index_entry.serialize())
        
        return index_start_offset

    def discard(self) -> None:
        """ Cleanup partial SSTable if something fails """
        if self._file:
            self._file.close()
        if os.path.exists(self.filepath):
            os.remove(self.filepath)

    def __enter__(self) -> 'SSTableWriter':
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[object]) -> None:
        if exc_type is not None:
            self.discard()
        else:
            self.finalize()


class SSTableReader:
    """Reader for SSTable files with sparse index support."""
    
    def __init__(self, filepath: str):
        """Initialize reader and load sparse index."""
        self.filepath = filepath
        self._file: Optional[BinaryIO] = None
        self._index_entries: List[IndexEntry] = []
        self._data_start_pos = 0
        self._data_size = 0
        
        self._load_metadata()
    
    def _load_metadata(self) -> None:
        """Load SSTable metadata and sparse index."""
        with open(self.filepath, 'rb') as f:
            # Read header to get data start position
            f.seek(0)
            header_data = f.read(HEADER_SIZE)
            if len(header_data) < HEADER_SIZE:
                raise ValueError("Invalid SSTable file: header too short")
            
            header_fields = struct.unpack(HEADER_FORMAT, header_data)
            magic, version, features, reserved, timestamp, entry_count, data_size, header_crc = header_fields
            
            if magic != SSTABLE_MAGIC:
                raise ValueError(f"Invalid magic number: {magic}")
            
            self._data_start_pos = HEADER_SIZE
            self._data_size = data_size
            
            # Read footer to get index offset
            f.seek(-FOOTER_SIZE, 2)  # Seek to footer
            footer_data = f.read(FOOTER_SIZE)
            if len(footer_data) < FOOTER_SIZE:
                raise ValueError("Invalid SSTable file: footer too short")
            
            data_crc, index_offset, footer_crc = struct.unpack(FOOTER_FORMAT, footer_data)
            
            # Load sparse index if present
            if index_offset > 0:
                self._load_index(f, index_offset)
    
    def _load_index(self, file: BinaryIO, index_offset: int) -> None:
        """Load the sparse index from file."""
        file.seek(index_offset)
        
        # Read index header
        index_header_data = file.read(INDEX_HEADER_SIZE)
        if len(index_header_data) < INDEX_HEADER_SIZE:
            raise ValueError("Invalid index: header too short")
        
        entry_count = struct.unpack(INDEX_HEADER_FORMAT, index_header_data)[0]
        
        # Read index entries
        for _ in range(entry_count):
            remaining_data = file.read(1024)  # Read in chunks
            if not remaining_data:
                raise ValueError("Unexpected end of index")
            
            index_entry, bytes_consumed = IndexEntry.deserialize(remaining_data)
            self._index_entries.append(index_entry)
            
            # Seek back if we read too much
            file.seek(-len(remaining_data) + bytes_consumed, 1)
    
    def get(self, key: str) -> Optional[DatabaseEntry]:
        """Get an entry by key using sparse index for fast lookup."""
        if not self._index_entries:
            # No index available, fall back to linear scan
            return self._linear_scan(key)
        
        # Find the appropriate index range using binary search
        start_offset = self._find_scan_start(key)
        
        # Scan from the start position until we find the key or exceed it
        with open(self.filepath, 'rb') as f:
            f.seek(start_offset)
            
            while f.tell() < self._data_start_pos + self._data_size:
                try:
                    # Read entry header to determine total size needed
                    current_pos = f.tell()
                    header_data = f.read(SEQUENCE_SIZE + KEY_LEN_SIZE)
                    
                    if len(header_data) < SEQUENCE_SIZE + KEY_LEN_SIZE:
                        break
                    
                    sequence = struct.unpack(SEQUENCE_FORMAT, header_data[:SEQUENCE_SIZE])[0]
                    key_length = struct.unpack(KEY_LENGTH_FORMAT, header_data[SEQUENCE_SIZE:])[0]
                    
                    # Validate key length
                    if key_length > MAX_KEY_SIZE:
                        raise ValueError(f"Key length {key_length} exceeds maximum {MAX_KEY_SIZE}")
                    
                    # Read key and value length
                    key_and_value_len_data = f.read(key_length + VALUE_LEN_SIZE)
                    if len(key_and_value_len_data) < key_length + VALUE_LEN_SIZE:
                        break
                    
                    entry_key = key_and_value_len_data[:key_length].decode('utf-8')
                    value_length = struct.unpack(VALUE_LENGTH_FORMAT, 
                                                key_and_value_len_data[key_length:])[0]
                    
                    # Validate value length
                    if value_length > MAX_VALUE_SIZE:
                        raise ValueError(f"Value length {value_length} exceeds maximum {MAX_VALUE_SIZE}")
                    
                    # Read value if present and create appropriate entry
                    if value_length > 0:
                        value_data = f.read(value_length)
                        if len(value_data) < value_length:
                            break
                        entry = DatabaseEntry.put(entry_key, sequence, value_data)
                    else:
                        entry = DatabaseEntry.delete(entry_key, sequence)
                    
                    # Check if we found our key
                    if entry.key == key:
                        return entry
                    elif entry.key > key:
                        # We've passed the key, it doesn't exist
                        break
                        
                except (ValueError, UnicodeDecodeError):
                    # Invalid data, we've reached the end or hit corruption
                    break
        
        return None
    
    def _find_scan_start(self, key: str) -> int:
        """Find the file offset to start scanning for the given key."""
        if not self._index_entries:
            return self._data_start_pos
        
        # Binary search to find the largest index key <= target key
        left, right = 0, len(self._index_entries) - 1
        start_offset = self._data_start_pos
        
        while left <= right:
            mid = (left + right) // 2
            mid_key = self._index_entries[mid].key
            
            if mid_key <= key:
                start_offset = self._index_entries[mid].file_offset
                left = mid + 1
            else:
                right = mid - 1
        
        return start_offset
    
    def _linear_scan(self, key: str) -> Optional[DatabaseEntry]:
        """Fallback linear scan when no index is available."""
        with open(self.filepath, 'rb') as f:
            f.seek(self._data_start_pos)
            
            while f.tell() < self._data_start_pos + self._data_size:
                try:
                    # Read entry header to determine total size needed
                    current_pos = f.tell()
                    header_data = f.read(SEQUENCE_SIZE + KEY_LEN_SIZE)
                    
                    if len(header_data) < SEQUENCE_SIZE + KEY_LEN_SIZE:
                        break
                    
                    sequence = struct.unpack(SEQUENCE_FORMAT, header_data[:SEQUENCE_SIZE])[0]
                    key_length = struct.unpack(KEY_LENGTH_FORMAT, header_data[SEQUENCE_SIZE:])[0]
                    
                    # Validate key length
                    if key_length > MAX_KEY_SIZE:
                        break
                    
                    # Read key and value length
                    key_and_value_len_data = f.read(key_length + VALUE_LEN_SIZE)
                    if len(key_and_value_len_data) < key_length + VALUE_LEN_SIZE:
                        break
                    
                    entry_key = key_and_value_len_data[:key_length].decode('utf-8')
                    value_length = struct.unpack(VALUE_LENGTH_FORMAT, 
                                                key_and_value_len_data[key_length:])[0]
                    
                    # Validate value length
                    if value_length > MAX_VALUE_SIZE:
                        break
                    
                    # Read value if present and create appropriate entry
                    if value_length > 0:
                        value_data = f.read(value_length)
                        if len(value_data) < value_length:
                            break
                        entry = DatabaseEntry.put(entry_key, sequence, value_data)
                    else:
                        entry = DatabaseEntry.delete(entry_key, sequence)
                    
                    if entry.key == key:
                        return entry
                    elif entry.key > key:
                        break
                        
                except (ValueError, UnicodeDecodeError):
                    break
        
        return None
    
    def close(self) -> None:
        """Close the reader."""
        if self._file:
            self._file.close()
            self._file = None