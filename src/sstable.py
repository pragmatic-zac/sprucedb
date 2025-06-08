import os
import struct
import zlib
from dataclasses import dataclass
from datetime import datetime
from enum import Flag, auto
from typing import Final, Optional, Tuple, BinaryIO

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
MAX_VALUE_SIZE: Final[int] = 1024 * 1024  # 1MB max value size

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
FOOTER_FORMAT: Final[str] = "!QII"

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

class SSTableFeatureFlags(Flag):
    """Feature flags for SSTable format"""
    NONE = 0
    COMPRESSION = auto()      # uses compression
    BLOOM_FILTER = auto()     # has bloom filter
    BLOCK_BASED = auto()      # uses block-based format


@dataclass
class SSTableEntry:
    key: str
    sequence: int
    value: Optional[bytes] = None

    def serialize(self) -> bytes:
        """
        Serialize entry to bytes in format:
        [sequence][key_length][key][value_length][value]
        """
        key_bytes = self.key.encode("utf-8")
        value_bytes = self.value if self.value is not None else b''

        if len(key_bytes) > MAX_KEY_SIZE:
            raise ValueError(f"Key size exceeds max of {MAX_KEY_SIZE} bytes")

        if len(value_bytes) > MAX_VALUE_SIZE:
            raise ValueError(f"Value size exceeds max of {MAX_VALUE_SIZE} bytes")

        if self.sequence < 0:
            raise ValueError("Sequence number must be non-negative")

        sequence_bytes = struct.pack(SEQUENCE_FORMAT, self.sequence)
        key_len_bytes = struct.pack(KEY_LENGTH_FORMAT, len(key_bytes))
        value_len_bytes = struct.pack(VALUE_LENGTH_FORMAT, len(value_bytes))

        return sequence_bytes + key_len_bytes + key_bytes + value_len_bytes + value_bytes

    @classmethod
    def deserialize(cls, data: bytes) -> Tuple['SSTableEntry', int]:
        """
        Deserialize bytes into SSTableEntry.
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

        value = data[value_offset:value_offset + value_length]

        bytes_consumed = value_offset + value_length
        return cls(key, sequence, value), bytes_consumed

    def __lt__(self, other: 'SSTableEntry') -> bool:
        # First compare by key, then by sequence number (higher sequence wins for same key)
        if self.key == other.key:
            return self.sequence < other.sequence
        return self.key < other.key

    def __gt__(self, other: 'SSTableEntry') -> bool:
        # First compare by key, then by sequence number (higher sequence wins for same key)
        if self.key == other.key:
            return self.sequence > other.sequence
        return self.key > other.key


# TODO: sketch data flow and layers
class SSTableWriter:
    def __init__(self, base_path: str, features: SSTableFeatureFlags = SSTableFeatureFlags.NONE):
        """Create new SSTable file with header"""
        self.base_path = base_path
        self.features = features
        self.entry_count = 0
        self.data_size = 0
        self._last_key: Optional[str] = None  # for enforcing sorted order
        self._file: Optional[BinaryIO] = None
        self._data_start_pos = 0
        self._data_crc = 0

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

    def add_entry(self, entry: SSTableEntry) -> None:
        """ Add entry, enforcing sort order """
        if self._file is None:
            raise RuntimeError("File not initialized")
            
        if self._last_key is not None and self._last_key > entry.key:
            raise ValueError('Entries are not in sorted order')

        if self._last_key is not None and self._last_key == entry.key:
            raise ValueError(f'Duplicate key: {entry.key}')

        entry_bytes = entry.serialize()
        self._file.write(entry_bytes)
        self._last_key = entry.key

        self._data_crc = zlib.crc32(entry_bytes, self._data_crc)
        self.entry_count += 1


    def finalize(self) -> None:
        """ Write header/footer, sync to disk, close file """
        if self._file is None:
            raise RuntimeError("File not initialized")
            
        # recalculate, pack, crc header
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

        # grab current position so we can return and write the footer here
        end_data_pos = self._file.tell()

        # calculate data size from file positions
        self.data_size = end_data_pos - self._data_start_pos

        # seek back to beginning and rewrite header, now with complete info
        self._file.seek(0)
        self._file.write(header)

        # calculate, pack, crc footer
        footer = struct.pack(FOOTER_FORMAT,self._data_crc, 0, 0)
        footer_crc = zlib.crc32(footer[:-4])
        footer = footer[:-4] + struct.pack("!I", footer_crc)

        self._file.seek(end_data_pos)
        self._file.write(footer)

        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()


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