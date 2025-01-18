import struct
from dataclasses import dataclass

from enum import Flag, auto
from typing import Final, Optional

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
# - Key length (I = 4 bytes unsigned int)
# - Key (variable)
# - Value length (I = 4 bytes unsigned int)
# - Value (variable)
KEY_LENGTH_FORMAT: Final[str] = "!I"    # format for key length
VALUE_LENGTH_FORMAT: Final[str] = "!I"   # format for value length

# header/footer sizes
HEADER_SIZE: Final[int] = struct.calcsize(HEADER_FORMAT)
FOOTER_SIZE: Final[int] = struct.calcsize(FOOTER_FORMAT)
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
    value: Optional[bytes] = None

    def serialize(self) -> bytes:
        """
        Serialize entry to bytes in format:
        [key_length][key][value_length][value]
        """
        key_bytes = self.key.encode("utf-8")
        value_bytes = self.value if self.value is not None else b''

        if len(key_bytes) > MAX_KEY_SIZE:
            raise ValueError(f"Key size exceeds max of {MAX_KEY_SIZE} bytes")

        if len(value_bytes) > MAX_VALUE_SIZE:
            raise ValueError(f"Value size exceeds max of {MAX_VALUE_SIZE} bytes")

        key_len_bytes = struct.pack(KEY_LENGTH_FORMAT, len(key_bytes))
        value_len_bytes = struct.pack(VALUE_LENGTH_FORMAT, len(value_bytes))

        return key_len_bytes + key_bytes + value_len_bytes + value_bytes

    @classmethod
    def deserialize(cls, data: bytes) -> tuple['SSTableEntry', int]:
        """
        Deserialize bytes into SSTableEntry.
        Returns tuple of (entry, bytes_consumed)
        """
        if len(data) < KEY_LEN_SIZE:
            raise ValueError("Data too short for key length")

        key_length = struct.unpack(KEY_LENGTH_FORMAT, data[:KEY_LEN_SIZE])[0]
        key_end = KEY_LEN_SIZE + key_length
        if len(data) < key_end:
            raise ValueError("Data too short for key")

        try:
            key = data[KEY_LEN_SIZE:key_end].decode('utf-8')
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
        return cls(key, value), bytes_consumed

    def __lt__(self, other: 'SSTableEntry') -> bool:
        return self.key < other.key

    def __gt__(self, other: 'SSTableEntry') -> bool:
        return self.key > other.key