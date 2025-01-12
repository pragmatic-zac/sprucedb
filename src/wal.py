import struct
import zlib
from enum import Enum
from typing import Optional, Tuple

"""
Write-ahead log entry format
[4 bytes] CRC32 (uint32)
[8 bytes] Timestamp (uint64)
[1 byte]  Operation Type (uint8: 1=PUT, 2=DELETE) 
[4 bytes] Key Length (uint32)
[4 bytes] Value Length (uint32)
[X bytes] Key (UTF-8 encoded)
[Y bytes] Value (bytes)
"""

class WALOperationType(Enum):
    PUT = 1
    DELETE = 2


class WALEntry:
    HEADER_FORMAT = "!IQBII"  # ! for network byte order, I=uint32, Q=uint64, B=uint8
    HEADER_FORMAT_SANS_CRC = "!QBII"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, timestamp: int, op_type: WALOperationType,
                 key: str, value: Optional[bytes] = None):
        self._timestamp = timestamp
        self._op_type = op_type
        self._key = key
        self._value = value if value else b''

    @classmethod
    def put(cls, timestamp: int, key: str, value: bytes) -> 'WALEntry':
        return cls(timestamp, WALOperationType.PUT, key, value)

    @classmethod
    def delete(cls, timestamp: int, key: str) -> 'WALEntry':
        return cls(timestamp, WALOperationType.DELETE, key)

    @property
    def timestamp(self) -> int:
        return self._timestamp

    @property
    def op_type(self) -> WALOperationType:
        return self._op_type

    @property
    def key(self) -> str:
        return self._key

    @property
    def value(self) -> Optional[bytes]:
        return self._value

    def serialize(self) -> bytes:
        key_bytes = self.key.encode("utf-8")
        value_bytes = self.value

        # does not include CRC
        raw_header = struct.pack(
            self.HEADER_FORMAT_SANS_CRC,
            self.timestamp,
            self.op_type.value,
            len(key_bytes),
            len(value_bytes)
        )

        # calculate CRC of header (minus CRC field) + data
        data_to_crc = raw_header + key_bytes + value_bytes
        crc = zlib.crc32(data_to_crc)

        # now pack the entire header
        header = struct.pack(
            self.HEADER_FORMAT,
            crc,
            self.timestamp,
            self.op_type.value,
            len(key_bytes),
            len(value_bytes)
        )

        return header + key_bytes + value_bytes

    @classmethod
    def deserialize(cls, data: bytes) -> 'WALEntry':
        if len(data) < cls.HEADER_SIZE:
            raise ValueError(f"Data too short for header: {len(data)} < {cls.HEADER_SIZE}")

        header = data[:cls.HEADER_SIZE]
        src_crc, timestamp, op_type_value, key_len, value_len = struct.unpack(cls.HEADER_FORMAT, header)

        expected_len = cls.HEADER_SIZE + key_len + value_len
        if len(data) < expected_len:
            raise ValueError(f"Data too short for payload: {len(data)} < {expected_len}")

        # unpack the raw data (minus crc) to verify
        data_to_verify = struct.pack(
            cls.HEADER_FORMAT_SANS_CRC,
            timestamp,
            op_type_value,
            key_len,
            value_len
        )
        payload_offset = cls.HEADER_SIZE
        key_end_offset = payload_offset + key_len
        key_bytes = data[payload_offset:key_end_offset]
        value_bytes = data[key_end_offset:key_end_offset + value_len]

        read_crc = zlib.crc32(data_to_verify + key_bytes + value_bytes)
        if src_crc != read_crc:
            raise ValueError("CRC check failed")

        try:
            op_type = WALOperationType(op_type_value)
        except ValueError:
            raise ValueError(f"Invalid operation type: {op_type_value}")

        try:
            key = key_bytes.decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError("Invalid UTF-8 encoding in key")

        if op_type == WALOperationType.DELETE:
            return cls.delete(timestamp, key)
        else:
            return cls.put(timestamp, key, value_bytes)

