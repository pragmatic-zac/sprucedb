import os
import struct
import zlib
from datetime import datetime
from enum import Enum
from typing import Optional, BinaryIO

"""
Write-ahead log entry format
[4 bytes] CRC32 (uint32)
[8 bytes] Sequence Number (uint64)
[8 bytes] Timestamp (uint64)
[1 byte]  Operation Type (uint8: 1=PUT, 2=DELETE) 
[4 bytes] Key Length (uint32)
[4 bytes] Value Length (uint32)
[X bytes] Key (UTF-8 encoded)
[Y bytes] Value (bytes)
"""

MAX_KEY_BYTES = 65536

class WALOperationType(Enum):
    PUT = 1
    DELETE = 2


class WALEntry:
    HEADER_FORMAT = "!IQQBII"  # ! for network byte order, I=uint32, Q=uint64, B=uint8
    HEADER_FORMAT_SANS_CRC = "!QQBII"  # sequence, timestamp, op_type, key_len, value_len
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, timestamp: int, op_type: WALOperationType,
                 key: str, sequence: int, value: bytes = b''):
        self._timestamp = timestamp
        self._op_type = op_type
        self._key = key
        self._value = value
        self._sequence = sequence

    @classmethod
    def put(cls, timestamp: int, key: str, value: bytes, sequence: int) -> 'WALEntry':
        return cls(timestamp, WALOperationType.PUT, key, sequence, value)

    @classmethod
    def delete(cls, timestamp: int, key: str, sequence: int) -> 'WALEntry':
        return cls(timestamp, WALOperationType.DELETE, key, sequence, b'')

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
    def value(self) -> bytes:
        return self._value

    @property
    def sequence(self) -> int:
        return self._sequence

    def serialize(self) -> bytes:
        key_bytes = self.key.encode("utf-8")
        value_bytes = self.value

        # does not include CRC
        raw_header = struct.pack(
            self.HEADER_FORMAT_SANS_CRC,
            self.sequence,
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
            self.sequence,
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
        src_crc, sequence, timestamp, op_type_value, key_len, value_len = struct.unpack(cls.HEADER_FORMAT, header)

        expected_len = cls.HEADER_SIZE + key_len + value_len
        if len(data) < expected_len:
            raise ValueError(f"Data too short for payload: {len(data)} < {expected_len}")

        # unpack the raw data (minus crc) to verify
        data_to_verify = struct.pack(
            cls.HEADER_FORMAT_SANS_CRC,
            sequence,
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
            return cls.delete(timestamp, key, sequence)
        else:
            return cls.put(timestamp, key, value_bytes, sequence)


class WriteAheadLog:
    def __init__(self, path: str):
        """Initialize a Write-Ahead Log."""
        self.base_path = path
        self.write_file: Optional[BinaryIO] = None
        self.read_file: Optional[BinaryIO] = None
        self.write_position = 0
        self.file_counter = 0

        # create directory if it doesn't exist
        os.makedirs(os.path.dirname(path), exist_ok=True)

        try:
            self._open_next_file()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize WAL at {path}") from e

    def _get_timestamped_path(self) -> str:
        """Generate timestamp-based WAL file path."""
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        path = f"{self.base_path}.{timestamp}.{self.file_counter}"
        self.file_counter += 1
        return path

    def _open_next_file(self) -> None:
        """Open a new WAL file with timestamp-based name."""
        if self.write_file:
            self.close()

        self.current_path = self._get_timestamped_path()
        self.write_file = open(self.current_path, 'ab+')
        self.read_file = open(self.current_path, 'rb')
        self.write_position = 0

    def rotate(self, sstable_id: str) -> str:
        """
        Rotate the WAL after a successful SSTable flush.

        Args:
            sstable_id: ID of the SSTable that contains the flushed data
                       (used for tracking which WAL files can be cleaned up)

        Returns:
            str: Path of the old WAL file that was rotated out
        """
        old_path = self.current_path

        # Write a special marker indicating successful flush
        # This helps during recovery to know this WAL was fully flushed
        self._write_flush_marker(sstable_id)

        # Open new WAL file
        self._open_next_file()

        return old_path

    def _write_flush_marker(self, sstable_id: str) -> None:
        """Write a marker indicating successful flush to SSTable."""
        if self.write_file:
            # Could use a special WALEntry type for this
            marker = f"FLUSHED_TO_SSTABLE:{sstable_id}\n".encode()
            self.write_file.write(marker)
            self.write_file.flush()
            os.fsync(self.write_file.fileno())

    def close(self) -> None:
        """Safely close the WAL file."""
        if self.write_file:
            self.write_file.flush()
            os.fsync(self.write_file.fileno())
            self.write_file.close()
            self.write_file = None
        if self.read_file:
            self.read_file.close()
            self.read_file = None

    def __enter__(self) -> 'WriteAheadLog':
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[object]) -> None:
        self.close()

    def write_to_log(self, op_type: WALOperationType, key: str, sequence: int, value: Optional[bytes] = None) -> int:
        """
        Write an operation to the WAL.

        Args:
            op_type: Type of operation (PUT or DELETE)
            key: Key being operated on
            sequence: Global sequence number for this operation
            value: Value to store (None for DELETE operations)

        Returns:
            int: Position where the entry was written

        Raises:
            RuntimeError: If WAL file is not available
            ValueError: If key/value validation fails
            IOError: If write fails
        """
        if not self.write_file:
            raise RuntimeError('WAL file not available!')

        if not key:
            raise ValueError('key is required')

        if len(key.encode()) > MAX_KEY_BYTES:
            raise ValueError('key exceeds max size')

        if sequence < 0:
            raise ValueError('sequence number must be non-negative')

        timestamp = int(datetime.utcnow().timestamp())

        entry = None
        if op_type == WALOperationType.DELETE:
            entry = WALEntry.delete(timestamp, key, sequence)
        else:
            if value is None:
                raise ValueError('value is required for PUT operations')
            entry = WALEntry.put(timestamp, key, value, sequence)

        serialized_entry = entry.serialize()
        current_position = self.write_position
        bytes_written = self.write_file.write(serialized_entry)
        self.write_position = current_position + bytes_written

        try:
            self.write_file.flush()
            os.fsync(self.write_file.fileno())
        except (OSError, ValueError) as e:
            raise IOError from e

        return current_position

    def read_log_entry(self, position: Optional[int] = None) -> Optional[WALEntry]:
        """
        Read a single WAL entry from the given position.

        Args:
            position: Position to read from. If None, reads from current read pointer position.

        Returns:
            WALEntry if entry was read successfully, None if EOF

        Raises:
            ValueError: If entry is corrupt/invalid
            IOError: If read fails
        """
        if not self.read_file:
            raise RuntimeError('WAL file not available!')

        if position is not None:
            self.read_file.seek(position)

        # determine number of bytes to read from header
        header_bytes = self.read_file.read(WALEntry.HEADER_SIZE)

        # validate header
        if not header_bytes:
            return None
        if len(header_bytes) < WALEntry.HEADER_SIZE:
            raise ValueError("Incomplete header")

        # unpack the header (and the values we care about)
        _, _, _, _, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header_bytes)
        bytes_to_read = key_len + value_len
        payload_bytes = self.read_file.read(bytes_to_read)

        # validate payload
        if len(payload_bytes) < bytes_to_read:
            raise ValueError("Incomplete payload")

        return WALEntry.deserialize(header_bytes + payload_bytes)
