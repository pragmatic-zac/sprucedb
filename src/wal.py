import os
import struct
import zlib
from datetime import datetime
from enum import Enum
from typing import Optional, BinaryIO, Iterator

from .entry import DatabaseEntry, EntryType

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
MAX_VALUE_BYTES = 1024 * 1024  # 1MB max value size, consistent with SSTable

class WALOperationType(Enum):
    PUT = 1
    DELETE = 2
    FLUSH = 3


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
    
    @classmethod
    def flush(cls, timestamp: int, sequence: int) -> 'WALEntry':
        """Create a flush marker entry."""
        return cls(timestamp=timestamp, op_type=WALOperationType.FLUSH, key="", sequence=sequence, value=b'')

    @classmethod
    def from_database_entry(cls, entry: DatabaseEntry, timestamp: Optional[int] = None) -> 'WALEntry':
        """Create a WALEntry from a unified DatabaseEntry."""
        # Use DatabaseEntry's timestamp if provided, otherwise use parameter or current time
        if timestamp is None:
            timestamp = entry.timestamp if entry.timestamp is not None else int(datetime.utcnow().timestamp())
        
        if entry.entry_type == EntryType.PUT:
            if entry.value is None:
                raise ValueError("PUT entries must have a value")
            return cls.put(timestamp, entry.key, entry.value, entry.sequence)
        else:
            return cls.delete(timestamp, entry.key, entry.sequence)
    
    def to_database_entry(self) -> DatabaseEntry:
        """Convert this WALEntry to a unified DatabaseEntry."""
        if self.op_type == WALOperationType.PUT:
            return DatabaseEntry.put(self.key, self.sequence, self.value, self.timestamp)
        elif self.op_type == WALOperationType.DELETE:
            return DatabaseEntry.delete(self.key, self.sequence, self.timestamp)
        else:
            # FLUSH operations cannot be converted to DatabaseEntry since they're WAL-specific
            raise ValueError(f"Cannot convert {self.op_type} WALEntry to DatabaseEntry")

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

    def is_flush_marker(self) -> bool:
        """Check if this entry is a flush marker."""
        return self.op_type == WALOperationType.FLUSH

    def get_flushed_sstable_id(self) -> Optional[str]:
        """
        Extract the SSTable ID from a flush marker entry.
        
        Returns:
            str: SSTable ID if this is a flush marker, None otherwise
        """
        if not self.is_flush_marker():
            return None
        
        if self.key.startswith("FLUSH:"):
            return self.key[6:]  # Remove "FLUSH:" prefix
        
        return None

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
        elif op_type == WALOperationType.PUT:
            return cls.put(timestamp, key, value_bytes, sequence)
        else:  # WALOperationType.FLUSH
            return cls(timestamp=timestamp, op_type=WALOperationType.FLUSH, key=key, sequence=sequence, value=b'')


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

    def rotate(self, sstable_id: str, sequence: int) -> str:
        """
        Rotate the WAL after a successful SSTable flush.

        Args:
            sstable_id: ID of the SSTable that contains the flushed data
                       (used for tracking which WAL files can be cleaned up)
            sequence: Current sequence number for the flush marker

        Returns:
            str: Path of the old WAL file that was rotated out
        """
        old_path = self.current_path

        # Write a special marker indicating successful flush
        # This helps during recovery to know this WAL was fully flushed
        self.write_flush_marker(sstable_id, sequence)

        # Open new WAL file
        self._open_next_file()

        return old_path

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

    def write_to_log(self, entry: DatabaseEntry) -> int:
        """
        Write a unified DatabaseEntry to the WAL.

        Args:
            entry: DatabaseEntry instance to write.

        Returns:
            int: Byte offset where the entry was written.

        Raises:
            RuntimeError: If the WAL is not currently writable.
            ValueError: If the entry violates WAL size constraints.
            IOError:   If the underlying file write fails.
        """
        if not self.write_file:
            raise RuntimeError('WAL file not available!')

        # Validate key/value size constraints that are WAL-specific
        if len(entry.key.encode()) > MAX_KEY_BYTES:
            raise ValueError('key exceeds max size')

        if entry.entry_type == EntryType.PUT and entry.value is not None:
            if len(entry.value) > MAX_VALUE_BYTES:
                raise ValueError(f'value exceeds max size of {MAX_VALUE_BYTES} bytes')

        # Convert to a WALEntry (adds timestamp if missing)
        wal_entry = WALEntry.from_database_entry(entry)

        serialized_entry = wal_entry.serialize()
        current_position = self.write_position
        bytes_written = self.write_file.write(serialized_entry)
        self.write_position = current_position + bytes_written

        try:
            self.write_file.flush()
            os.fsync(self.write_file.fileno())
        except (OSError, ValueError) as e:
            raise IOError from e

        return current_position

    def write_flush_marker(self, sstable_id: str, sequence: int) -> int:
        """
        Write a flush marker entry to the WAL.
        
        Args:
            sstable_id: ID of the SSTable that was flushed
            sequence: Current sequence number for the flush marker
            
        Returns:
            int: Byte offset where the flush marker was written
        """
        if not self.write_file:
            raise RuntimeError('WAL file not available!')
        
        # Create flush marker entry with the SSTable ID in the key
        timestamp = int(datetime.utcnow().timestamp())
        flush_entry = WALEntry(
            timestamp=timestamp, 
            op_type=WALOperationType.FLUSH,
            key=f"FLUSH:{sstable_id}", 
            sequence=sequence, 
            value=b''
        )
        
        serialized_entry = flush_entry.serialize()
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

    @staticmethod
    def read_all_entries(file_path: str) -> Iterator[WALEntry]:
        """
        Read all entries from a WAL file, handling corruption gracefully.
        
        This method continues reading even if individual entries are corrupted,
        logging warnings for corrupted entries but not failing the entire read.
        
        Args:
            file_path: Path to the WAL file to read
            
        Yields:
            WALEntry: Valid entries from the WAL file
        """
        import logging
        logger = logging.getLogger("sprucedb.wal.replay")
        
        try:
            with open(file_path, 'rb') as file:
                position = 0
                entry_count = 0
                corruption_count = 0
                
                while True:
                    try:
                        file.seek(position)
                        
                        # Try to read header
                        header_bytes = file.read(WALEntry.HEADER_SIZE)
                        if not header_bytes:
                            break  # EOF
                        
                        if len(header_bytes) < WALEntry.HEADER_SIZE:
                            logger.warning("Incomplete header at position %d in %s, stopping replay", 
                                         position, file_path)
                            break
                        
                        # Extract payload size from header
                        _, _, _, _, key_len, value_len = struct.unpack(WALEntry.HEADER_FORMAT, header_bytes)
                        
                        # Read payload
                        payload_size = key_len + value_len
                        payload_bytes = file.read(payload_size)
                        
                        if len(payload_bytes) < payload_size:
                            logger.warning("Incomplete payload at position %d in %s, stopping replay", 
                                         position, file_path)
                            break
                        
                        # Try to deserialize complete entry
                        entry_data = header_bytes + payload_bytes
                        entry = WALEntry.deserialize(entry_data)
                        
                        yield entry
                        entry_count += 1
                        position += len(entry_data)
                        
                    except ValueError as e:
                        corruption_count += 1
                        logger.warning("Corrupted entry at position %d in %s: %s", 
                                     position, file_path, e)
                        
                        # Try to find the next valid entry by scanning ahead
                        position += 1
                        if position >= os.path.getsize(file_path):
                            break
                            
                    except Exception as e:
                        logger.error("Unexpected error reading WAL file %s at position %d: %s", 
                                   file_path, position, e)
                        break
                
                logger.info("WAL replay from %s: %d entries read, %d corruptions skipped", 
                           file_path, entry_count, corruption_count)
                           
        except OSError as e:
            logger.error("Failed to open WAL file %s for replay: %s", file_path, e)
            # Don't yield anything if we can't open the file
            return

    @staticmethod
    def has_flush_marker_at_end(file_path: str) -> bool:
        """
        Check if a WAL file ends with a FLUSH marker.
        
        This method uses read_all_entries logic to sequentially
        read through the file and check the last valid entry.
        Could be optimized to not read the entire file, but this
        works for now.
        """
        import logging
        logger = logging.getLogger("sprucedb.wal.check")
        
        try:
            entries = list(WriteAheadLog.read_all_entries(file_path))
            
            if not entries:
                return False  # Empty file has no FLUSH marker
            
            # Check if the last valid entry is a FLUSH marker
            last_entry = entries[-1]
            is_flush = last_entry.is_flush_marker()
            
            if is_flush:
                logger.debug("WAL file %s ends with FLUSH marker (SSTable: %s)", 
                           file_path, last_entry.get_flushed_sstable_id())
            
            return is_flush
            
        except Exception as e:
            logger.warning("Error checking for FLUSH marker in %s: %s", file_path, e)
            # If we can't determine, assume it needs replay (safer default)
            return False
