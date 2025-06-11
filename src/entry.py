"""
Unified database entry interface for SpruceDB.

This module provides a common entry format that can be used by both WAL and SSTable,
while allowing each to maintain their specific serialization requirements.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class EntryType(Enum):
    """Type of database entry operation."""
    PUT = 1
    DELETE = 2


@dataclass(frozen=True)
class DatabaseEntry:
    """
    Unified database entry representation.
    
    This serves as the canonical format for entries in SpruceDB, providing
    a common interface that both WAL and SSTable can convert to/from.
    """
    key: str
    sequence: int
    entry_type: EntryType
    value: Optional[bytes] = None
    timestamp: Optional[int] = None  # WAL-specific, optional for SSTable
    
    def __post_init__(self) -> None:
        """Validate entry constraints."""
        if not self.key:
            raise ValueError("key cannot be empty")
        
        if self.sequence < 0:
            raise ValueError("sequence number must be non-negative")
            
        if self.entry_type == EntryType.PUT and self.value is None:
            raise ValueError("PUT entries must have a value")
            
        if self.entry_type == EntryType.DELETE and self.value is not None:
            raise ValueError("DELETE entries cannot have a value")
    
    @classmethod
    def put(cls, key: str, sequence: int, value: bytes, timestamp: Optional[int] = None) -> 'DatabaseEntry':
        """Create a PUT entry."""
        return cls(key=key, sequence=sequence, entry_type=EntryType.PUT, 
                  value=value, timestamp=timestamp)
    
    @classmethod 
    def delete(cls, key: str, sequence: int, timestamp: Optional[int] = None) -> 'DatabaseEntry':
        """Create a DELETE entry."""
        return cls(key=key, sequence=sequence, entry_type=EntryType.DELETE, 
                  value=None, timestamp=timestamp)
    
    def is_tombstone(self) -> bool:
        """Check if this entry represents a deletion (tombstone)."""
        return self.entry_type == EntryType.DELETE
    
    def __lt__(self, other: 'DatabaseEntry') -> bool:
        """Sort entries by key, then by sequence number (higher sequence wins for same key)."""
        if self.key == other.key:
            return self.sequence < other.sequence
        return self.key < other.key
    
    def __gt__(self, other: 'DatabaseEntry') -> bool:
        """Sort entries by key, then by sequence number (higher sequence wins for same key)."""
        if self.key == other.key:
            return self.sequence > other.sequence
        return self.key > other.key 