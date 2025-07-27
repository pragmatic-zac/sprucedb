import logging
from pathlib import Path

from src.configuration import Configuration
from src.entry import DatabaseEntry
from src.sstable import SSTableReader, SSTableWriter

from .wal import WriteAheadLog
from .skiplist import SkipList

class Database:
    def __init__(self, config: Configuration):
        """
        Initialize a new SpruceDB instance.
        
        Args:
            config: Configuration values for the database
        """
        self.config = config
        self.logger = logging.getLogger("sprucedb.database")
        
        self.base_path = Path(config.base_path)
        self.sstables_dir = self.base_path / "sstables"
        self.wal_dir = self.base_path / "wal"
        self.manifest_dir = self.base_path / "manifest"
        
        # Initialize directory structure
        self._init_directories()
        
        # Initialize components
        self.memtable: SkipList[DatabaseEntry] = SkipList()
        self.wal: WriteAheadLog = WriteAheadLog(str(self._init_wal_path()))

        self.seq_no: int = 0 # TODO - load this from existing files on startup
        
        self.logger.info("Database initialized at %s", self.base_path)
        
    def _init_directories(self) -> None:
        """Create necessary directory structure if it doesn't exist."""
        try:
            # Create base directory and subdirectories
            self.base_path.mkdir(parents=True, exist_ok=True)
            self.sstables_dir.mkdir(exist_ok=True)
            self.wal_dir.mkdir(exist_ok=True)
            self.manifest_dir.mkdir(exist_ok=True)
                
        except OSError as e:
            self.logger.error("Failed to initialize database directories: %s", e)
            raise RuntimeError(f"Failed to initialize database directories: {e}")
            
    def _init_wal_path(self) -> Path:
        """Initialize the Write-Ahead Log path."""
        try:
            wal_path = self.wal_dir / "current.wal"
            return wal_path
        except Exception as e:
            self.logger.error("Failed to initialize WAL: %s", e)
            raise RuntimeError(f"Failed to initialize WAL: {e}")
            
    def close(self) -> None:
        """Safely close the database."""
        if self.wal:
            self.wal.close()
        self.logger.info("Database closed")

    def _get_next_sequence(self) -> int:
        self.seq_no = self.seq_no + 1
        return self.seq_no
    
    def _should_flush(self) -> bool:
        return self.memtable.size >= self.config.memtable_flush_threshold
    
    def _flush_memtable_to_sstable(self) -> None:
        # use generator from memtable to feed data to SSTableWriter
        writer = SSTableWriter(base_path=str(self.sstables_dir))
        for entry in self.memtable:
            writer.add_entry(entry)
        
        # Get the SSTable ID before finalizing
        sstable_id = writer.sstable_id
        
        # Finalize the SSTable
        writer.finalize()
        
        # rotate WAL with the actual SSTable ID
        old_path = self.wal.rotate(sstable_id=sstable_id, sequence=self._get_next_sequence())
        self.logger.debug(f'Rotated WAL - closed file -> {old_path}')

        # reset memtable - but TODO, could this cause data loss?
        # if data is written to current memtable after flush but before replacement?
        self.memtable = SkipList()

    def put(self, key: str, value: bytes) -> None:
        seq_num = self._get_next_sequence()

        entry = DatabaseEntry.put(key, seq_num, value)
        self.wal.write_to_log(entry)

        # TODO - add deferred commit until after memtable insert
        # right now we could end up in an inconsistent state if WAL succeeds but memtable fails
        self.memtable.insert(key, entry)

        # could also consider checking every N inserts instead of every single time
        if self._should_flush():
            self._flush_memtable_to_sstable()


    def get(self, key: str) -> bytes | None:
        # Search memtable first (most recent data)
        memtable_result = self.memtable.search(key)
        if memtable_result is not None:
            # Handle tombstones from memtable
            if memtable_result.is_tombstone():
                return None
            return memtable_result.value

        # Search SSTables from newest to oldest
        sst_files = [
            f for f in self.sstables_dir.iterdir() 
            if f.is_file() and '.' in f.name
        ]
        
        # Sort by timestamp in filename (newest first)
        # SSTable filenames are like "base_path.20240101120000"
        # Extract timestamp (last part after final dot) and sort in reverse
        sst_files.sort(key=lambda f: f.name.split('.')[-1], reverse=True)
        
        for sst_file in sst_files:
            try:
                reader = SSTableReader(str(sst_file))
                try:
                    result = reader.get(key)
                    if result:
                        if result.is_tombstone():
                            return None
                        return result.value
                finally:
                    reader.close()
            except Exception as e:
                self.logger.warning("Failed to read from SSTable %s while searching for key=%s: %s", 
                                  sst_file.name, key, e)
                continue

        return None
    
    def delete(self, key: str) -> None:
        seq_num = self._get_next_sequence()
        entry = DatabaseEntry.delete(key, seq_num)

        self.wal.write_to_log(entry)
        self.memtable.insert(key, entry)