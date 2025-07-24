from pathlib import Path

from src.configuration import Configuration
from src.entry import DatabaseEntry

from .wal import WriteAheadLog
from .skiplist import SkipList

class Database:
    def __init__(self, config: Configuration):
        """
        Initialize a new SpruceDB instance.
        
        Args:
            config: Configuration values for the database
        """
        self.base_path = Path(config.base_path)
        self.sstables_dir = self.base_path / "sstables"
        self.wal_dir = self.base_path / "wal"
        self.manifest_dir = self.base_path / "manifest"
        
        # Initialize directory structure
        self._init_directories()
        
        # Initialize components
        self.memtable: SkipList = SkipList()
        self.wal: WriteAheadLog = WriteAheadLog(str(self._init_wal_path()))

        self.seq_no: int = 0 # TODO - load this from existing files on startup
        
    def _init_directories(self) -> None:
        """Create necessary directory structure if it doesn't exist."""
        try:
            # Create base directory and subdirectories
            self.base_path.mkdir(parents=True, exist_ok=True)
            self.sstables_dir.mkdir(exist_ok=True)
            self.wal_dir.mkdir(exist_ok=True)
            self.manifest_dir.mkdir(exist_ok=True)
                
        except OSError as e:
            raise RuntimeError(f"Failed to initialize database directories: {e}")
            
    def _init_wal_path(self) -> Path:
        """Initialize the Write-Ahead Log path."""
        try:
            wal_path = self.wal_dir / "current.wal"
            return wal_path
        except Exception as e:
            raise RuntimeError(f"Failed to initialize WAL: {e}")
            
    def close(self) -> None:
        """Safely close the database."""
        if self.wal:
            self.wal.close()

    def _get_next_sequence(self) -> int:
        self.seq_no = self.seq_no + 1
        return self.seq_no

    def put(self, key: str, value: bytes) -> None:
        seq_num = self._get_next_sequence()

        entry = DatabaseEntry.put(key, seq_num, value)
        self.wal.write_to_log(entry)

        self.memtable.insert(key, entry)

    def get(self, key: str) -> DatabaseEntry | None:
        # check every single level - returning as soon as I find the value
        # read from memtable first
        # if not found there, read from sstables from newest to oldest
        return None