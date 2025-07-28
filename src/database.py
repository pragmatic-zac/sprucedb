import logging
from pathlib import Path
from typing import List

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

        # Replay existing WAL files to recover data and sequence numbers
        recovered_seq_no = self._replay_wal_files()
        self.seq_no = recovered_seq_no
        
        self.logger.info("Database initialized at %s (recovered sequence: %d)", 
                        self.base_path, self.seq_no)
        
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

    def _discover_wal_files(self) -> List[Path]:
        """
        Discover existing WAL files in chronological order for replay.
        
        Returns:
            List[Path]: WAL files sorted by creation time (oldest first)
        """
        if not self.wal_dir.exists():
            return []
        
        wal_files = []
        for file_path in self.wal_dir.iterdir():
            if file_path.is_file() and file_path.name.startswith("current.wal."):
                wal_files.append(file_path)
        
        # Sort by timestamp in filename (oldest first for replay order)
        # WAL filenames are like "current.wal.20240101120000.0"
        # Extract timestamp part and sort chronologically
        def extract_timestamp(path: Path) -> str:
            parts = path.name.split('.')
            if len(parts) >= 3:
                return parts[2]  # timestamp part
            return "0"  # fallback for malformed names
        
        wal_files.sort(key=extract_timestamp)
        
        self.logger.debug("Discovered %d WAL files for replay", len(wal_files))
        return wal_files

    def _should_replay_wal_file(self, wal_file_path: str) -> bool:
        """
        Determine if a WAL file should be replayed by checking if it ends with a FLUSH marker.
        
        WAL files that end with FLUSH markers have been completely flushed to SSTables
        and their entries should not be replayed to avoid duplicating data.
        
        Args:
            wal_file_path: Path to the WAL file to check
            
        Returns:
            bool: True if file should be replayed, False if it's been fully flushed
        """
        # Use efficient method to check for FLUSH marker at end
        if WriteAheadLog.has_flush_marker_at_end(wal_file_path):
            self.logger.debug("WAL file %s ends with FLUSH marker, skipping replay", 
                            wal_file_path)
            return False
        
        return True

    def _replay_wal_files(self) -> int:
        """
        Replay WAL files that haven't been fully flushed to SSTables.
        
        Only replays WAL files that don't end with FLUSH markers to avoid
        duplicating data that's already been persisted to SSTables.
        
        Returns:
            int: Highest sequence number encountered during replay
        """
        wal_files = self._discover_wal_files()
        
        if not wal_files:
            self.logger.info("No WAL files found for replay")
            return 0
        
        highest_sequence = 0
        total_entries = 0
        files_skipped = 0
        
        for wal_file in wal_files:
            # Check if this WAL file has been fully flushed
            if not self._should_replay_wal_file(str(wal_file)):
                files_skipped += 1
                self.logger.debug("Skipping fully flushed WAL file: %s", wal_file.name)
                
                # Still need to track sequence numbers from skipped files
                # TODO - not sure we need this
                try:
                    for wal_entry in WriteAheadLog.read_all_entries(str(wal_file)):
                        highest_sequence = max(highest_sequence, wal_entry.sequence)
                except Exception as e:
                    self.logger.warning("Error reading sequence numbers from %s: %s", 
                                      wal_file.name, e)
                continue
            
            self.logger.info("Replaying WAL file: %s", wal_file.name)
            file_entries = 0
            
            try:
                for wal_entry in WriteAheadLog.read_all_entries(str(wal_file)):
                    # Track sequence numbers from all entries (including FLUSH markers)
                    highest_sequence = max(highest_sequence, wal_entry.sequence)
                    
                    # Skip FLUSH markers - they're operational metadata, not user data
                    if wal_entry.is_flush_marker():
                        self.logger.debug("Skipping FLUSH marker for SSTable: %s (seq: %d)", 
                                        wal_entry.get_flushed_sstable_id(), wal_entry.sequence)
                        continue
                    
                    # Convert WAL entry to DatabaseEntry and insert into memtable
                    try:
                        db_entry = wal_entry.to_database_entry()
                        
                        # Insert into memtable
                        self.memtable.insert(db_entry.key, db_entry)
                        
                        file_entries += 1
                        total_entries += 1
                        
                    except ValueError as e:
                        self.logger.warning("Failed to convert WAL entry to DatabaseEntry: %s", e)
                        continue
                        
            except Exception as e:
                self.logger.error("Error replaying WAL file %s: %s", wal_file.name, e)
                continue
            
            self.logger.info("Replayed %d entries from %s", file_entries, wal_file.name)
        
        self.logger.info("WAL replay complete: %d files processed, %d skipped (flushed), %d total entries replayed, highest sequence: %d", 
                        len(wal_files), files_skipped, total_entries, highest_sequence)
        return highest_sequence
            
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