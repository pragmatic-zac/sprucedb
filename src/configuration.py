import os
import logging
import sys
from typing import Optional

class Configuration:
    def __init__(self) -> None:
        self.base_path: str = os.environ.get("SPRUCE_BASE_PATH", "spruce_data")
        
        # Logging configuration
        self.log_level: str = os.environ.get("SPRUCE_LOG_LEVEL", "INFO").upper()
        self.log_format: str = os.environ.get(
            "SPRUCE_LOG_FORMAT", 
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.log_file: Optional[str] = os.environ.get("SPRUCE_LOG_FILE")
        
        # Initialize logging
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Configure logging for the application."""
        # Get numeric log level
        numeric_level = getattr(logging, self.log_level, logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(self.log_format)
        
        # Get root logger for sprucedb
        root_logger = logging.getLogger("sprucedb")
        root_logger.setLevel(numeric_level)
        
        # Remove existing handlers to avoid duplicates
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(numeric_level)
        root_logger.addHandler(console_handler)
        
        # File handler if specified
        if self.log_file:
            try:
                file_handler = logging.FileHandler(self.log_file)
                file_handler.setFormatter(formatter)
                file_handler.setLevel(numeric_level)
                root_logger.addHandler(file_handler)
            except OSError as e:
                # Fall back to console logging only
                root_logger.warning("Failed to open log file %s: %s", self.log_file, e)
