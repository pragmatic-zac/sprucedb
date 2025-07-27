from src.configuration import Configuration
from src.database import Database

def main() -> None:
    # os.environ["SPRUCE_LOG_LEVEL"] = "DEBUG"  # For detailed debugging
    # os.environ["SPRUCE_LOG_FILE"] = "sprucedb.log"  # For file logging
    
    config = Configuration()
    db = Database(config)
    
    db.put("user:123", b"michael_scott")
    result = db.get("user:123")
    if result is not None:
        print(f"Retrieved: {result.decode('utf-8')}")
    else:
        print("Retrieved: None")
    
    missing = db.get("user:999")
    if missing is not None:
        print(f"Missing key result: {missing.decode('utf-8')}")
    else:
        print("Missing key result: None")
    db.close()

if __name__ == "__main__":
    main()