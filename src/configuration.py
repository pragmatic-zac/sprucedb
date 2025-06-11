import os

class Configuration:
    def __init__(self) -> None:
        self.base_path: str = os.environ.get("SPRUCE_BASE_PATH", "spruce_data")
