from pathlib import Path

# Path to this file
CONFIG_PATH = Path(__file__).resolve()

# Project root = one level above 'src'
BASE_DIR = CONFIG_PATH.parent.parent
DATA_DIR = BASE_DIR / "data"
# Now you can reference other folders relative to BASE_DIR
FAILED = DATA_DIR / "failed"