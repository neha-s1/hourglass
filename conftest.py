# Puts the repository root on sys.path so tests can import `examples.kvstore`.
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
