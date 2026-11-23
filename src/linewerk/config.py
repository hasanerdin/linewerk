from pathlib import Path

def get_project_root() -> Path:
    current_path = Path(__file__).resolve()
    for parent in current_path.parents:
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent

    raise FileNotFoundError("Project root has not been found!")

ROOT_DIR = get_project_root()
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"