import zipfile
import tempfile
import shutil
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi

from linewerk.config import RAW_DIR

TARGET_SCOPE = [
    "train_numeric.csv",
    "train_date.csv",
    "test_numeric.csv",
    "test_date.csv"
]

COMPETITION_SLUG = "bosch-production-line-performance"

def format_size(bytes_size: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"

        bytes_size /= 1024

    return f"{bytes_size:.2f} PB"

def check_file_exist(file_name: str) -> bool:
    target_file_path = RAW_DIR / file_name
    done_flag_path = RAW_DIR / f"{file_name}.done"
    if target_file_path.exists() and done_flag_path.exists():
        extracted_size = target_file_path.stat().st_size
        print(f"File already exists. Extracted size: {format_size(extracted_size)}")
        return True

    return False

def download_file(kaggle_api: KaggleApi, file_name: str) -> int:
    download_path = f"{file_name}.zip"
    zip_file_path = RAW_DIR / download_path
    if not zip_file_path.exists():
        with tempfile.TemporaryDirectory(dir=RAW_DIR) as temp_dir:
            kaggle_api.competition_download_file(competition=COMPETITION_SLUG, file_name=download_path, path=temp_dir)
            
            temp_file_path = Path(temp_dir, download_path)
            shutil.move(temp_file_path, zip_file_path)
        
    return zip_file_path.stat().st_size
    

def unzip_file(file_name: str) -> int:
    with tempfile.TemporaryDirectory(dir=RAW_DIR) as temp_dir:
        zip_file_path = RAW_DIR / f"{file_name}.zip"
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
            
        unzipped_file_path = Path(temp_dir, file_name)
        target_file_path = RAW_DIR / file_name
        shutil.move(unzipped_file_path, target_file_path)

        zip_file_path.unlink()

        return target_file_path.stat().st_size

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    for file_name in TARGET_SCOPE:
        if check_file_exist(file_name):
            continue

        target_file_path = RAW_DIR / file_name
        if target_file_path.exists():
            target_file_path.unlink()

        zip_file_size = download_file(api, file_name)
        extracted_file_size = unzip_file(file_name)

        print(f"Zip Size: {format_size(zip_file_size)} | Extracted Size: {format_size(extracted_file_size)}")

        done_flag_path = RAW_DIR / f"{file_name}.done"
        with open(done_flag_path, "w") as f:
            f.write("OK")
    
if __name__ == "__main__":
    main()