import os
import requests
import sys
from pathlib import Path
from zipfile import ZipFile
import shutil
from datetime import datetime, timedelta, timezone

class GDELTDownloader:
    """
    A class to download GDELT files organized in separate folders.
    """
    
    def __init__(self, base_dir="data"):
        
        self.base_dir = Path(base_dir)
        self.file_types = {
            "export": "export.CSV.zip",
            "mentions": "mentions.CSV.zip", 
            "gkg": "gkg.csv.zip"
        }
        self.base_url = "http://data.gdeltproject.org/gdeltv2/"
        
        # Clean and create directory structure
        self.clean_and_create_directories()
    
    def clean_and_create_directories(self):
        """Delete existing folders and create fresh directory structure."""
        # Clean base directory if it exists
        if self.base_dir.exists():
            print(f"Cleaning directory: {self.base_dir}")
            shutil.rmtree(self.base_dir)
        
        # Create fresh directory structure
        self.base_dir.mkdir(parents=True, exist_ok=True)
        for folder_name in self.file_types.keys():
            folder_path = self.base_dir / folder_name
            folder_path.mkdir(exist_ok=True)
            print(f"Created fresh directory: {folder_path}")

    def list_downloaded_files(self):
        """List all downloaded files in the directory structure."""
        print(f"\nFiles in {self.base_dir}:")
        print("="*50)
        
        for folder_name in self.file_types.keys():
            folder_path = self.base_dir / folder_name
            files = list(folder_path.glob("*"))
            
            zip_files = [f for f in files if f.suffix == '.zip']
            csv_files = [f for f in files if f.suffix == '.csv']
            
            print(f"\n{folder_name.upper()} ({len(files)} files):")
            print(f"  ZIP files: {len(zip_files)}, CSV files: {len(csv_files)}")
            
            for file_path in sorted(files):
                file_size = file_path.stat().st_size / (1024*1024)  # MB
                file_type = "ZIP" if file_path.suffix == ".zip" else "CSV"
                print(f"  {file_path.name} ({file_size:.1f} MB) [{file_type}]")

    def download_file(self, url, save_path):
        """
        Download a single file from URL.
        
        Args:
            url (str): URL to download from
            save_path (Path): Path to save the file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print(f"Downloading: {url}")
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                with open(save_path, "wb") as f:
                    f.write(response.content)
                print(f" Downloaded: {save_path}")
                return True
            else:
                print(f" Data not available: {url} (Status: {response.status_code})")
                return False
                
        except requests.RequestException as e:
            print(f" Error downloading {url}: {str(e)}")
            return False
    
    def extract_zip_file(self, zip_file_path):
        """
        Extract a zip file and store contents in the same folder, then delete the zip file.
        
        Args:
            zip_file_path (Path): Path to the zip file
        """
        try:
            output_dir = zip_file_path.parent
            print(f"Extracting: {zip_file_path.name}")
            
            with ZipFile(zip_file_path, "r") as zip_file:
                zip_file.extractall(output_dir)
            
            # Delete the zip file after successful extraction
            zip_file_path.unlink()
            print(f" Extracted and deleted: {zip_file_path.name}")
            
        except Exception as e:
            print(f" Error extracting {zip_file_path}: {str(e)}")
    
    def extract_all_zip_files(self):
        """
        Extract all zip files in all subdirectories and delete the zip files.
        """
        print(f"\nExtracting all zip files in {self.base_dir}...")
        print("="*50)
        
        total_extracted = 0
        
        for folder_name in self.file_types.keys():
            folder_path = self.base_dir / folder_name
            zip_files = list(folder_path.glob("*.zip"))
            
            print(f"\nExtracting {len(zip_files)} files in {folder_name}:")
            for zip_file in zip_files:
                self.extract_zip_file(zip_file)
                total_extracted += 1
        
        print(f"\n Total files extracted: {total_extracted}")

    def download_data(self,intervals):
        total_files = len(intervals) * len(self.file_types)
        downloaded_files = 0
        failed_files = 0
            
        for interval in intervals:
            for folder_name, file_type in self.file_types.items():
                    # Construct URL and file path
                url = f"{self.base_url}{interval}.{file_type}"
                filename = f"{interval}.{file_type}"
                save_path = self.base_dir / folder_name / filename
                    
                    # Download the file (no need to check if exists since we cleaned directories)
                if self.download_file(url, save_path):
                    downloaded_files += 1
                else:
                    failed_files += 1
            
            print(f"\nDownload summary:")
            print(f"Successfully downloaded: {downloaded_files}")
            print(f"Failed downloads: {failed_files}")
            print(f"Total Files: {total_files}")
    
def validate_and_round_datetime(datetime_str):
    try:
        dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    except ValueError:
        return None, None  # Invalid input

    # Round to nearest 15 minutes
    minute = dt.minute
    remainder = minute % 15
    if remainder < 7.5:
        rounded_minute = minute - remainder
    else:
        rounded_minute = minute + (15 - remainder)

    if rounded_minute == 60:
        dt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        dt = dt.replace(minute=rounded_minute, second=0, microsecond=0)

    # Must be earlier than current UTC
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    if dt >= now_utc:
        return None, None

    end_date_time = dt + timedelta(hours=3)

    return dt.strftime("%Y-%m-%d %H:%M"), end_date_time.strftime("%Y-%m-%d %H:%M")

def loop_through_15_min_intervals(start_time_str, end_time_str):
    # Convert input strings to datetime objects
    start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
    end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M")
    
    current_time = start_time
    formatted_timestamps = []

    # Loop through 15-minute intervals
    while current_time <= end_time:
        # Format timestamp as: YYYYMMDDHHMMSS
        timestamp_str = current_time.strftime("%Y%m%d%H%M%S")
        formatted_timestamps.append(timestamp_str)
        current_time += timedelta(minutes=15)

    # Join all timestamps into a single string (newline-separated)
  
    return formatted_timestamps

  
if __name__ == "__main__":
    if len(sys.argv) not in [3, 4, 5, 6]:
        print("Error: Invalid number of arguments")
        print("Usage: python script.py <data_directory> <start_date> [<start_time>] [<end_date> [<end_time>]]")
        print()
        print("Examples:")
        print("  Single hour:     python script.py data 2024-01-15 14:30")
        print("  Full day:        python script.py data 2024-01-15")
        print("  Custom range:    python script.py data 2024-01-15 14:30 2024-01-15 18:45")
        print("  Multi-day:       python script.py data 2024-01-15 2024-01-16")
        sys.exit(1)

    data_dir = sys.argv[1]
    start_date = sys.argv[2]
    start_time = sys.argv[3] if len(sys.argv) >= 4 and ":" in sys.argv[3] else "00:00"

    start_datetime, default_end = validate_and_round_datetime(start_date + " " + start_time)
    start_datetime is not None or sys.exit("Error")
    if len(sys.argv) == 6:
        end_datetime = sys.argv[4] + " " + sys.argv[5]
    else:
        end_datetime = default_end
    intervals=loop_through_15_min_intervals(start_datetime, end_datetime)
    print(intervals)
    downloader = GDELTDownloader(base_dir=data_dir)
    
    downloader.download_data(intervals)
    # Extract all zip files after downloading
    downloader.extract_all_zip_files()
    
    # List files again to show extracted CSVs
    print("\nAfter extraction:")
    downloader.list_downloaded_files()
   


