import os
import requests
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import ZipFile
import shutil


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
        self._clean_and_create_directories()
    
    def _clean_and_create_directories(self):
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
    
    def _get_15_minute_intervals(self, base_datetime):
        """Get all 4 intervals for a specific hour (00, 15, 30, 45 minutes)."""
        base_hour = base_datetime.replace(minute=0, second=0, microsecond=0)
        intervals = []
        
        for minute in [0, 15, 30, 45]:
            intervals.append(base_hour.replace(minute=minute))
            
        return intervals
    
    def _get_daily_intervals(self, target_date):
        """Get all 15-minute intervals for a full day (24 hours × 4 intervals = 96 intervals)."""
        intervals = []
        start_dt = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        for hour in range(24):
            for minute in [0, 15, 30, 45]:
                interval_dt = start_dt.replace(hour=hour, minute=minute)
                intervals.append(interval_dt)
            
        return intervals
    
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
    
    def download_data(self, datetime_str, duration="hour"):
        """
        Download GDELT data for specified duration (hour or day).
        
        Args:
            datetime_str (str): Date and time in format "YYYY-MM-DD HH:MM" for hour, 
                               or "YYYY-MM-DD" for day
            duration (str): "hour" for hourly data or "day" for daily data
        """
        try:
            if duration == "hour":
                # Parse the input datetime for hour
                target_dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
                print(f"Downloading GDELT data for hour: {target_dt.strftime('%Y-%m-%d %H:00')}")
                
                # Get all 4 intervals for the specified hour
                intervals = self._get_15_minute_intervals(target_dt)
                
            elif duration == "day":
                # Parse the input date for day
                target_dt = datetime.strptime(datetime_str, "%Y-%m-%d")
                print(f"Downloading GDELT data for day: {target_dt.strftime('%Y-%m-%d')}")
                
                # Get all intervals for the day (24 hours × 4 intervals)
                intervals = self._get_daily_intervals(target_dt)
                
            else:
                print("Error: Invalid duration. Use 'hour' or 'day'")
                return
            
            total_files = len(intervals) * len(self.file_types)
            downloaded_files = 0
            failed_files = 0
            
            print(f"Total files to download: {total_files}")  
            print(f"Total intervals: {len(intervals)}")
            
            for interval_dt in intervals:
                timestamp_str = interval_dt.strftime("%Y%m%d%H%M%S")
                print(f"\nProcessing interval: {interval_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                
                for folder_name, file_type in self.file_types.items():
                    # Construct URL and file path
                    url = f"{self.base_url}{timestamp_str}.{file_type}"
                    filename = f"{timestamp_str}.{file_type}"
                    save_path = self.base_dir / folder_name / filename
                    
                    # Download the file (no need to check if exists since we cleaned directories)
                    if self.download_file(url, save_path):
                        downloaded_files += 1
                    else:
                        failed_files += 1
            
            print(f"\nDownload summary:")
            print(f"Successfully downloaded: {downloaded_files}")
            print(f"Failed downloads: {failed_files}")
            print(f"Total attempted: {total_files}")

        except ValueError as e:
            print(f"Error: Invalid datetime format. Please use:")
            print(f"For hour: 'YYYY-MM-DD HH:MM' (e.g., '2024-01-15 14:30')")
            print(f"For day: 'YYYY-MM-DD' (e.g., '2024-01-15')")
        except Exception as e:
            print(f"Error: {str(e)}")
    
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


# Example usage
if __name__ == "__main__":
    
    if len(sys.argv) not in [3, 4]:
        print("Error: Invalid number of arguments")
        print("For hourly data: python script.py <data_directory> <date> <time>")
        print("For daily data: python script.py <data_directory> <date>")
        print("Examples:")
        print("  Hourly: python script.py data 2024-01-15 14:30")
        print("  Daily:  python script.py data 2024-01-15")
        sys.exit(1)
    
    date = sys.argv[2]
    data_dir = sys.argv[1]
    downloader = GDELTDownloader(base_dir=data_dir)
    
    if len(sys.argv) == 4:
        # Hourly download
        time = sys.argv[3]
        datetime_str = f"{date} {time}"
        print(f"Downloading hourly GDELT data for: {datetime_str}")
        downloader.download_data(datetime_str, duration="hour")
    else:
        # Daily download
        datetime_str = date
        print(f"Downloading daily GDELT data for: {datetime_str}")
        downloader.download_data(datetime_str, duration="day")
    
    downloader.list_downloaded_files()
    
    # Extract all zip files after downloading
    downloader.extract_all_zip_files()
    
    # List files again to show extracted CSVs
    print("\nAfter extraction:")
    downloader.list_downloaded_files()