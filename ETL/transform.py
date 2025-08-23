import os
import shutil
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import split, explode, col, coalesce, lit, trim
from pyspark.sql import types as T


def get_dataframe(file_path: str, spark):
    """
    Read and process GDELT files based on file type with selective column reading
    Returns DataFrame with only required columns and proper data types
    """
    file = os.path.basename(file_path).lower()
    
    if "export" in file:
        raw_df = spark.read.csv(
            file_path,
            sep="\t",
            header=False,
            inferSchema=True,
            multiLine=True,
            escape='"'
        )
        return raw_df.select(
            raw_df["_c0"].cast(T.StringType()).alias("GlobalEventID"),
            raw_df["_c1"].cast(T.IntegerType()).alias("SQLDATE"),
            raw_df["_c6"].cast(T.StringType()).alias("Actor1Name"),
            raw_df["_c16"].cast(T.StringType()).alias("Actor2Name"),
            raw_df["_c26"].cast(T.StringType()).alias("EventCode"),
            raw_df["_c27"].cast(T.StringType()).alias("EventBaseCode"),
            raw_df["_c28"].cast(T.StringType()).alias("EventRootCode"),
            raw_df["_c29"].cast(T.IntegerType()).alias("QuadClass"),
            raw_df["_c30"].cast(T.DoubleType()).alias("GoldsteinScale"),
            raw_df["_c33"].cast(T.IntegerType()).alias("NumMentions"),
            raw_df["_c34"].cast(T.DoubleType()).alias("AvgTone"),
            raw_df["_c40"].cast(T.DoubleType()).alias("Actor1Geo_Lat"),
            raw_df["_c41"].cast(T.DoubleType()).alias("Actor1Geo_Long"),
            raw_df["_c48"].cast(T.DoubleType()).alias("Actor2Geo_Lat"),
            raw_df["_c49"].cast(T.DoubleType()).alias("Actor2Geo_Long"),
            raw_df["_c56"].cast(T.DoubleType()).alias("ActionGeo_Lat"),
            raw_df["_c57"].cast(T.DoubleType()).alias("ActionGeo_Long"),
            raw_df["_c59"].cast(T.IntegerType()).alias("DateAdded"),
            raw_df["_c60"].cast(T.StringType()).alias("SOURCEURL")
        )
    
    elif "mentions" in file:
        raw_df = spark.read.csv(
            file_path,
            sep="\t",
            header=False,
            inferSchema=True,
            multiLine=True,
            escape='"'
        )
        return raw_df.select(
            raw_df["_c0"].cast(T.StringType()).alias("GlobalEventID"),
            raw_df["_c2"].cast(T.StringType()).alias("MentionTimeDate"),
            raw_df["_c4"].cast(T.StringType()).alias("MentionsSource"),
            raw_df["_c5"].cast(T.StringType()).alias("MentionIdentifier"),
            raw_df["_c11"].cast(T.DoubleType()).alias("Confidence")
        )
    
    elif "gkg" in file:
        raw_df = spark.read.csv(
            file_path,
            sep="\t",
            header=False,
            inferSchema=True,
            multiLine=True,
            escape='"'
        )
        return raw_df.select(
            raw_df["_c0"].cast(T.StringType()).alias("GKGRECORDID"),
            raw_df["_c1"].cast(T.IntegerType()).alias("DATE"),
            raw_df["_c3"].cast(T.StringType()).alias("SourceCommonName"),
            raw_df["_c4"].cast(T.StringType()).alias("DocumentIdentifier"),
            raw_df["_c7"].cast(T.StringType()).alias("Themes"),
            raw_df["_c11"].cast(T.StringType()).alias("Persons"),
            raw_df["_c13"].cast(T.StringType()).alias("Organizations"),
            raw_df["_c18"].cast(T.StringType()).alias("SharingImage"),
            raw_df["_c19"].cast(T.StringType()).alias("RelatedImages"),
            raw_df["_c23"].cast(T.StringType()).alias("AllNames"),
            raw_df["_c26"].cast(T.StringType()).alias("Extras")
        )
    
    else:
        raise ValueError(f"Unknown file type for schema: {file}")





def create_spark_session():
    """Create Spark session with optimized configuration for GDELT processing"""
    return SparkSession.builder \
        .appName("GDELT_Transform") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()


def cleanup_metadata_files(directory):
    """
    Clean up Spark metadata files and other unwanted files from directory
    """
    directory_path = Path(directory)
    if not directory_path.exists():
        return
        
    unwanted_patterns = [
        "_SUCCESS", "_committed", "_started",
        "*.crc", "part-*", ".*"
    ]
    
    for item in directory_path.rglob("*"):
        try:
            # Remove Spark metadata files
            if item.name in ["_SUCCESS", "_committed", "_started"]:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                print(f"Removed metadata: {item}")
            
            # Remove .crc files and hidden files
            elif item.name.startswith(".") and item.suffix in [".crc", ""]:
                if item.is_file():
                    item.unlink()
                    print(f"Removed hidden/crc file: {item}")
            
            # Remove part files that aren't our final CSVs
            elif item.name.startswith("part-") and not item.parent.name.startswith("temp_"):
                if item.is_file():
                    item.unlink()
                    print(f"Removed part file: {item}")
                    
        except Exception as e:
            print(f"Warning: Could not remove {item}: {e}")


def write_clean_csv(df, output_dir, filename_base, max_retries=3):
    """
    Write Spark DataFrame as CSV preserving the base filename.
    Deletes Spark temp folders and _SUCCESS files with retry logic.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    temp_dir = output_path / f"temp_{filename_base}"
    dst_file = output_path / f"{filename_base}.csv"
    
    for attempt in range(max_retries):
        try:
            # Clean up any existing temp directory
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
            
            temp_dir.mkdir(exist_ok=True)
            
            # Write CSV with better error handling
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(temp_dir))
            
            # Find the CSV file (more robust search)
            csv_files = [f for f in temp_dir.iterdir() 
                        if f.is_file() and f.suffix == ".csv" and f.name.startswith("part-")]
            
            if not csv_files:
                raise FileNotFoundError(f"No CSV files found in {temp_dir}")
            
            src_file = csv_files[0]
            
            # Remove existing destination file if it exists
            if dst_file.exists():
                dst_file.unlink()
                print(f"Removed existing file: {dst_file}")
            
            # Move the file to final destination
            shutil.move(str(src_file), str(dst_file))
            
            # Clean up temp directory and Spark metadata
            cleanup_spark_files(output_path, temp_dir)
            
            print(f"CSV written successfully: {dst_file}")
            return True
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {filename_base}: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying... ({max_retries - attempt - 1} attempts left)")
                # Clean up before retry
                cleanup_spark_files(output_path, temp_dir)
            else:
                print(f"Failed to write {filename_base}.csv after {max_retries} attempts")
                cleanup_spark_files(output_path, temp_dir)
                raise
    
    return False


def cleanup_spark_files(output_dir, temp_dir=None):
    """Clean up Spark temporary files and metadata"""
    try:
        # Remove temp directory if provided
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        
        # Remove Spark metadata files from output directory
        output_path = Path(output_dir)
        if output_path.exists():
            for item in output_path.iterdir():
                if (item.name in ["_SUCCESS", "_committed", "_started"] or 
                    item.name.startswith(".") or 
                    (item.name.startswith("part-") and not item.name.endswith(".csv")) or
                    item.suffix == ".crc"):
                    try:
                        if item.is_file():
                            item.unlink()
                        elif item.is_dir():
                            shutil.rmtree(item, ignore_errors=True)
                    except Exception:
                        pass  # Ignore cleanup errors
    except Exception as e:
        print(f"Warning: Cleanup failed: {e}")


def process_events_folder(input_dir, output_dir):
    """Process events files with proper schema and column selection"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not input_path.exists():
        print(f"Input directory does not exist: {input_dir}")
        return False
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files
    csv_files = [f for f in input_path.iterdir() if f.suffix.lower() == '.csv']
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return False
    
    print(f"Found {len(csv_files)} CSV files in {input_dir}")
    
    success_count = 0
    for file_path in csv_files:
        file_base = file_path.stem
        print(f"Processing events: {file_path.name}")
        
        try:
            # Use the efficient get_dataframe method
            df = get_dataframe(str(file_path), spark)
            
            # Check if DataFrame is empty
            if df.count() == 0:
                print(f"Warning: {file_path.name} is empty, skipping...")
                continue
                
            # DataFrame already has the right columns and names
            if write_clean_csv(df, output_dir, file_base):
                success_count += 1
                
        except Exception as e:
            print(f"Error processing {file_path.name}: {str(e)}")
            continue
    
    # Clean up metadata files after processing
    cleanup_metadata_files(output_dir)
    
    print(f"Successfully processed {success_count}/{len(csv_files)} events files")
    return success_count > 0


def process_mentions_folder(input_dir, output_dir):
    """Process mentions files with proper schema and column selection"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not input_path.exists():
        print(f"Input directory does not exist: {input_dir}")
        return False
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files
    csv_files = list(input_path.glob("*.csv")) + list(input_path.glob("*.CSV"))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return False
    
    print(f"Found {len(csv_files)} CSV files in {input_dir}")
    
    success_count = 0
    for file_path in csv_files:
        file_base = file_path.stem
        print(f"Processing mentions: {file_path.name}")
        
        try:
            # Use the efficient get_dataframe method
            df = get_dataframe(str(file_path), spark)
            
            # Check if DataFrame is empty
            if df.count() == 0:
                print(f"Warning: {file_path.name} is empty, skipping...")
                continue
                
            # DataFrame already has the right columns and names
            if write_clean_csv(df, output_dir, file_base):
                success_count += 1
                
        except Exception as e:
            print(f"Error processing {file_path.name}: {str(e)}")
            continue
    
    # Clean up metadata files after processing
    cleanup_metadata_files(output_dir)
    
    print(f"Successfully processed {success_count}/{len(csv_files)} mentions files")
    return success_count > 0


def process_gkg_folder(input_dir, output_dir):
    """Process GKG files with proper schema and column selection"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not input_path.exists():
        print(f"Input directory does not exist: {input_dir}")
        return False
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files
    csv_files = list(input_path.glob("*.csv")) + list(input_path.glob("*.CSV"))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return False
    
    print(f"Found {len(csv_files)} GKG files in {input_dir}")
    
    success_count = 0
    for file_path in csv_files:
        file_base = file_path.stem
        print(f"Processing GKG: {file_path.name}")
        
        try:
            # Use the efficient get_dataframe method  
            df = get_dataframe(str(file_path), spark)
            
            # Check if DataFrame is empty
            if df.count() == 0:
                print(f"Warning: {file_path.name} is empty, skipping...")
                continue
            
            # Write main GKG CSV
            if not write_clean_csv(df, output_dir, file_base):
                print(f"Failed to write main GKG file: {file_base}")
                continue
            
            # Create exploded CSVs for each dimension
            explode_columns = {
                "Themes": "theme",
                "Persons": "person", 
                "Organizations": "organization",
                "AllNames": "name"
            }
            
            for col_name, new_col in explode_columns.items():
                try:
                    exploded_df = df.select(
                        col("GKGRECORDID"),
                        explode(split(coalesce(col(col_name), lit("")), ";")).alias(new_col)
                    ).filter(col(new_col) != "").withColumn(new_col, trim(col(new_col)))
                    
                    # Only write if there's data
                    if exploded_df.count() > 0:
                        write_clean_csv(exploded_df, output_dir, f"{file_base}_{new_col}")
                    else:
                        print(f"No data for {col_name} in {file_base}")
                        
                except Exception as e:
                    print(f"Error processing {col_name} for {file_base}: {str(e)}")
                    continue
            
            success_count += 1
            
        except Exception as e:
            print(f"Error processing GKG file {file_path.name}: {str(e)}")
            continue
    
    # Clean up metadata files after processing
    cleanup_metadata_files(output_dir)
    
    print(f"Successfully processed {success_count}/{len(csv_files)} GKG files")
    return success_count > 0


def main():
    """Main processing function with proper folder structure handling"""
    print("Starting GDELT Transform Process...")
    
    # Create Spark session
    global spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce Spark logging
    
    try:
        # Check command line arguments
        if len(sys.argv) != 3:
            print("Usage: python transform.py <extract_path> <transform_path>")
            print("Example: python transform.py data/extract data/transform")
            sys.exit(1)
        
        base_input = sys.argv[1]
        base_output = sys.argv[2]
        
        # Ensure base output directory exists
        Path(base_output).mkdir(parents=True, exist_ok=True)
        
        success_flags = []
        
        # Process events (export folder)
        print("\n" + "="*50)
        print("PROCESSING EVENTS")
        print("="*50)
        events_input = os.path.join(base_input, "export")
        events_output = os.path.join(base_output, "export")
        success_flags.append(process_events_folder(events_input, events_output))
        
        # Process mentions
        print("\n" + "="*50)
        print("PROCESSING MENTIONS")
        print("="*50)
        mentions_input = os.path.join(base_input, "mentions")
        mentions_output = os.path.join(base_output, "mentions")
        success_flags.append(process_mentions_folder(mentions_input, mentions_output))
        
        # Process GKG
        print("\n" + "="*50)
        print("PROCESSING GKG")
        print("="*50)
        gkg_input = os.path.join(base_input, "gkg")
        gkg_output = os.path.join(base_output, "gkg")
        success_flags.append(process_gkg_folder(gkg_input, gkg_output))
        
        # Final cleanup of any remaining metadata files
        print("\n" + "="*50)
        print("FINAL CLEANUP")
        print("="*50)
        cleanup_metadata_files(base_output)
        
        # Summary
        print("\n" + "="*50)
        print("TRANSFORM SUMMARY")
        print("="*50)
        print(f"Events: {'Success' if success_flags[0] else 'Failed'}")
        print(f"Mentions: {'Success' if success_flags[1] else 'Failed'}")
        print(f"GKG: {'Success' if success_flags[2] else 'Failed'}")
        
        if all(success_flags):
            print("\nAll transformations completed successfully!")
            print("All metadata files have been cleaned up.")
        else:
            print(f"\nSome transformations failed. Check logs above.")
            
    except Exception as e:
        print(f"\nFatal error in main process: {str(e)}")
        raise
    finally:
        # Always stop Spark session
        try:
            spark.stop()
            print("\nSpark session stopped")
        except:
            pass


if __name__ == "__main__":
    main()