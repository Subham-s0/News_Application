from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import os
from pathlib import Path

# Create Spark session
spark = SparkSession.builder \
    .appName("GDELT_Selective_Transform") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Method 1: Create schemas with only required columns (Recommended)
# -------------------------------
# EVENTS - Only selected columns schema
# -------------------------------
def get_events_selected_schema():
    """Schema for only the selected events columns"""
    return StructType([
        StructField("GlobalEventID", IntegerType(), True),
        StructField("SQLDATE", IntegerType(), True), 
        StructField("Actor1Name", StringType(), True),
        StructField("Actor2Name", StringType(), True),
        StructField("EventCode", StringType(), True),
        StructField("EventBaseCode", StringType(), True),
        StructField("EventRootCode", StringType(), True),
        StructField("QuadClass", IntegerType(), True),
        StructField("GoldsteinScale", DoubleType(), True),
        StructField("NumMentions", IntegerType(), True),
        StructField("AvgTone", DoubleType(), True),
        StructField("Actor1Geo_Lat", DoubleType(), True),
        StructField("Actor1Geo_Long", DoubleType(), True),
        StructField("Actor2Geo_Lat", DoubleType(), True),
        StructField("Actor2Geo_Long", DoubleType(), True),
        StructField("ActionGeo_Lat", DoubleType(), True),
        StructField("ActionGeo_Long", DoubleType(), True),
        StructField("DateAdded", StringType(), True),
        StructField("SOURCEURL", StringType(), True)
    ])

# -------------------------------
# MENTIONS - Only selected columns schema  
# -------------------------------
def get_mentions_selected_schema():
    """Schema for only the selected mentions columns"""
    return StructType([
        StructField("GlobalEventID", LongType(), True),
        StructField("MentionType", IntegerType(), True),
        StructField("MentionSourceName", StringType(), True),
        StructField("MentionIdentifier", StringType(), True),
        StructField("SentenceID", IntegerType(), True)
    ])

# -------------------------------
# GKG - Only selected columns schema
# -------------------------------
def get_gkg_selected_schema():
    """Schema for only the selected GKG columns"""
    return StructType([
        StructField("GKGRECORDID", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("SourceCommonName", StringType(), True),
        StructField("DocumentIdentifier", StringType(), True),
        StructField("Themes", StringType(), True),
        StructField("Persons", StringType(), True),
        StructField("Organizations", StringType(), True),
        StructField("SharingImage", StringType(), True),
        StructField("AllNames", StringType(), True),
        StructField("Extras", StringType(), True)
    ])

# Method 2: Read full file then select columns (Alternative approach)
# -------------------------------
def read_and_select_columns(file_path, column_positions, column_names, delimiter="\t"):
    """
    Read CSV file and select specific columns by position
    
    Args:
        file_path: Path to the CSV file
        column_positions: List of column positions to select (0-indexed)
        column_names: List of names for the selected columns
        delimiter: File delimiter (default: tab)
    
    Returns:
        DataFrame with selected columns and proper headers
    """
    # Read the full file first without schema
    df_full = spark.read \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("delimiter", delimiter) \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(file_path)
    
    # Select only required columns by position and rename them
    selected_columns = []
    for i, pos in enumerate(column_positions):
        if pos < len(df_full.columns):
            selected_columns.append(col(f"_c{pos}").alias(column_names[i]))
    
    df_selected = df_full.select(*selected_columns)
    return df_selected

# -------------------------------
# Processing Functions
# -------------------------------

def process_events_selective(input_file, output_file):
    """Process events file with only selected columns"""
    print(f"Processing Events: {input_file}")
    
    # Column positions in original GDELT events file
    events_positions = [0, 1, 6, 16, 26, 27, 28, 29, 30, 33, 34, 40, 41, 48, 49, 56, 57, 59, 60]
    events_names = [
        "GlobalEventID", "SQLDATE", "Actor1Name", "Actor2Name", "EventCode",
        "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale",
        "NumMentions", "AvgTone", "Actor1Geo_Lat", "Actor1Geo_Long",
        "Actor2Geo_Lat", "Actor2Geo_Long", "ActionGeo_Lat", "ActionGeo_Long",
        "DateAdded", "SOURCEURL"
    ]
    
    # Read and select columns
    df = read_and_select_columns(input_file, events_positions, events_names)
    
    # Write with headers
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_file)
    
    print(f"✓ Events file processed and saved to: {output_file}")
    return df

def process_mentions_selective(input_file, output_file):
    """Process mentions file with only selected columns"""
    print(f"Processing Mentions: {input_file}")
    
    # Column positions in original GDELT mentions file  
    mentions_positions = [0, 2, 3, 5, 11]  # Based on your useful_col
    mentions_names = [
        "GlobalEventID", "MentionType", "MentionSourceName", 
        "MentionIdentifier", "SentenceID"
    ]
    
    # Read and select columns
    df = read_and_select_columns(input_file, mentions_positions, mentions_names)
    
    # Write with headers
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_file)
    
    print(f"✓ Mentions file processed and saved to: {output_file}")
    return df

def process_gkg_selective(input_file, output_file):
    """Process GKG file with only selected columns"""
    print(f"Processing GKG: {input_file}")
    
    # Column positions in original GDELT GKG file
    gkg_positions = [0, 1, 3, 4, 7, 11, 13, 18, 23, 26]  # Based on your selected_col
    gkg_names = [
        "GKGRECORDID", "DATE", "SourceCommonName", "DocumentIdentifier",
        "Themes", "Persons", "Organizations", "SharingImage", "AllNames", "Extras"
    ]
    
    # Read and select columns
    df = read_and_select_columns(input_file, gkg_positions, gkg_names)
    
    # Write with headers
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_file)
    
    print(f"✓ GKG file processed and saved to: {output_file}")
    return df

# -------------------------------
# Utility function to clean up Spark output files
# -------------------------------
def cleanup_and_rename_spark_output(temp_output_path, final_output_path):
    """Clean up Spark temp files and rename the actual CSV file"""
    import shutil
    
    temp_path = Path(temp_output_path)
    final_path = Path(final_output_path)
    
    # Ensure final directory exists
    final_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Find the actual CSV file in the temp directory
    csv_files = list(temp_path.glob("part-*.csv"))
    
    if csv_files:
        # Move the CSV file to final location
        shutil.move(str(csv_files[0]), str(final_path))
        print(f"✓ File saved as: {final_path}")
        
        # Clean up temp directory
        if temp_path.exists():
            shutil.rmtree(temp_path, ignore_errors=True)
    else:
        print(f"✗ No CSV files found in {temp_path}")

# -------------------------------
# Updated processing functions with proper file structure
# -------------------------------
def process_events_selective_structured(input_file, output_base_dir):
    """Process events file with proper directory structure"""
    print(f"Processing Events: {input_file}")
    
    # Column positions in original GDELT events file
    events_positions = [0, 1, 6, 16, 26, 27, 28, 29, 30, 33, 34, 40, 41, 48, 49, 56, 57, 59, 60]
    events_names = [
        "GlobalEventID", "SQLDATE", "Actor1Name", "Actor2Name", "EventCode",
        "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale",
        "NumMentions", "AvgTone", "Actor1Geo_Lat", "Actor1Geo_Long",
        "Actor2Geo_Lat", "Actor2Geo_Long", "ActionGeo_Lat", "ActionGeo_Long",
        "DateAdded", "SOURCEURL"
    ]
    
    # Read and select columns
    df = read_and_select_columns(input_file, events_positions, events_names)
    
    # Create output directory structure: data/transform/export/
    export_output_dir = Path(output_base_dir) / "export"
    export_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get original filename without extension
    input_path = Path(input_file)
    original_filename = input_path.stem  # e.g., "20240101150000.export"
    
    # Temp and final paths
    temp_output = export_output_dir / f"temp_{original_filename}"
    final_output = export_output_dir / f"{original_filename}.csv"
    
    # Write with headers to temp location
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(temp_output))
    
    # Clean up and rename
    cleanup_and_rename_spark_output(temp_output, final_output)
    
    return df

def process_mentions_selective_structured(input_file, output_base_dir):
    """Process mentions file with proper directory structure"""
    print(f"Processing Mentions: {input_file}")
    
    # Column positions in original GDELT mentions file  
    mentions_positions = [0, 2, 3, 5, 11]
    mentions_names = [
        "GlobalEventID", "MentionType", "MentionSourceName", 
        "MentionIdentifier", "SentenceID"
    ]
    
    # Read and select columns
    df = read_and_select_columns(input_file, mentions_positions, mentions_names)
    
    # Create output directory structure: data/transform/mentions/
    mentions_output_dir = Path(output_base_dir) / "mentions"
    mentions_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get original filename without extension
    input_path = Path(input_file)
    original_filename = input_path.stem  # e.g., "20240101150000.mentions"
    
    # Temp and final paths
    temp_output = mentions_output_dir / f"temp_{original_filename}"
    final_output = mentions_output_dir / f"{original_filename}.csv"
    
    # Write with headers to temp location
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(temp_output))
    
    # Clean up and rename
    cleanup_and_rename_spark_output(temp_output, final_output)
    
    return df

def process_gkg_selective_structured(input_file, output_base_dir):
    """Process GKG file with proper directory structure"""
    print(f"Processing GKG: {input_file}")
    
    # Column positions in original GDELT GKG file
    gkg_positions = [0, 1, 3, 4, 7, 11, 13, 18, 23, 26]
    gkg_names = [
        "GKGRECORDID", "DATE", "SourceCommonName", "DocumentIdentifier",
        "Themes", "Persons", "Organizations", "SharingImage", "AllNames", "Extras"
    ]
    
    # Read and select columns
    df = read_and_select_columns(input_file, gkg_positions, gkg_names)
    
    # Create output directory structure: data/transform/gkg/
    gkg_output_dir = Path(output_base_dir) / "gkg"
    gkg_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get original filename without extension
    input_path = Path(input_file)
    original_filename = input_path.stem  # e.g., "20240101150000.gkg"
    
    # Temp and final paths
    temp_output = gkg_output_dir / f"temp_{original_filename}"
    final_output = gkg_output_dir / f"{original_filename}.csv"
    
    # Write with headers to temp location
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(temp_output))
    
    # Clean up and rename
    cleanup_and_rename_spark_output(temp_output, final_output)
    
    return df

# -------------------------------
# Main processing function with proper file structure
# -------------------------------
def process_gdelt_files_selective(input_dir, output_dir):
    """
    Process all GDELT files in directory with selective column extraction
    Maintains proper directory structure: data/transform/export/, data/transform/mentions/, data/transform/gkg/
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Process different file types with structured output
    file_processors = {
        "export": process_events_selective_structured,
        "mentions": process_mentions_selective_structured, 
        "gkg": process_gkg_selective_structured
    }
    
    summary = {"export": 0, "mentions": 0, "gkg": 0, "errors": 0}
    
    for file_type, process_func in file_processors.items():
        print(f"\n{'='*60}")
        print(f"PROCESSING {file_type.upper()} FILES")
        print(f"Output directory: {output_path}/{file_type}/")
        print(f"{'='*60}")
        
        # Find files of this type
        pattern = f"*.{file_type}.*"
        files = list(input_path.glob(pattern))
        
        if not files:
            print(f"No {file_type} files found with pattern: {pattern}")
            continue
            
        print(f"Found {len(files)} {file_type} files to process")
        
        for file_path in files:
            try:
                process_func(str(file_path), str(output_path))
                summary[file_type] += 1
            except Exception as e:
                print(f"✗ Error processing {file_path.name}: {e}")
                summary["errors"] += 1
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Export files processed: {summary['export']}")
    print(f"Mentions files processed: {summary['mentions']}")
    print(f"GKG files processed: {summary['gkg']}")
    print(f"Errors encountered: {summary['errors']}")
    print(f"Output directory structure:")
    print(f"  {output_path}/export/")
    print(f"  {output_path}/mentions/")
    print(f"  {output_path}/gkg/")
    
    return summary

# -------------------------------
# Alternative: Direct schema-based reading (Method 1)
# -------------------------------
def process_with_custom_schema(file_path, schema, output_path, delimiter="\t"):
    """
    Process file using custom schema (only reads required columns)
    Note: This works best when you can restructure your source files
    """
    df = spark.read \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("delimiter", delimiter) \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .schema(schema) \
        .csv(file_path)
    
    # Write with headers
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
    
    return df

# -------------------------------
# Example usage
# -------------------------------
if __name__ == "__main__":
    # Single file processing
    events_file = r"D:\GDELT\s\20250815183000.export.CSV"
    mentions_file = r"D:\GDELT\s\20250815183000.mentions.CSV" 
    gkg_file = r"D:\GDELT\s\20250815183000.gkg.csv"
    
    output_dir = r"D:\GDELT\transformed"
    
    # Process individual files
    # process_events_selective(events_file, f"{output_dir}/events_selected")
    # process_mentions_selective(mentions_file, f"{output_dir}/mentions_selected")
    # process_gkg_selective(gkg_file, f"{output_dir}/gkg_selected")
    
    # Or process entire directory
    input_dir = r"D:\GDELT\s"
    process_gdelt_files_selective(input_dir, output_dir)
    
    # Stop Spark session
    spark.stop()