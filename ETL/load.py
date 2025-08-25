import time
import sys
import os
import psycopg2
from pyspark.sql import SparkSession
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Util.Utility import setup_logging, format_time

def create_spark_session(logger, spark_config):
    """Initialize Spark session with PostgreSQL JDBC driver."""
    logger.debug("Initializing Spark Session with default parameters")
    return (SparkSession.builder
            .master("local[*]")
            .appName("GDELTDataLoad")
            .config("spark.driver.memory", spark_config["driver_memory"])
            .config("spark.executor.memory", spark_config["executor_memory"])
            .config("spark.executor.cores", spark_config["executor_cores"]) 
            .config("spark.executor.instances", spark_config["executor_instances"])
            .config("spark.jars", r"D:\GDELT\GDELT\GDELT\jars\postgresql-42.7.7.jar")  # Add PostgreSQL JDBC driver
            .config("spark.files.overwrite", "true")
            .getOrCreate()
    )

def create_postgres_tables(logger, pg_un, pg_pw, pg_host):
    """Create PostgreSQL tables for GDELT data if they don't exist using psycopg2."""
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(
            dbname="News",
            user=pg_un,  
            password=pg_pw,  
            host=pg_host,
            port="5432"
        )
        cursor = conn.cursor()
        
        logger.debug("Successfully connected to postgres database")

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS events (
                GlobalEventID VARCHAR(50) PRIMARY KEY,
                SQLDATE INTEGER,
                Actor1Name TEXT,
                Actor2Name TEXT,
                EventCode VARCHAR(10),
                EventBaseCode VARCHAR(10),
                EventRootCode VARCHAR(10),
                QuadClass INTEGER,
                GoldsteinScale DOUBLE PRECISION,
                NumMentions INTEGER,
                AvgTone DOUBLE PRECISION,
                Actor1Geo_Lat DOUBLE PRECISION,
                Actor1Geo_Long DOUBLE PRECISION,
                Actor2Geo_Lat DOUBLE PRECISION,
                Actor2Geo_Long DOUBLE PRECISION,
                ActionGeo_Lat DOUBLE PRECISION,
                ActionGeo_Long DOUBLE PRECISION,
                DateAdded BIGINT,
                SOURCEURL TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS mentions (
                GlobalEventID VARCHAR(50),
                MentionTimeDate BIGINT,
                MentionsSource TEXT,
                MentionIdentifier TEXT,
                Confidence DOUBLE PRECISION,
                FOREIGN KEY (GlobalEventID) REFERENCES events(GlobalEventID)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS gkg (
                GKGRECORDID VARCHAR(50) PRIMARY KEY,
                DATE TEXT,
                SourceCommonName TEXT,
                DocumentIdentifier TEXT,
                Themes TEXT,
                Persons TEXT,
                Organizations TEXT,
                SharingImage TEXT,
                RelatedImages TEXT,
                AllNames TEXT,
                Extras TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS gkg_themes (
                GKGRECORDID VARCHAR(50),
                theme TEXT,
                FOREIGN KEY (GKGRECORDID) REFERENCES gkg(GKGRECORDID)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS gkg_persons (
                GKGRECORDID VARCHAR(50),
                person TEXT,
                FOREIGN KEY (GKGRECORDID) REFERENCES gkg(GKGRECORDID)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS gkg_organizations (
                GKGRECORDID VARCHAR(50),
                organization TEXT,
                FOREIGN KEY (GKGRECORDID) REFERENCES gkg(GKGRECORDID)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS gkg_names (
                GKGRECORDID VARCHAR(50),
                name TEXT,
                FOREIGN KEY (GKGRECORDID) REFERENCES gkg(GKGRECORDID)
            );
            """
        ]
        
        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        logger.info("PostgreSQL tables for GDELT data created successfully")
        
    except Exception as e:
        logger.warning(f"Error creating tables: {e}")
    finally:
        logger.debug("Closing connection and cursor to postgres db")
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def truncate_all_tables_if_exists(logger, pg_un, pg_pw, pg_host):
    """Check if PostgreSQL tables exist and truncate them if they do."""
    tables = [
        ("export", "export", "events"),
        ("mentions", "mentions", "mentions"),
        ("gkg", "gkg", "gkg"),
        ("gkg", "gkg_theme", "gkg_themes"),
        ("gkg", "gkg_person", "gkg_persons"),
        ("gkg", "gkg_organization", "gkg_organizations"),
        ("gkg", "gkg_name", "gkg_names")
    ]

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="News",
            user=pg_un,
            password=pg_pw,
            host=pg_host,
            port="5432"
        )
        cursor = conn.cursor()

        for _, _, table_name in tables:
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = %s
                );
            """, (table_name,))
            exists = cursor.fetchone()[0]

            if exists:
                cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
                logger.info(f"Table '{table_name}' exists and was truncated.")
            else:
                logger.info(f"Table '{table_name}' does not exist, skipping truncate.")
        
        conn.commit()

    except Exception as e:
        logger.warning(f"Error checking/truncating tables: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_to_postgres(logger, spark, input_dir, pg_un, pg_pw, pg_host):
    """Load CSV files to PostgreSQL."""
    jdbc_url = f"jdbc:postgresql://{pg_host}:5432/News"
    connection_properties = {
        "user": pg_un,  
        "password": pg_pw, 
        "driver": "org.postgresql.Driver"
    }
    
    # Define the tables and their corresponding CSV paths
    tables = [
        ("export", "export", "events"),
        ("mentions", "mentions", "mentions"),
        ("gkg", "gkg", "gkg"),
        ("gkg", "gkg_theme", "gkg_themes"),
        ("gkg", "gkg_person", "gkg_persons"),
        ("gkg", "gkg_organization", "gkg_organizations"),
        ("gkg", "gkg_name", "gkg_names")
    ]
    
    for folder, file_pattern, table_name in tables:
        try:
            # Read CSV files
            csv_path = os.path.join(input_dir, folder, f"*{file_pattern}*.csv")
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(csv_path)
            
            # Write to PostgreSQL
            df.write \
                .mode("append") \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            
            logger.info(f"Loaded {table_name} to PostgreSQL from {csv_path}")
            
        except Exception as e:
            logger.warning(f"Error loading {table_name}: {e}")

if __name__ == "__main__":
    logger = setup_logging("load_gdelt.log")

    if len(sys.argv) != 10:
        logger.error("Usage: python load/execute.py <input_dir> <pg_un> <pg_pw> <pg_host> <master_ip> <d_mem> <e_mem> <e_core> <e_inst>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    pg_un = sys.argv[2]
    pg_pw = sys.argv[3]
    pg_host = sys.argv[4]

    spark_config = {}
    spark_config["master_ip"] = sys.argv[5]
    spark_config["driver_memory"] = sys.argv[6]
    spark_config["executor_memory"] = sys.argv[7]
    
    spark_config["executor_cores"] = sys.argv[8]
    spark_config["executor_instances"] = sys.argv[9]
    
    logger.info("GDELT Load stage started")
    start = time.time()

    spark = create_spark_session(logger, spark_config)
    spark.sparkContext.setLogLevel("WARN")  # Reduce Spark logging
    truncate_all_tables_if_exists(logger, pg_un, pg_pw, pg_host)
    create_postgres_tables(logger, pg_un, pg_pw, pg_host)
    load_to_postgres(logger, spark, input_dir, pg_un, pg_pw, pg_host)

    end = time.time()
    logger.info("GDELT Load stage completed")
    logger.info(f"Total time taken {format_time(end-start)}")