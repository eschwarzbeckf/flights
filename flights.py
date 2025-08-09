from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

def write_to_mariadb(batch_df, batch_id):
    """
    Writes a micro-batch DataFrame to a MariaDB table.
    This function is executed on a Spark executor for each batch.
    """
    print(f"--- Processing Batch ID: {batch_id} ---")
    
    # Caching the DataFrame can sometimes help with performance and reliability.
    batch_df.persist()
    
    # Ensure the dataframe is not empty to avoid creating unnecessary connections.
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty. Skipping write.")
        batch_df.unpersist()
        return

    print(f"Batch {batch_id} contains {batch_df.count()} rows to write.")

    # JDBC connection properties.
    # The key change is adding '?sessionVariables=sql_mode=ANSI_QUOTES' to the URL.
    # This tells MariaDB to treat double-quoted strings as identifiers, which matches Spark's default behavior.
    jdbc_url = "jdbc:mariadb://db:3306/airplanes?sessionVariables=sql_mode=ANSI_QUOTES"
    jdbc_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.mariadb.jdbc.Driver"
    }
    
    # Write the batch DataFrame to MariaDB.
    try:
        batch_df.write \
            .jdbc(url=jdbc_url, table="curr_flights", mode="overwrite", properties=jdbc_properties)
        print(f"Successfully wrote batch {batch_id} to MariaDB.")
    except Exception as e:
        print(f"ERROR: Failed to write batch {batch_id} to MariaDB.")
        print(f"Error details: {e}")
        # Re-raising the exception will cause the streaming query to fail.
        # You might want to handle this differently, e.g., write to a dead-letter queue.
        raise e
    finally:
        # Unpersist the DataFrame to free up memory.
        batch_df.unpersist()

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_JAVA_SDK_PACKAGE = "com.amazonaws:aws-java-sdk-bundle:1.12.367"
MARIADB_JAR_PACKAGE = "org.mariadb.jdbc:mariadb-java-client:3.2.0"
minio_path = "s3a://flights/"

print("\n--- Building spark session ---\n")
# Spark session build
spark = SparkSession.builder \
        .appName("Flights") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "8nf54yOno6QaSNgjTKQC") \
        .config("spark.hadoop.fs.s3a.secret.key", "bpD8MH2glh4vZ5Mr0kYlfhpJTTNv8bglKr74pW2U") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", f"{HADOOP_AWS_PACKAGE},{AWS_JAVA_SDK_PACKAGE},{MARIADB_JAR_PACKAGE}") \
        .getOrCreate()

# Create schema
df = spark.read\
        .csv(minio_path, header=True, inferSchema=True)

print("\n--- Getting schema ---\n")
flights_schema = df.schema

# Spark streaming frame
ss = spark.readStream\
    .format("csv")\
    .schema(flights_schema)\
    .option("header",True)\
    .option("maxFilesPerTrigger",1)\
    .option("latestFirst",True)\
    .option("cleanSource","delete")\
    .load(minio_path)

print("\n--- Creating Spark streaming dataframe ---\n")
# Filtered ss
ss_filter = ss.filter((ss.on_ground == False))\
            .filter(ss.callsign.isNotNull())\
            .filter(ss.latitude.isNotNull())\
            .filter(ss.longitude.isNotNull())\
            .withColumns({
                'last_contact':F.from_unixtime(ss.last_contact,'yyyy-MM-dd HH:mm:ss').cast(TimestampType()),
                'time_position':F.from_unixtime(ss.time_position,'yyyy-MM-dd HH:mm:ss').cast(TimestampType())
            })

print("--- Starting Spark Streaming ---")

# Spark Streaming Starts
query=ss_filter.writeStream\
    .outputMode("update")\
    .foreachBatch(write_to_mariadb)\
    .trigger(processingTime='3 seconds')\
    .start()

query.awaitTermination()
