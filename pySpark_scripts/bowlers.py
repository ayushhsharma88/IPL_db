#====================================================================================================================================#
#                                                                                                                                    #
#                 *************************************************************************************************                  #
#                 *               This Is pySpark script, used to load data in PostgreSQL database                *                  #
#                 *************************************************************************************************                  #
#                                                                                                                                    #
#             Script Name  = bowlers.py                                                                                              #
#             Description  = This PySpark script reads the 'bowlers.csv' file from HDFS, processes and cleans the data,              #
#                            and writes it to a PostgreSQL database.                                                                 #
#                            This script is intended for use in data pipelines where player stats are ingested                       #
#                            from HDFS and stored in a relational database for further analysis or reporting.                        #
#             Arguments    = None                                                                                                    #
#             Dependencies = send_failure_mail, send_success_mail                                                                    #
#             Author       = Ayush Sharma                                                                                            #
#             Email        = myproject.dea@gmail.com                                                                                 #
#             Date         = 18-04-2025 (dd-mm-yyyy format)                                                                          #
#                                                                                                                                    #
#                                                                                                                                    #
#====================================================================================================================================#


from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, lit, current_date, to_date
from datetime import datetime
import subprocess
import psycopg2
import traceback
import sys

# --- Start Timer ---
script_name = "bowlers_script"
start_time = datetime.now()
execution_date = start_time.date()
status = "Started"
data_loaded = "No"   # Default to No

try:
    # Generate Partition Name
    table_name = "bowlers"
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    partition_name = f"{table_name}_{timestamp}"
    hdfs_partition_path = f"/partitions/{partition_name}"

    # Initiate Spark session
    spark = SparkSession.builder.appName("bowlers.csv script").getOrCreate()

    # Read CSV from HDFS
    bowlers = spark.read.csv('hdfs://localhost:9000/files/bowlers/bowlers_*.csv', sep=',', header=True, inferSchema=True)

    # Transformations
    bowlers = bowlers.withColumn("bowling_avg", col("bowling_avg").try_cast("numeric"))
    bowlers = bowlers.withColumn("total_overs", col("total_overs").try_cast("numeric"))
    bowlers = bowlers.withColumn("economy_rate", col("economy_rate").try_cast("numeric"))
    bowlers = bowlers.withColumn("strike_rate", col("strike_rate").try_cast("numeric"))
    bowlers = bowlers.select([trim(col(c)).alias(c) if bowlers.schema[c].dataType.simpleString() == 'string' else col(c) for c in bowlers.columns])

    # Add New Column
    bowlers = bowlers.withColumn("partition_name", lit(partition_name))
    bowlers = bowlers.withColumn("load_date", to_date(current_date()))

    # Write to PostgreSQL
    bowlers.write.format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/IPL_db")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "bowlers")\
        .option("user", "postgres")\
        .option("password", "password")\
        .mode('append')\
        .save()

    # If write was successful
    data_loaded = "Yes"

    # Write to HDFS partition
    bowlers.write.mode("overwrite").option("header", True).csv(f"hdfs://localhost:9000{hdfs_partition_path}")

    # Write row count to file
    inserted_count = bowlers.count()
    with open("/home/hadoop/row_counts/bowlers_count.txt", "w") as f:
        f.write(str(inserted_count))

    # Log to hdfs_partition_log table
    conn = psycopg2.connect(dbname="IPL_db", user="postgres", password="password", host="localhost")
    cur = conn.cursor()
    date = datetime.today().date()
    cur.execute("""
        INSERT INTO hdfs_partition_log (table_name, partition_name, hdfs_path, date)
        VALUES (%s, %s, %s, %s)
    """, (table_name, partition_name, hdfs_partition_path, date))
    conn.commit()
    cur.close()
    conn.close()

    status = "Success"

    print("*****SPARK JOB HAS RUN SUCCESSFULLY.*****")
    print("******TRANSFORMATION HAS BEED DONE.******")
    print("***BOWLERS.CSV FILE COPIED TO ARCHIVES.**")
    print("*********BOWLERS.CSV HAS LOADED.*********")

except Exception as e:
    status = "Failed"
    print(f"[ERROR] Script failed: {e}")
    traceback.print_exc()
    sys.exit(1)


finally:
    end_time = datetime.now()
    run_time = end_time - start_time

    # Log script run metadata
    try:
        conn = psycopg2.connect(dbname="IPL_db", user="postgres", password="password", host="localhost")
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO script_execution_log (script_name, execution_date, start_time, end_time, run_time, data_loaded, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (script_name, execution_date, start_time, end_time, run_time, data_loaded, status))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Could not log script execution: {e}")

#hadoop command to move file from landing location to archives
subprocess.run(
    "hdfs dfs -mv /files/bowlers/bowlers_*.csv /archives/bowlers",
    shell=True
)


