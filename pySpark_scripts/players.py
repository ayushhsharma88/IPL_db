#====================================================================================================================================#
#                                                                                                                                    #
#                 *************************************************************************************************                  #
#                 *               This Is pySpark script, used to load data in PostgreSQL database                *                  #
#                 *************************************************************************************************                  #
#                                                                                                                                    #
#             Script Name  = players.py                                                                                              #
#             Description  = This PySpark script reads the 'players.csv' file from HDFS, processes and cleans the data,              #
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
from pyspark.sql.functions import trim, col, lit
from datetime import datetime
import subprocess
import psycopg2
import traceback
import sys

# --- Start Timer ---
script_name = "players_script"
start_time = datetime.now()
execution_date = start_time.date()
status = "Started"

try:
    # Generate Partition Name
    table_name = "players"
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    partition_name = f"{table_name}_{timestamp}"
    hdfs_partition_path = f"/partitions/{partition_name}"

    # Initiate Spark session
    spark = SparkSession.builder.appName("Players.csv script").getOrCreate()

    # Read CSV from HDFS
    players = spark.read.csv('hdfs://localhost:9000/files/players.csv', sep=',', header=True, inferSchema=True)

    # Transformations
    players = players.withColumn("average", col("average").try_cast("int"))
    players = players.select([trim(col(c)).alias(c) if players.schema[c].dataType.simpleString() == 'string' else col(c) for c in players.columns])
    players = players.withColumn("partition_name", lit(partition_name))

    # Write to PostgreSQL
    players.write.format("jdbc")\
        .option("url", "jdbc:postgresql_url")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "players")\
        .option("user", "db_user")\
        .option("password", "db_password")\
        .mode('append')\
        .save()

    # Write to HDFS partition
    players.write.mode("overwrite").csv(f"hdfs://localhost:9000{hdfs_partition_path}")

    # Write row count to file
    inserted_count = players.count()
    with open("/home/hadoop/row_counts/players_count.txt", "w") as f:
        f.write(str(inserted_count))

    # Log to hdfs_partition_log table
    conn = psycopg2.connect(dbname="IPL_db", user="db_user", password="db_password", host="localhost")
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

except Exception as e:
    status = "Failed"
    print(f"[ERROR] Script failed: {e}")
    traceback.print_exc()

finally:
    end_time = datetime.now()
    run_time = end_time - start_time

    # Log script run metadata
    try:
        conn = psycopg2.connect(dbname="IPL_db", user="db_user", password="db_password", host="192.168.1.9")
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO script_execution_log (script_name, execution_date, start_time, end_time, run_time, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (script_name, execution_date, start_time, end_time, run_time, status))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Could not log script execution: {e}")

#hadoop command to move file from landing location to archives
subprocess.run(['hdfs','dfs','-mv','/files/players.csv','/archives'])

print("*****SPARK JOB HAS RUN SUCCESSFULLY.*****")
print("******TRANSFORMATION HAS BEED DONE.******")
print("**PLAYERS.CSV FILE COPIED TO ARCHIVES.**")
print("*********PLAYERS.CSV HAS LOADED.*********")