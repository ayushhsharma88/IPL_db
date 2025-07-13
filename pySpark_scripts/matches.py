#====================================================================================================================================#
#                                                                                                                                    #
#                 *************************************************************************************************                  #
#                 *               This Is pySpark script, used to load data in PostgreSQL database                *                  #
#                 *************************************************************************************************                  ##                                                                                                                                    #
#             Script Name  = matches.py                                                                                              #
#             Description  = This PySpark script reads the 'matches.csv' file from HDFS, processes and cleans the data,              #
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
import subprocess
from pyspark.sql.functions import when,trim,col,substring,lit
from datetime import datetime
import psycopg2
import traceback
import sys

# --- Start Timer ---
script_name = "matches_script"
start_time = datetime.now()
execution_date = start_time.date()
status = "Started"

try:
    #Generate Partition Name
    table_name = "matches"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    partition_name = f"{table_name}_{timestamp}"
    hdfs_partition_path = f"/partitions/{partition_name}"

    #initiate spark session
    spark = SparkSession.builder \
        .appName("Matches.csv script") \
        .getOrCreate()

    #read file from hdfs
    matches=spark.read.csv('hdfs://localhost:9000/files/matches.csv',sep=',',header=True,inferSchema=True)

    #extract year from date column and create a new column
    matches=matches.withColumn('season',substring("date",7,4))

    #create a new column with condition
    matches=matches.withColumn("rained",when(col("method")=="D/L","rain").otherwise(None))

    #trim extra spaces from start and end
    matches=matches.select([ trim(col(c)).alias(c) if matches.schema[c].dataType.simpleString() == 'string' else col(c)
        for c in matches.columns
    ])

    #change datatype of columns
    matches=matches.withColumn("player_id",col("player_id").try_cast("int"))
    matches=matches.withColumn("result_margin",col("result_margin").try_cast("int"))
    matches=matches.withColumn("target_runs",col("target_runs").try_cast("int"))
    matches=matches.withColumn("target_overs",col("target_overs").try_cast("int"))

    #Add Partition Column
    matches = matches.withColumn("partition_name", lit(partition_name))

    #load data to postgreSQL database
    matches.write.format("jdbc")\
    .option("url","jdbc:postgresql_url")\
    .option("driver","org.postgresql.Driver")\
    .option("dbtable","matches")\
    .option("user","db_user")\
    .option("password","db_password")\
    .mode('append')\
    .save()

    #Write to HDFS Partition
    matches.write.mode("overwrite").csv(f"hdfs://localhost:9000{hdfs_partition_path}")

    inserted_count = matches.count()

    with open("/home/hadoop/row_counts/matches_count.txt", "w") as f:
        f.write(str(inserted_count))

    #Log to Partition Table
    try:
        conn = psycopg2.connect(
            dbname="IPL_db", user="db_user", password="db_password", host="localhost"
        )
        cur = conn.cursor()
        date = datetime.today().date()
        cur.execute("""
            INSERT INTO hdfs_partition_log (table_name, partition_name, hdfs_path, date)
            VALUES (%s, %s, %s, %s)
        """, (table_name, partition_name, hdfs_partition_path, date))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Could not log partition: {e}")
    
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
        conn = psycopg2.connect(dbname="IPL_db", user="db_user", password="db_password", host="localhost")
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
subprocess.run(['hdfs','dfs','-mv','/files/matches.csv','/archives'])

print("*****SPARK JOB HAS RUN SUCCESSFULLY.*****")
print("******TRANSFORMATION HAS BEED DONE.******")
print("**MATCHES.CSV FILE COPIED TO ARCHIVES.**")
print("*********MATCHES.CSV HAS LOADED.*********")