#====================================================================================================================================#
#                                                                                                                                    #
#                 *************************************************************************************************                  #
#                 *           This Is Airflow Dag, used to run and schedule pySpark scripts in sequence           *                  #
#                 *************************************************************************************************                  #
#                                                                                                                                    #
#             Script Name  = monitoring_script.py                                                                                    #
#             Description  = This script will track hdfs partition in hadoop and postgresql table.                                   #
#             Arguments    = None                                                                                                    #
#             Dependencies = None                                                                                                    #
#             Author       = Ayush Sharma                                                                                            #
#             Email        = myproject.dea@gmail.com                                                                                 #
#             Date         = 18-04-2025 (dd-mm-yyyy format)                                                                          #
#                                                                                                                                    #
#                                                                                                                                    #
#====================================================================================================================================#


import subprocess
import psycopg2

# Get list of partitions from DB
conn = psycopg2.connect(
    host="localhost", dbname="IPL_db", user="db_user", password="db_password"
)
cur = conn.cursor()
cur.execute("SELECT partition_name FROM hdfs_partition_log")
partitions = [row[0] for row in cur.fetchall()]

for partition in partitions:
    hdfs_path = f"/partitions/{partition}"
    result = subprocess.run(["hdfs", "dfs", "-test", "-d", hdfs_path])
    
    if result.returncode != 0:
        # HDFS partition does not exist → delete data from players and metadata
        cur.execute("DELETE FROM batsmen WHERE partition_name = %s", (partition,))
        cur.execute("DELETE FROM bowlers WHERE partition_name = %s", (partition,))
        cur.execute("DELETE FROM matches WHERE partition_name = %s", (partition,))
        cur.execute("DELETE FROM deliveries WHERE partition_name = %s", (partition,))
        print(f"Deleted rows for missing HDFS partition: {partition}")

conn.commit()
cur.close()
conn.close()

print("PARTITION CLEANUP COMPLETED")
