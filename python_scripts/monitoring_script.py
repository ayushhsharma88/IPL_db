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
        cur.execute("DELETE FROM players WHERE partition_name = %s", (partition,))
        cur.execute("DELETE FROM matches WHERE partition_name = %s", (partition,))
        cur.execute("DELETE FROM deliveries WHERE partition_name = %s", (partition,))
        print(f"Deleted rows for missing HDFS partition: {partition}")

conn.commit()
cur.close()
conn.close()

print("PARTITION CLEANUP COMPLETED")
