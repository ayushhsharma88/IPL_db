import subprocess
import psycopg2
import time

start = time.time()

# Step 1: Fetch all partitions from DB
conn = psycopg2.connect(
    host="localhost", dbname="IPL_db", user="postgres", password="password"
)
cur = conn.cursor()
cur.execute("SELECT partition_name FROM hdfs_partition_log")
db_partitions = set(row[0] for row in cur.fetchall())

# Step 2: List all folders under /partitions from HDFS (only 1 subprocess call)
hdfs_ls = subprocess.run(
    ["hdfs", "dfs", "-ls", "/partitions"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)

# Parse existing partition folders
hdfs_partitions = set()
if hdfs_ls.returncode == 0:
    for line in hdfs_ls.stdout.splitlines():
        if "/partitions/" in line:
            parts = line.strip().split()
            if len(parts) == 8:  # typical output format
                path = parts[-1]
                partition = path.split("/")[-1]
                hdfs_partitions.add(partition)
else:
    print("Error listing HDFS directory:", hdfs_ls.stderr)

#print(f"Total partitions in HDFS: {len(hdfs_partitions)}")

# Step 3: Find missing ones
missing_partitions = db_partitions - hdfs_partitions
print(f"Missing partitions: {missing_partitions}")

# Step 4: Batch delete from all 4 tables
if missing_partitions:
    partition_tuple = tuple(missing_partitions)
    cur.execute("DELETE FROM batsmen WHERE partition_name IN %s", (partition_tuple,))
    cur.execute("DELETE FROM bowlers WHERE partition_name IN %s", (partition_tuple,))
    cur.execute("DELETE FROM matches WHERE partition_name IN %s", (partition_tuple,))
    cur.execute("DELETE FROM deliveries WHERE partition_name IN %s", (partition_tuple,))
    cur.execute("DELETE FROM hdfs_partition_log WHERE partition_name IN %s", (partition_tuple,))
    print(f"Deleted rows for {len(missing_partitions)} partitions.")

conn.commit()
cur.close()
conn.close()

print("PARTITION CLEANUP COMPLETED")
print("Total Time:", time.time() - start, "seconds")

