import io, csv, psycopg2
from datetime import *
from urllib import request

conn = psycopg2.connect(dbname="postgres", user="postgres") # Connect to DB initially created in PostGRE
conn.autocommit = True
cur = conn.cursor()
#cur.execute("CREATE USER admin WITH SUPERUSER PASSWORD 'admin'") # Create the superuser first
#cur.execute("CREATE DATABASE ihw OWNER admin")
conn.commit()
conn.close()

conn = psycopg2.connect(dbname="ihw", user="admin", password="admin")
cur = conn.cursor()

cur.execute("""CREATE TABLE storm (
            fid INTEGER,
            datetime TIMESTAMP,
            btid INTEGER,
            name VARCHAR(20),
            lat DECIMAL(3,1),
            long DECIMAL(4,1),
            wind_kts INTEGER,
            pressure INTEGER,
            cat char(2),
            basin VARCHAR(20),
            shape_leng REAL
            )
""")
cur.execute("CREATE USER data_viewer; GRANT SELECT, UPDATE, INSERT ON storm TO data_viewer")

response = request.urlopen('https://dq-content.s3.amazonaws.com/251/storm_data.csv')
reader = csv.reader(io.TextIOWrapper(response))
next(reader)
for row in reader:

    timestamp = datetime(int(row[1]), int(row[2]), int(row[3]), int(row[4][:2]), int(row[4][2:4])) #Create new time format, joining date and time of the storm

    updated_row = row[0:1] + row[5:]
    updated_row.insert(1, timestamp)
    
    cur.execute("INSERT INTO storm VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", updated_row)

#conn.commit()
conn.rollback()
conn.close()
