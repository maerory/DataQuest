import sqlite3
import pandas as pd

conn = sqlite3.connect('moma.db')
moma_iter = pd.read_csv("moma.csv",chunksize=1000)
for chunk in moma_iter:
    chunk.to_sql('exhibitions', conn, if_exists='append',index=False)

    results_df = pd.read_sql('PRAGMA table_info(exhibitions)', conn)
    print(results_df)

## Adding to the database table exhibitions by chunks
conn = sqlite3.connect('moma.db')
moma_iter = pd.read_csv('moma.csv',chunksize=1000)
for chunk in moma_iter:
    chunk['ExhibitionSortOrder'] = chunk['ExhibitionSortOrder'].astype('int16') ##Transforming exhibitionsortorder to int type
    chunk.to_sql("exhibitions", conn, if_exists='append', index=False) ##Append option adds to an existing table
results_df = pd.read_sql("PRAGMA table_info('exhibitions');", conn)

## Get the counts of each exhibitionids from our database
conn = sqlite3.connect('moma.db')
eid_counts = pd.read_sql("SELECT exhibitionid, COUNT(*) as counts FROM exhibitions GROUP BY exhibitionid ORDER BY counts DESC", conn)
print(eid_counts[:10])

## Do the same thing after you have loaded the data into pandas
q = 'select exhibitionid from exhibitions;'
eid_df = pd.read_sql(q, conn)
eid_pandas_counts = eid_df['ExhibitionID'].value_counts()
print(eid_pandas_counts[:10])
