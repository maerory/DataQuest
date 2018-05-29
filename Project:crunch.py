import pandas as pd
pd.options.display.max_columns = 99

crunch = pd.read_csv("crunchbase-investments.csv",nrows=5)
print(crunch)

# Calculate memory footprint for each col.
counter = 0
series_memory_fp = pd.Series()
chunk_iter = pd.read_csv("crunchbase-investments.csv", chunksize=5000, encoding='ISO-8859-1')
for chunk in chunk_iter:
    if counter == 0:
        series_memory_fp = chunk.memory_usage(deep=True)/ 1048576
    else:
        series_memory_fp += chunk.memory_usage(deep=True)/ 1048576
    counter += 1
    
series_memory_fp.sum()

#Chunk size
loan = pd.read_csv('crunchbase-investments.csv', nrows=5000,  encoding='ISO-8859-1')
loan.memory_usage(deep=True).sum() / 1048576

null_list = [] 
chunk_iter = pd.read_csv("crunchbase-investments.csv", chunksize=5000, encoding='ISO-8859-1')
for chunk in chunk_iter:
    null_list.append(chunk.isnull().sum())

combined_nulls = pd.concat(null_list)
unique_combined_nulls = combined_nulls.groupby(combined_nulls.index).sum()
print(unique_combined_nulls)

drop_cols = ['company_permalink', 'investor_permalink','investor_category_code']
keep_cols = chunk.columns.drop(drop_cols)
keep_cols.tolist()

col_types = {}
chunk_iter = pd.read_csv('crunchbase-investments.csv', chunksize= 5000, encoding='ISO-8859-1', 
                         usecols=keep_cols)
for chunk in chunk_iter:
    for col in chunk.columns:
        if col not in col_types:
            col_types[col] = [str(chunk.dtypes[col])]
        else:
            col_types[col].append(str(chunk.dtypes[col]))
unique_col_types = {}
for k,v in col_types.items():
    unique_col_types[k] = set(col_types[k])
unique_col_types

for chunk in chunk_iter:
    object_cols = chunk.select_dtypes(include=['object'])
    for col in object_cols:
        num_unique_value = len(chunk[col].unique())
        num_total_value = len(chunk[col])


importimport  sqlite3sqlite3
conn  ==  sqlite3sqlite3..connectconnect(('crunchbase.db''crunchb )
chunk_iter = pd.read_csv('crunchbase-investments.csv', chunksize=5000, encoding='ISO-8859-1')

for chunk in chunk_iter:
    chunk.to_sql("investments", conn, if_exists='append', index=False)

