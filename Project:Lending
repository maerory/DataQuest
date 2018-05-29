import pandas as pd
pd.options.display.max_columns = 99

#Check the first five rows to see what kind of data it contains
print(pd.read_csv("loans_2007.csv",nrows=5))

#Finding the right chunksize for the efficient use of memory
loans = pd.read_csv("loans_2007.csv", nrows=3200)
print(loans.memory_usage(deep=True).sum() / 1048576)

#Input expected chunk memory and find the appropriate chunk size
def find_chunk(filename, memory) {
    size = 0
    while (pd.read_csv(filename, nrows=memory).memory_usage(deep=True).sum() / 1048576 < memory):
        size += 100
}

#Find the column lengths
print('total columns = ', len(loans.columns))
print('numeric type columns = ', len(loans.select_dtypes(include=['float','int']).columns))
print('string type columns = ', len(loans.select_dtypes(include=['object']).columns))

#Find row length
chunk_iter = pd.read_csv("loans_2007.csv", nrows=3200)
total_rows = 0
for chunk in chunk_iter:
    total_rows += len(chunk)
print(total_rows)

#Check whether the column types differ by chunks
chunk_iter = pd.read_csv('loans_2007.csv', chunksize=3000)
for chunk in chunk_iter:
    print(chunk.dtypes.value_counts())

## Create dictionary (key: column, value: list of Series objects representing each chunk's value counts)
chunk_iter = pd.read_csv('loans_2007.csv', chunksize=3000)
str_cols_vc = {}
for chunk in chunk_iter:
    str_cols = chunk.select_dtypes(include=['object'])
    for col in str_cols.columns:
        current_col_vc = str_cols[col].value_counts()
        if col in str_cols_vc:
            str_cols_vc[col].append(current_col_vc)
        else:
            str_cols_vc[col] = [current_col_vc]
print(str_cols_vc)

## Combine the value counts.
combined_vcs = {}
for col in str_cols_vc:
    combined_vc = pd.concat(str_cols_vc[col])
    final_vc = combined_vc.groupby(combined_vc.index).sum()
    combined_vcs[col] = final_vc
print(combined_vcs.keys())

## Find columns with fewer unique values
obj_cols = loans.select_dtypes(include=['object'])
category_col = []
for col in obj_cols:
    num_unique_values = len(loans[col].unique())
    num_total_values = len(loans[col])
    print(num_unique_values, " values in ", col, " column.")
    if (num_unique_values / num_total_values) < 0.5:
        print(num_unique_values, " / ", num_total_values, "in ", col," columns.")
        category_col.append(col)
print(category_col)

#Find col without missing values
float_cols = loans.select_dtypes(include=['float'])
for col in float_cols:
    if loans[col].isnull().sum() == 0:
        print(col, " does not have a missing value!")

#Calculating total memory usage
chunk_iter = pd.read_csv("loans_2007.csv", chunksize=3200)
total_memory_usage = 0
for chunk in chunk_iter:
    total_memory_usage += chunk.memory_usage(deep=True)
print(total_memory_usage.sum() / 1048576, " megabytes of memory used") 

#Convert certain columns to date and categories
convert_col_dtypes = {
    "sub_grade": "category", "home_ownership": "category", 
    "verification_status": "category", "purpose": "category"
}

chunk_iter = pd.read_csv('loans_2007.csv', chunksize=3000, dtype=convert_col_dtypes, parse_dates=["issue_d","earliest_cr_line","last_pymnt_d","last_credit_pull_d"])
my_counts = {}
for chunk in chunk_iter:
    term_cleaned = chunk['term'].str.lstrip(" ").str.rstrip(" months")
    revol_cleaned = chunk['revol_util'].str.rstrip("%")
    chunk['term'] = pd.to_numeric(term_cleaned)
    chunk['revol_util'] = pd.to_numeric(revol_cleaned)
    float_cols = chunk.select_dtypes(include=['float'])
    for col in float_cols.columns:
        missing_values = len(chunk) - chunk[col].count()
        if col in my_counts:
            my_counts[col] = my_counts[col] + missing_values
        else:
            my_counts[col] = missing_values

print(chunk.dtypes)
print(my_counts)
