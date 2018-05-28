import pandas as pd

moma = pd.read_csv("moma.csv") # Reading the moma data with moma csv
print(moma.info()) # moma.info() prints the memory usage of the dataframe
print(moma._data) # _data attribute of a dataframe retrieves different Block Managers that groups different columns
total_bytes = moma.size * 8 # size attribute finds the total byte of the dataframe
total_megabytes = total_bytes / 1048576
print(total_bytes)
print(total_megabytes)

obj_cols = moma.select_dtypes(include=['object']) #Select only the columns with object data types
obj_cols_mem = obj_cols.memory_usage(deep=True) # by setting the deep attribute to true you can view the specific details of memory usage
obj_cols_sum = obj_cols_mem.sum() /1048576
print(obj_cols_sum)

# Transforming one of moma table's column to a fitting int type to decrease the memory usage
col_max = moma['ExhibitionSortOrder'].max()
col_min = moma['ExhibitionSortOrder'].min()

if col_max < np.iinfo("int8").max and col_min > np.iinfo("int8").min:
    moma['ExhibitionSortOrder'] = moma['ExhibitionSortOrder'].astype("int8")
elif col_max < np.iinfo("int16").max and col_min > np.iinfo("int16").min:
    moma['ExhibitionSortOrder'] = moma['ExhibitionSortOrder'].astype("int16")
elif col_max < np.iinfo("int32").max and col_min > np.iinfo("int32").min:
    moma['ExhibitionSortOrder'] = moma['ExhibitionSortOrder'].astype("int32")
else: 
    moma['ExhibitionSortOrder'] = moma['ExhibitionSortOrder'].astype("int64")

print(moma['ExhibitionSortOrder'].dtype)
print(moma['ExhibitionSortOrder'].memory_usage(deep=True))

# Transform all float column to its appropriate data types
float_cols = moma.select_dtypes(include=['float'])
for col in float_cols:
    moma[col] = pd.to_numeric(moma[col], downcast='float') #To numeric function finds the suitable data type for the column

print(float_cols.dtypes)

# Converting the columns to datetime formats and checking the memory usage
moma["ExhibitionBeginDate"] = pd.to_datetime(moma["ExhibitionBeginDate"])
moma["ExhibitionEndDate"] = pd.to_datetime(moma["ExhibitionEndDate"])
print(moma[["ExhibitionBeginDate", "ExhibitionEndDate"]].memory_usage(deep=True))

# Find and transform the data type which are fit for 'category' data types
obj_cols = moma.select_dtypes(include=['object'])
for col in obj_cols:
    num_unique_values = len(moma[col].unique())
    num_total_values = len(moma[col])
    if num_unique_values / num_total_values < 0.5:
        moma[col] = moma[col].astype('category')

print(moma.info(memory_usage='deep'))

# Having special options when learning data
keep_cols = ['ExhibitionID', 'ExhibitionNumber', 'ExhibitionBeginDate', 'ExhibitionEndDate', 'ExhibitionSortOrder', 'ExhibitionRole', 'ConstituentType', 'DisplayName', 'Institution', 'Nationality', 'Gender']

moma = pd.read_csv('moma.csv', usecols = keep_cols, parse_dates = ["ExhibitionBeginDate", "ExhibitionEndDate"])
print(moma.memory_usage(deep=True).sum()/(1024*1024))
