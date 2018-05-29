import pandas as pd
import matplotlib.pyplot as plt

# Read the file in chunks and calculate the memory usage of each chunks in megabyte
# then plot it on a histogram
chunk_iter = pd.read_csv("moma.csv", chunksize=250)
memory_footprints = [chunk.memory_usage(deep=True).sum() / 1048576 for chunk in chunk_iter]
plt.hist(memory_footprints)
plt.show()

# Calculate the number of rows in chuncks
chunk_iter = pd.read_csv("moma.csv", chunksize=250)
num_rows = sum([len(chunk) for chunk in chunk_iter])

print(num_rows)

# Perform operations (calculating the constituents) on each chunk and combine them again using concat
lifespans = []
dtypes = {
    'ConstituentBeginDate': float,
    'ConstituentEndDate': float,
}
chunk_iter = pd.read_csv("moma.csv", chunksize=250, dtype=dtypes)
for chunk in chunk_iter:
    lifespans.append(chunk['ConstituentEndDate'] - chunk['ConstituentBeginDate'])
lifespans_dist = pd.concat(lifespans)

# Use batch-processing to collect all the numbers of gender 
chunk_iter = pd.read_csv("moma.csv", chunksize=250, usecols=['Gender'])
overall_vc = [chunk['Gender'].value_counts() for chunk in chunk_iter] #Use value_counts() method to aggregate the values of a column
combined_vc = pd.concat(overall_vc)
print(combined_vc)
final_vc = combined_vc.groupby(combined_vc.index).sum()
print( final_vc)

# group by exhibition and count the gender partipants of the exhibition
chunk_iter = pd.read_csv("moma.csv", chunksize=1000)
df_list = []
for chunk in chunk_iter:
    temp = chunk['Gender'].groupby(chunk['ExhibitionID']).value_counts()
    df_list.append(temp)
final_df = pd.concat(df_list)
id_gender_counts = final_df.groupby(final_df.index).sum()
