## Finding the number of unique elements in a row of a data table named query
## This code runs in linear time

counts_increments = 0
value_checks = 0
counts = {}
duplicates = []

for item in query:
    if item not in counts:
        counts[item] = 0
    counts_increments += 1
    counts[item] += 1
for count in counts:
    value_checks += 1
    if counts[item] > 1:
        duplicates.append(count)

print(counts_increments)
print(value_checks)
    
## We have two difference algorithms for finding dupicate elements in a row
## The first algorithm uses pandas defined function to find duplicates
## The second alorithm is using list to store values that have appeared before

import time
import statistics
import matplotlib.pyplot as plt

def pandas_algo():
    duplicate_series = query_series.duplicated()
    duplicate_values_series = query_series[duplicate_series]
    
def algo():
    counts = {}
    for item in query:
        if item not in counts:
            counts[item] = 0
        counts[item] += 1

    duplicates = []
    for key, val in counts.items():
        if val > 1:
            duplicates.append(key)
        
pandas_elapsed = []
for i in range(1000):
    start = time.time()
    pandas_algo()
    pandas_elapsed.append(time.time() - start)

elapsed = []
for i in range(1000):
    start = time.time()
    algo()
    elapsed.append(time.time() - start)

print(statistics.median(pandas_elapsed))
print(statistics.median(elapsed))

plt.hist(pandas_elapsed)
plt.show()
plt.hist(elapsed)

## Another algorithm that uses set to determine unique and duplicate values

import time
import statistics
def algo():
    unique = set()
    duplicates = set()
    for item in query:
        if item in unique:
            duplicates.add(item)
        else:
            unique.add(item)

elapsed = []
for i in range(1000):
    start = time.time()
    algo()
    elapsed.append(time.time() - start)

print(statistics.median(elapsed))

## Using groupby function to find product_link with hightest relevance for each unique query in "Ecommerce5000.csv" table
## Algo function is the same function using different functions

import time 
import statistics
import pandas as pd

def run_with_timing(func):
    elapsed =[]
    for i in range(10):
        start = time.time()
        func()
        elapsed.append(time.time() - start)
    return statistics.median(elapsed)

def pandas_algo():
    get_max_relevance = lambda x : x.loc[x['relevance'].idxmax(), "product_link"] ## loc function finds the data by using row names
    return data.groupby("query").apply(get_max_relevance)

def algo():
    links = {}
    for i, row in enumerate(query):
        if row not in links:
            links[row] = [0, ""]
        if relevance[i] > links[row][0]:
            links[row] = [relevance[i], product_link[i]]
    return links
    
print(run_with_timing(pandas_algo))
print(run_with_timing(algo))
