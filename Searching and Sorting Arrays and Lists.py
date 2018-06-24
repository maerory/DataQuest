import pandas as pd
import math

# Fetching the data from csv
data = pd.read_csv("amounts.csv")
amounts = list(data["Amount"])
times = [int(i) for i in list(data["Time"])]

## Linear search with time complexity of O(n)
def linear_search(array, search):
    indexes = []
    for i, item in enumerate(array):
        if item == search:
            indexes.append(i)
    return indexes

sevens = linear_search(times,7)

## Linear multi search that matches a list of multiple stuff
def linear_multi_search(array, search):
    indexes = []
    for index, item in enumerate(array):
        if item == search:
            indexes.append(index)
    return indexes

transactions = [[times[i], amounts[i]] for i in range(len(amounts))]
results = linear_multi_search(transactions, [56, 10.84])


## Swap function to change places within array
def swap(array, pos1, pos2):
    store = array[pos1]
    array[pos1] = array[pos2]
    array[pos2] = store
## insertion sort with O(n^2)
def insertion_sort(array):
    for i in range(1, len(array)):
        j = i
        while j > 0 and array[j - 1] > array[j]:
            swap(array, j, j-1)
            j-=1
## binary search, sorts the array first and search for the value
def binary_search(array, search):
    insertion_sort(array)
    m = 0
    i = 0
    z = len(array)-1
    while i <= z:
        m = math.floor(i + ((z-i)/2))
        if array[m] == search:
            return m
        if array[m] < search:
            i = m + 1
        if array[m] > search:
            z = m - 1
              
result = binary_search(times, 56)

## Another version of binary search that uses pythons natural function sort
def binary_search(array, search):
    array.sort()
    m = 0
    i = 0
    z = len(array) - 1
    while i<= z:
        m = math.floor(i + ((z - i) / 2))
        if array[m] == search:
            return m
        elif array[m] < search:
            i = m + 1
        elif array[m] > search:
            z = m - 1
            
transactions = ["{}_{}".format(times[i], amounts[i]) for i in range(len(amounts))]

result = binary_search(transactions, "56_10.84")

## This binary search returns the index at the end of search even if 
## the search fails to find the value
def binary_search(array, search):
    array.sort()
    m = 0
    i = 0
    z = len(array) - 1
    while i<= z:
        m = math.floor(i + ((z - i) / 2))
        if array[m] == search:
            return m
        elif array[m] < search:
            i = m + 1
        elif array[m] > search:
            z = m - 1
    return m
## Fuzzy match returns values that are between the lower and upper range
def fuzzy_match(array, lower, upper, m):
    j = m
    l = m+1
    matches = []
    while (j > 0) and (lower <= array[j] <= upper):
        matches.append(array[j])
        j -= 1
    while (l < len(array)) and (lower <= array[l] <= upper):
        matches.append(array[l])
        l += 1
    return matches

m = binary_search(amounts, 150)
matches = fuzzy_match(amounts, 100, 2000, m)
print(matches)
