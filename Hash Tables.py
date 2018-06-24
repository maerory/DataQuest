import os
import numpy as np
import time
import matplotlib.pyplot as plt

## Loading the quote data to variable quote
quotes = {}
for file in os.listdir("lines"):
    with open("lines/{}".format(file), 'r') as f:
        quote = f.read()
    quotes[file.replace(".txt", "")] = quote

## Simple Hash example
def simple_hash(key):
    key = str(key)
    char = key[0]
    return ord(char)

xmen_hash = simple_hash("xmen")
things_hash = simple_hash("10thingsihateaboutyou")

## Use the modulo function to improve our hash table
def simple_hash(key):
    key = str(key)
    return ord(key[0]) % 20

xmen_hash = simple_hash("xmen")
things_hash = simple_hash("10thingsihateaboutyou")

## Create the class 'HashTable' using simple hash function
def simple_hash(key):
    key = str(key)
    code = ord(key[0])
    return code % 20

class HashTable():
    
    def __init__(self, size):
        self.array = np.zeros(size, dtype=np.object)
        self.size = size
    
    def __getitem__(self, key):
        ind = simple_hash(key)
        return self.array[ind]
    
    def __setitem__(self, key, value):
        ind = simple_hash(key)
        self.array[ind] = value

hash_table = HashTable(20)

with open("lines/xmen.txt", 'r') as f:
    hash_table["xmen"] = f.read()

## Modifying the class to create a list of values if there is a hash collision
class HashTable():
    
    def __init__(self, size):
        self.array = np.zeros(size, dtype=np.object)
        self.size = size
    
    def __getitem__(self, key):
        ind = simple_hash(key)
        return self.array[ind]
    
    def __setitem__(self, key, value):
        position = simple_hash(key)
        if not isinstance(self.array[position], list):
            self.array[position] = []
        self.array[position].append(value)
        
hash_table = HashTable(20)
with open("lines/xmen.txt", "r") as f:
    line = f.read()
    hash_table["xmen"] = line
    
with open("lines/xmenoriginswolverine.txt", "r") as f:
    line = f.read()
    hash_table["xmen"] = line

## Our HahTable class now stores key and value in the form of tuple
class HashTable():
    
    def __init__(self, size):
        self.array = np.zeros(size, dtype=np.object)
        self.size = size
    
    def __getitem__(self, key):
        position = simple_hash(key)
        for g in self.array[position]:
            if g[0] == key:
                return g[1]
        return None
    
    def __setitem__(self, key, value):
        position = simple_hash(key)
        if not isinstance(self.array[position], list):
            self.array[position] = []
        self.array[position].append((key,value))
        
hash_table = HashTable(20)
with open("lines/xmen.txt","r") as f:
    hash_table["xmen"] = f.read()
with open("lines/xmenoriginswolverine.txt","r") as f:
    hash_table["xmenoriginswolverine"] = f.read()

## Our class now changes the original value of the key if a same key is inserted into the hash
class HashTable():
    
    def __init__(self, size):
        self.array = np.zeros(size, dtype=np.object)
        self.size = size
    
    def __getitem__(self, key):
        ind = simple_hash(key)
        for k,v in self.array[ind]:
            if key == k:
                return v
    
    def __setitem__(self, key, value):
        ind = simple_hash(key)
        if not isinstance(self.array[ind], list):
            self.array[ind] = []
        replace = None
        for i,data in enumerate(self.array[ind]):
            if data[0] == key:
                replace = i
        if replace is None:
            self.array[ind].append((key,value))
        else:
            self.array[ind][replace] = (key, value)

hash_table = HashTable(20)

with open("lines/xmen.txt", 'r') as f:
    hash_table["xmen"] = f.read()

with open("lines/xmenoriginswolverine.txt", 'r') as f:
    hash_table["xmen"] = f.read()


## Profiling our class and checking the time
def profile_table(size):
    start = time.time()
    hash_table = HashTable(size)
    directory = "lines"
    
    for filename in os.listdir(directory):
        name = filename.replace(".txt", "")
        hash_table[name] = quotes[name]
    
    duration = time.time() - start
    return duration
lengths = [1,10,20,30,40,50]
times = []
for l in lengths:
    times.append(profile_table(l))
