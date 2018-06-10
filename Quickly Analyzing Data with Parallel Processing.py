import concurrent.futures
import math
from collections import Counter
import os
import re


## We use concurrent.futures, a new module to create workers for our task
numbers = [1,10,20,50]
pool = concurrent.futures.ThreadPoolExecutor(max_workers=5) # creates 5 threads to execute our task
roots = list(pool.map(math.sqrt, numbers))


## create a function that calculates total number of line 
pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)

def file_length(file):
    with open(file) as f:
        counter = 0
        for line in f:
            counter += 1
    return counter
# must format the line in order to execute with listdir function
filenames = ['lines/{}'.format(f) for f in os.listdir("lines")] # os.listdir function finds all the file name in given path
lengths = pool.map(file_length, filenames)
lengths = list(lengths)

movie_lengths = {}
for i in range(len(lengths)):
    # save the file name after removing the directory in front
    movie_lengths[filenames[i].replace("lines/", "")] = lengths[i] 
# by submitting key as a lamdba, the max function uses the movie length to find the maximum value and return the key associated with it
most_lines = max(movie_lengths.keys(), key=(lambda k: movie_lengths[k]))


## finding the longest line in the moviews using thread
def longest_line(filename):
    with open(filename) as f:
        max_length = 0
        longest_line = ""
        for line in f:
            line_length = len(line)
            if line_length > max_length:
                max_length = line_length
                longest_line = line
    # stores both max length, and longest line together
    return max_length, longest_line

results = []
pool = concurrent.futures.ThreadPoolExecutor(max_workers=5)
filenames = ["lines/{}".format(f) for f in os.listdir("lines")]
lengths = pool.map(longest_line, filenames)
lengths = list(lengths) # Must convert to list at the end since pool returns a generator

line_lengths = {}
for i in range(len(lengths)):
    line_lengths[filenames[i].replace("lines/", "")] = lengths[i]

longest_line_movie = max(line_lengths.keys(), key=(lambda k: line_lengths[k][0]))
# return the longest line accessed by [1]
longest_line = line_lengths[longest_line_movie][1]


## Using ProccessPool Executor to find the most common word in given set of text files
def most_common_words(filename):
    with open(filename) as f:
        words = f.read().split(" ")
    count = Counter(words)
    # Counter object's most_common function returns the common words in order with their number of appearances
    return count.most_common()[0][0]

pool = concurrent.futures.ProcessPoolExecutor(max_workers=2)
filenames = ["lines/{}".format(f) for f in os.listdir("lines")]
words = pool.map(most_common_words, filenames)
words = list(words)

common_words = {}
for i in range(len(filenames)): # length is the list of 
    common_words[filenames[i].replace("lines/","")] = words[i]


## We filter out punctuations and words that are shorter than length of 5
def most_common_word(filename):
    # Fill in the function here
    with open(filename) as f:
        data = f.read().lower()
    # the sub function of 're' substitutes certain words in the data which  matches the given regular expression. W of "\W+" means any non-alphabetical letter
    data = re.sub("\W+", " ", data)
    words = data.split(" ")
    words = [w for w in words if len(w) >= 5]
    count = Counter(words)
    return count.most_common()[0][0]

start = time.time()
pool = concurrent.futures.ProcessPoolExecutor(max_workers=5)
filenames = ["lines/{}".format(f) for f in os.listdir("lines")]
words = pool.map(most_common_word, filenames)
words = list(words)

common_words = {}
for i in range(len(lengths)):
    common_words[filenames[i].replace("lines/", "")] = words[i]
print(common_words)


## Finding top 200 words across all of the movies   

def word_frequencies(filename):
    with open(filename) as f:
        data = f.read().lower()
    data = re.sub("\W+", " ", data)
    words = data.split(" ")
    words = [word for word in words if len(word) >= 5]
    count = Counter(words)
    return dict(count)
    
pool = concurrent.futures.ProcessPoolExecutor(max_workers=2)
filenames = ["lines/{}".format(f) for f in os.listdir("lines")]
word_counts = pool.map(word_frequencies, filenames)
word_counts = list(word_counts)

total_word_counts = {}

# Iterate through each movies, each words per movie
for wc in word_counts:
    for k,v in wc.items():
        if k not in total_word_counts:
            total_word_counts[k] = 0
        total_word_counts[k] += v
# Use counter again to find most common words
top_200 = Counter(total_word_counts).most_common(200)



