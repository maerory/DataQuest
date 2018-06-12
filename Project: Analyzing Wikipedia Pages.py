import os

import re

import time

import concurrent.futures

from bs4 import BeautifulSoup

from collections import Counter



filenames = os.listdir("wiki")

len(filenames)

print(filenames[0])



with open("wiki/" + filenames[0]) as f:

    print(f.read())

    

content = []

for filename in os.listdir("wiki"):

    with open("wiki/"+filename) as f:

        content.append(f.read())

print(len(content))

article = []

for filename in os.listdir("wiki"):

    article.append(filename[:-5])

print(len(article))



def parse_html(html):

    soup = BeautifulSoup(html,'html.parser')

    return str(soup.find_all("div", id="content")[0])



start = time.time()

pool = concurrent.futures.ProcessPoolExecutor(max_workers=5)

parsed = pool.map(parse_html, content)

parsed = list(parsed)

end = time.time()

print(end - start)



find most used tags

def count_tags(html):

    soup = BeautifulSoup(html, 'html.parser')

    tags = {}

    for tag in soup.find_all():

        if tag not in tags:

            tags[tag.name] = 0

        tags[tag.name] += 1

    return tags

start = time.time()

pool = concurrent.futures.ProcessPoolExecutor(max_workers=5)

tags = pool.map(count_tags, parsed)

tags = list(tags)

tag_counts = {}

for tag in tags:

    for k,v in tag.items():

        if k not in tag_counts:

            tag_counts[k] = 0

        tag_counts[k] += v

end = time.time()

print(end - start)

            

Find words with most occurences

def count_words(html):

    soup = BeautifulSoup(html, 'html.parser')

    words = {}

    text = soup.get_text()

    text = re.sub("\W+", " ", text.lower())

    words = text.split(" ")

    words = [word for word in words if len(word) >= 5]

    return Counter(words).most_common(10)

start = time.time()

pool = concurrent.futures.ProcessPoolExecutor(max_workers=5)

words = pool.map(count_words, content)

words = list(words)

word_count = {}

for wc in words:

    for word, count in wc:

        if word not in word_count:

            word_count[word]= 0

        word_count[word] += count

end = time.time()

print(end - start)

    
