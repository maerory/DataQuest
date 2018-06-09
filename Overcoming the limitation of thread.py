## CPU Bound Programs

import threading
import time
import statistics
import pandas
import multiprocessing
from multiprocessing import Pool

## Comparing the two process of using original function and thread

def read_data():
    with open("Emails.csv") as f:
        data = f.read()

times = []
for i in range(100):
    start = time.time()
    read_data()
    times.append(time.time()-start)
    
threaded_times = []
for i in range(100):
    start = time.time()
    t1 = threading.Thread(target=read_data)
    t2 = threading.Thread(target=read_data)
    t1.start()
    t2.start()
    for thread in [t1,t2]:
        thread.join()
    end = time.time()
    threaded_times.append(end - start)
print(statistics.median(times))
print(statistics.median(threaded_times))

# counting the number of capital letters and measuring the time it took
emails = pandas.read_csv("Emails.csv")
capital_letters = []
start = time.time()
for email in emails["RawText"]:
    num_capital = sum([x.isupper() for x in email])
    capital_letters.append(num_capital)
total = time.time() - start

print(total)

# Usng two threads to do the same thing
capital_letters1 = []
capital_letters2 = []
start = time.time()
def count_capital_letters(email):
    return len([letter for letter in email if letter.isupper()])
def count_capitals_in_emails(start, finish, capital_letters):
    for email in emails["RawText"][start:finish]:
        capital_letters.append(count_capital_letters(email))

t1 = threading.Thread(target=count_capitals_in_emails, args=(0, 3972, capital_letters1))
t2 = threading.Thread(target=count_capitals_in_emails, args=(3972, 7946, capital_letters2))
t1.start()
t2.start()

for thread in [t1, t2]:
    thread.join()
total = time.time() - start

print(total)
# Thread can have overhead associated with each thread, thus decreasing actual performance
# Threads are not usually good in CPU-Bound task or very quick tasks.

# Thread run inside processes, each process has its own memory and all the threads inside share the same memory
# Heavier operation takes longer time, threads are much faster to make inside those operations.


## CPU Bound tasks are easier with multiprocessing library. Multiprocessing is very similar to threading, it forks the method with a child processer and allows it to work on the same task.

capital_letters1 = []
capital_letters2 = []

start = time.time()
def count_capital_letters(email):
    return len([letter for letter in email if letter.isupper()])

def count_capitals_in_emails(start, finish, capital_letters):
    for email in emails["RawText"][start:finish]:
        capital_letters.append(count_capital_letters(email))

p1 = multiprocessing.Process(target=count_capitals_in_emails, args=(0, 3972, capital_letters1))
p2 = multiprocessing.Process(target=count_capitals_in_emails, args=(3972, 7946, capital_letters2))
p1.start()
p2.start()

for process in [p1, p2]:
    process.join()
total = time.time() - start
print(total)

# Multiprocessing allows the task to run in separate CPUs since it can sidestep from GIL of python. However, they don't share memory so you cannot modify values in the process and change the main program thread memory.

# Using four multiprocessing tech. The decrease in time is not really proportional to number of processors.
start = time.time()
def count_capital_letters(email):
    return len([letter for letter in email if letter.isupper()])

def count_capitals_in_emails(start, finish):
    for email in emails["RawText"][start:finish]:
        capital_letters.append(count_capital_letters(email))

p1 = multiprocessing.Process(target=count_capitals_in_emails, args=(0, 1986,))
p2 = multiprocessing.Process(target=count_capitals_in_emails, args=(1986, 3972, ))
p3 = multiprocessing.Process(target=count_capitals_in_emails, args=(3972, 5958, ))
p4 = multiprocessing.Process(target=count_capitals_in_emails, args=(5958, 7946, ))

p1.start()
p2.start()
p3.start()
p4.start()

for process in [p1, p2, p3, p4]:
    process.join()
total = time.time() - start
print(total)

# Using pipe to actually transform the data with multiprocessing
parent_conn1, child_conn1 = multiprocessing.Pipe()
parent_conn2, child_conn2 = multiprocessing.Pipe()

def count_capital_letters(email):
    return len([letter for letter in email if letter.isupper()])

def count_capitals_in_emails(start, finish, conn):
    capital_letters= []
    for email in emails["RawText"][start:finish]:
        capital_letters.append(count_capital_letters(email))
    conn.send(capital_letters)
    conn.close()
        
start = time.time()
p1 = multiprocessing.Process(target=count_capitals_in_emails, args=(0, 3972, child_conn1))
p2 = multiprocessing.Process(target=count_capitals_in_emails, args=(3972, 7946, child_conn2))
p1.start()
p2.start()

capital_letters1 = parent_conn1.recv()
capital_letters2 = parent_conn2.recv()
                             
for process in [p1, p2]:
    process.join()
total = time.time() - start
print(total)
print(capital_letters1)


# Using Pool automatically creates multiprocessors to process
def count_capital_letters(email):
    return len([x for x in email if x.isupper()])        
    
start = time.time()
capital_letters = p.map(count_capital_letters, emails["RawText"])
total = time.time() - start
print(total)

# Remember that threads and proccesses both can have deadlock


