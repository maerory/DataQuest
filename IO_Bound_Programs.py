## Improving I/O Performance

# I/O bound refers to a condition in which the time it takes to complete a computation is 
# determined principally by the period spend waiting for input/output operations

import cProfile
import sqlite3

query = "SELECT DISTINCT teamID from Teams inner join TeamsFranchises on Teams.franchID == TeamsFranchises.franchID where TeamsFranchises.active = 'Y';"
conn = sqlite3.connect("lahman2015.sqlite")

cur = conn.cursor()
# Fetchall returns the rows in form of list, so we must select the first element of each row even though we are only getting one value per row
teams = [row[0] for row in cur.execute(query).fetchall()]
query = "SELECT SUM(HR) FROM Batting WHERE teamId=?"
#Define a method that calculated home runs per team
def calculate_runs(teams): 
    home_runs = []
    for team in teams:
        runs = cur.execute(query, [team]).fetchall() # Must send the parameters in a form of list
        runs = runs[0][0]
        home_runs.append(runs)
    return home_runs
# Profile string is the line of code that you want to time with cProfile
profile_string = "home_runs = calculate_runs(teams)"
cProfile.run(profile_string)


# Using memory to improve speed
memory = sqlite3.connect(':memory:') # create a memory database
disk = sqlite3.connect('lahman2015.sqlite')
dump = "".join([line for line in disk.iterdump() if "Batting" in line]) # we dump needed data into our local created memory
memory.executescript(dump)
cur = memory.cursor()

# We use the same function of "calculate_runs" as above
profile_string = "home_runs = calculate_runs(teams)"
cProfile.run(profile_string)

# Now we use threading to solve the same problem, threading creates multiple instances that works on a problem to divide our task
import threading

def task(team):
    print(team)
for k, team in enumerate(teams):
    thread = threading.Thread(target=task, args=(team,))
    thread.start()
    print("Started task {}".format(k))
print(teams)

# However, due to threading orders being mixed up during processes, we might have to set some time in between to view the correct orders of our task
def task(team):
    print(team)
    time.sleep(3)
for i, team in enumerate(teams):
    thread = threading.Thread(target=task, args=(team,))
    thread.start()
    print("Started task {}".format(i))
print(teams)


# join in thread connects multiple threads together and make each thread wait for the other to finish
def task(team):
    print(team)
for i in range(11):
    team_names = teams[i*5:(i+1)*5] #Filter out oly the part we need
    threads = []
    for team in team_names:
        thread = threading.Thread(target=task, args=(team,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    print("Finished batch {}".format(i))
print(teams)


# Thread locking allows us to ensure that only one thread is accessing a shraed resources at the smae time. Threads must acquire locks and release them in order to process given tasks

import threading
import time
import sys

lock = threading.Lock()
def task(team):
    lock.acquire()
    print(team)
    sys.stdout.flush()
    lock.release()
for i in range(11):
    team_names = teams[i*5:(i+1)*5]
    threads = []
    for team in team_names:
        thread = threading.Thread(target=task, args=(team,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    print("Finished batch {}".format(i))
print(teams)

# There are processes that are thread safe and not thread safe
# Threadsafe: reading, querying, accessing memory
# Not Threadsafe: modifying data in memory, writing file, adding/modifying data

query = "SELECT DISTINCT teamID from Teams inner join TeamsFranchises on Teams.franchID == TeamsFranchises.franchID where TeamsFranchises.active = 'Y';"
# The check_same_thread keyword should be set to true if we are performing thread safe prcoesses.
conn = sqlite3.connect("lahman2015.sqlite", check_same_thread=False)
cur = conn.cursor()
teams = [row[0] for row in cur.execute(query).fetchall()]

query = "SELECT SUM(HR) FROM Batting WHERE teamId=?"
lock = threading.Lock()

def calculate_runs(team):
    cur = conn.cursor()
    runs = cur.execute(query,[team]).fetchall()[0][0]
    lock.acquire()
    print(team)
    print(runs)
    sys.stdout.flush()
    lock.release()
    return runs

threads = []
for team in teams:
    thread = threading.Thread(target=calculate_runs, args=(team,))
    thread.start()
    threads.append(thread)
for thread in threads:
    thread.join()


## Using thread to perform multiple functions that access the database
conn = sqlite3.connect("lahman2015.sqlite", check_same_thread=False)

best = {}
lock = threading.Lock()

def best_batter():
    cur = conn.cursor()
    query = """
    SELECT 
        ((CAST(H AS FLOAT) + BB + HBP) / (AB + BB + HBP + SF)) + ((H + "2B" + 2*"3B" + 3*HR) / AB) as OBP,  
        playerID
    FROM Batting
    GROUP BY Batting.playerID
    HAVING AB > 100
    ORDER BY OBP desc
    LIMIT 20;
    """
    players = cur.execute(query).fetchall()
    names = [p[1] for p in players]
    best["batter"] = names
    lock.acquire()
    print("Done finding best batters.")
    lock.release()
    
    
def best_pitcher():
    cur = conn.cursor()
    query = """
    SELECT 
        ((13*CAST(HR AS FLOAT) + 3*BB - 2*SO) / IPOuts) + 3.2 as FIP,  
        playerID
    FROM Pitching
    GROUP BY Pitching.playerID
    HAVING IPOuts > 100
    ORDER BY FIP asc
    LIMIT 20;
    """
    players = cur.execute(query).fetchall()
    names = [p[1] for p in players]
    best["pitcher"] = names
    lock.acquire()
    print("Done finding best pitchers.")
    lock.release()

def best_fielder():
    cur = conn.cursor()
    query = """
    SELECT 
        (CAST(A AS FLOAT) + PO) / G as RF,  
        playerID
    FROM Fielding
    GROUP BY Fielding.playerID
    HAVING G > 100
    ORDER BY RF desc
    LIMIT 20;
    """
    players = cur.execute(query).fetchall()
    names = [p[1] for p in players]
    best["fielder"] = names
    lock.acquire()
    print("Done finding best fielders.")
    lock.release()

threads = []
for func in [best_fielder, best_batter, best_pitcher]:
    thread = threading.Thread(target=func)
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

print(best)
    

