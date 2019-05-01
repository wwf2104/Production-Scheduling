# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 13:59:25 2019

@author: Winnie
"""
"""
ALL IMPORTS
"""
import pandas as pd
from numpy.random import exponential as exp, random as rand
import datetime as dt
from statistics import mean
import copy

"""
CONSTANT VALUES/PROBABILITIES/BUCKETS ETC.
"""
# Upper bound of probability buckets for crime types
CRIMES = ['viol', 'misd', 'fel']
CRIMES_PROBS = [0.1033, 0.6753, 1.0]

# Upper bound of probability buckets for crime occuring in precinct
PRECINCTS = [1, 5, 6, 7, 9, 10, 13, 17, 19, 20, 23, 24, 25, 26, 28, 30, 32, 33, 34]
PREC_PROBS = [0.0855661712265676, 0.12675717694993233, 0.18877193780491172,
              0.22425582449431156, 0.2776847259429418, 0.32845095500424826,
              0.4071208209933604, 0.4426160796819739, 0.5236935603617595,
              0.5730155455229251, 0.6089445647529758, 0.6624855616224268,
              0.715115173983465, 0.7451340027650203, 0.7922628166492566,
              0.8460296289316656, 0.8968315985619294, 0.9398453733021198, 1.0]

# Hourly rate, rounded to nearest int
LAM = 150
# Police officers needed for crime type
NEEDED = dict(zip(CRIMES, [4,4,4]))
#NEEDED = dict(zip(CRIMES, [2,4,6]))

# Avg processing times for different types of crimes
# Any extreme values (processing time > 2 days) are removed beforehand
# Rounded to nearest minute
PJ_VIOL = dt.timedelta(minutes=45)
PJ_MISD = dt.timedelta(hours=1, minutes=18)
PJ_FEL = dt.timedelta(hours=1, minutes=53)
CRIME_PJ = dict(zip(CRIMES,[PJ_VIOL,PJ_MISD,PJ_FEL]))

SHIFT_BUCKETS = [dt.timedelta(hours=8),
          dt.timedelta(hours=16),
          dt.timedelta(days=1)]

# Based off of Rule of 60 article, NYC uses 2/3 of TOTAL_ACTIVE for patrol
TOTAL_ACTIVE = 7845
PATROL = int(TOTAL_ACTIVE * 2 / 3)
EQUAL_DISTR = [int(PATROL/3/len(PRECINCTS))] * len(PRECINCTS)
"""
ANY NECESSARY FUNCTIONS TO RUN SIMULATION
"""
# get first key for allocation dictionary
def get_shift(time):
    return [dt.timedelta(seconds=time.seconds) < shift for shift in SHIFT_BUCKETS].index(True)
# law_cat_cd
def crime_type():
    crime = rand()
    find = [crime < p for p in CRIMES_PROBS].index(True)
    return CRIMES[find - 1]
# precinct
def prec_type():
    prec = rand()
    find = [prec < p for p in PREC_PROBS].index(True)
    return PRECINCTS[find - 1]
# Release Dates of Crimes
def arrival():
    time = dt.timedelta(hours = exp(1/LAM)*60)
    crime = crime_type()
    return (NEEDED[crime], time, prec_type(), CRIME_PJ[crime])
# Check if there are enough officers for dispatch
def dispatch(alloc, queue, out, time, max_avail, max_out):
    if alloc > queue[0]['ppl']:
        rep = queue.pop(0)
        out.append((rep['ppl'], time + rep['pj']))
        alloc -= rep['ppl']
        return alloc, queue, out, max(sum(o[0] for o in out)/max_avail, max_out)
    else:
        return False
"""
SIMULATION CODE
Simulate simTime days (include jobs with rj within the 24 hour period)
"""

def runSim(simTime, alloc):
    orig = copy.deepcopy(alloc)
    # Generate interarrival times for the 24 hour period
    times = []
    time = dt.timedelta(days=0)
    while time < dt.timedelta(days=simTime):
        new_arrival = arrival()
        time += new_arrival[1]
        times.append(new_arrival)
    # Group times by precinct
    # crimes_prec is a dict of precinct:crimes
    crimes_prec = pd.DataFrame.from_records(times, columns=['ppl','rj','prec','pj'])

    distr = crimes_prec.groupby('prec')
    distr = distr.size().apply(lambda x: x/distr.size().sum()*100)
    print('Total reports generated:', len(times))
    print('Distribution of Reports (%)')
    print(distr)

    grouped = crimes_prec.groupby('prec').groups
    crimes_prec = {prec:crimes_prec.iloc[reports] for prec,reports in grouped.items()}
    # uj is the number of times a police officer could not immediately respond
    # Max +1 per report
    uj = 0
    workloads = []
    queues = []
    # for each precinct
    for prec in crimes_prec:
        #print('Starting Precinct ',prec)
        time = dt.timedelta(days=0)
        shift = 0
        # out holds the number of officers sent and times that they returns
        # out = (people out, time of return)
        out = []
        inQueue = []
        max_avail = orig[shift][prec]
        max_out = 0
        max_queue = 0
        for idx in crimes_prec[prec].index:
            report = crimes_prec[prec].loc[idx]
            time += report['rj']
            new_shift = get_shift(time)
            # if new shift begins, restart out queue
            if shift!=new_shift:
                alloc = orig
                shift = new_shift
                out.clear()
                max_avail = orig[shift][prec]
            else: # return officers to precinct
                while len(out) > 0:
                    if out[0][1] < time:
                        alloc[shift][prec] += out[0][0]
                        out.pop(0)
                    else:
                        break
            # Check if any reports are queued: FIFO
            if len(inQueue) > 0:
                # Assume
                while len(inQueue) > 0:
                    check = dispatch(alloc[shift][prec], inQueue, out, time, max_avail, max_out)
                    if check:
                        alloc[shift][prec], inQueue, out, max_out = check
                    # Not enough available officers to finish queue
                    else:
                        inQueue.append(report)
                        uj += 1
                        max_queue = max(len(inQueue), max_queue)
                        break
            # Queue is (now) empty, check if we can dispatch current report
            if len(inQueue) == 0:
                check = dispatch(alloc[shift][prec], [report], out, time, max_avail, max_out)
                if check:
                    alloc[shift][prec], out, max_out = check[0],check[2],check[3]
                else:
                    inQueue.append(report)
                    uj += 1
                    max_queue = max(len(inQueue), max_queue)

        workloads.append(max_out)
        queues.append(max_queue)

    # Based on rule of 60
    perform = pd.DataFrame.from_records(
            list(zip(workloads,queues)),
            index = crimes_prec.keys(), columns=['maxWorkloads', 'maxQueue'])
    perform.index.name = 'Precinct'
    perform['above60'] = perform['maxWorkloads'].apply(lambda x: x>0.6)
    print('Precinct Performance')
    print(perform)
    print('Average maxWorkloads =', perform['maxWorkloads'].mean())
    print('Average maxQueue =', perform['maxQueue'].mean())
    print('Uj =',uj)

    return distr, perform, uj/len(times)*100

"""
RUN CODE

allocations should be a dictionary with format:
    {shift1: {prec_num: officers allocated,
             prec_num: officers allocated,
             etc.},
    shift2: ...,
    shift3: ...}
"""
#d, p, u = runSim(30, baseline)
#w.to_csv('MaxWorkloads_30days_444.csv', index=False)


def main(loops, days=180, alloc=None):
    if not alloc:
        alloc = {0: dict(zip(PRECINCTS, EQUAL_DISTR)),
                 1: dict(zip(PRECINCTS, EQUAL_DISTR)),
                 2: dict(zip(PRECINCTS, EQUAL_DISTR))}
    avg_avgw = []
    avg_q = []
    avg_u = []
    for i in range(loops):
        d, p, u = runSim(days, alloc)
        avg_avgw.append(p['maxWorkloads'].mean())
        avg_q.append(p['maxQueue'].mean())
        avg_u.append(u)

    performance = {}
    performance['best'] = {'avg_w' : min(avg_avgw),
               'q' : min(avg_q),
               'u' : min(avg_u)}
    performance['avg'] = {'avg_w' : mean(avg_avgw),
               'q' : mean(avg_q),
               'u' : mean(avg_u)}
    performance['worst'] = {'avg_w' : max(avg_avgw),
               'q' : max(avg_q),
               'u' : max(avg_u)}

    return performance

p = main(50, 180)




