import json
import pandas as pd
import threading
import queue
from itertools import permutations
import time


RANK_CUTOFF = 8_000
# RANK_CUTOFF = 1_000
# RANK_CUTOFF = 100
# RANK_CUTOFF = 15
TOTAL = RANK_CUTOFF ** 6
NUM_THREADS = 100
SUCC_CUTOFF = 8

print(f"WORDSET SIZE: {RANK_CUTOFF}")
print(f"TOTAL: {TOTAL:,}")


with open('frequencies.json') as file:
    data = json.load(file)
    cleaned = {key:val for key, val in data.items() if "'" not in key}

df = pd.DataFrame({
    "word": cleaned.keys(),
    "freq": cleaned.values()
})

df = df.sort_values(by='freq', ascending=False)
selection = df.head(RANK_CUTOFF)
wordset = set(selection['word'])


starts = {}
mids = {}
ends = {}
for word in wordset:
    starts[word[0]] = starts.get(word[0], []) + [word]
    mids[word[2]] = mids.get(word[2], []) + [word]
    ends[word[4]] = ends.get(word[4], []) + [word]


def printMat(a, b, c, x, y, z):
    print(a)
    print(b)
    print(c)
    print()
    print(x)
    print(y)
    print(z)
    print()
    print(' '.join(a))
    print(f'{x[1]}   {y[1]}   {z[1]}')
    print(' '.join(b))
    print(f'{x[3]}   {y[3]}   {z[3]}')
    print(' '.join(c))


counter_lock = threading.Lock()
counter = 0

total_lock = threading.Lock()
total_iters = 0

print_lock = threading.Lock()

q = queue.Queue()

def producer():
    global total_iters

    for aStart in starts:
        for aMid in mids:
            for aEnd in ends:
                for bStart in starts:
                    for bMid in mids:
                        for bEnd in ends:
                            for cStart in starts:
                                for cMid in mids:
                                    for cEnd in ends:
                                        item = (aStart, aMid, aEnd, bStart, bMid, bEnd, cStart, cMid, cEnd) 
                                        q.put(item)
                                        with total_lock:
                                            total_iters += 1


def worker():
    global counter
    while True:
        try:
            item = q.get(timeout=1)
            (aStart, aMid, aEnd, bStart, bMid, bEnd, cStart, cMid, cEnd) = item
        except queue.Empty:
            break
        else:
            aCands = set(starts.get(aStart, [])).intersection(set(mids.get(aMid, []))).intersection(set(ends.get(aEnd, [])))
            if len(aCands) == 0:
                continue
            bCands = set(starts.get(bStart, [])).intersection(set(mids.get(bMid, []))).intersection(set(ends.get(bEnd, [])))
            if len(bCands) == 0:
                continue
            cCands = set(starts.get(cStart, [])).intersection(set(mids.get(cMid, []))).intersection(set(ends.get(cEnd, [])))
            if len(cCands) == 0:
                continue
            xCands = set(starts.get(aStart, [])).intersection(set(mids.get(bStart, []))).intersection(set(ends.get(cStart, [])))
            if len(xCands) == 0:
                continue
            yCands = set(starts.get(aMid, [])).intersection(set(mids.get(bMid, []))).intersection(set(ends.get(cMid, [])))
            if len(yCands) == 0:
                continue
            zCands = set(starts.get(aEnd, [])).intersection(set(mids.get(bEnd, []))).intersection(set(ends.get(cEnd)))
            if len(zCands) == 0:
                continue

            prod = len(aCands) * len(bCands) * len(cCands) * len(xCands) * len(yCands) * len(zCands)
            with counter_lock:
                counter += prod



# Start threads
threads = []
for _ in range(NUM_THREADS):
    thread = threading.Thread(target=worker)
    thread.start()
    threads.append(thread)

start = time.time()
producer_thread = threading.Thread(target=producer)
producer_thread.start()


# Trackers
succ_empty = 0
while True:
    if q.qsize() == 0:
        succ_empty += 1
        if succ_empty >= SUCC_CUTOFF:
            print(f'BREAKING because of {SUCC_CUTOFF}th successive empty q')
            break
    else:
        succ_empty = 0

    with total_lock:
        tbefore = total_iters
    with counter_lock:
        cbefore = counter
    time.sleep(1)
    with total_lock:
        tafter = total_iters
    with counter_lock:
        cafter = counter

    print(f"Q size  : {q.qsize():,}")
    print(f"C before: {cbefore:,}")
    print(f"C after : {cafter:,}")
    print(f"T before: {tbefore:,}")
    print(f"T after : {tafter:,}")
    print(f"Pairs generated: {(tafter - tbefore):,}")
    print(f"Found per sec: {(cafter - cbefore):,}")
    print(f"Full runtime: {time.time() - start:.2f}\n")


# Closing
for thread in threads:
    thread.join()

print('\n' + '=' * 20)
print('  FINISHED')
print('=' * 20 + '\n')

print(f'Final count: {counter:,}')
print(f'Out of {TOTAL:,} pairs {counter / TOTAL:.4f} ({counter / TOTAL})')
print(f'Producer built {total_iters:,} pairs')
print(f'Finished in {time.time() - start:.3f} seconds')

