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
NUM_THREADS = 500
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
        for a in starts[aStart]:
            aMid = a[2]
            aEnd = a[4]
            for x in starts.get(aStart, []):
                for y in starts.get(aMid, []):
                    for z in starts.get(aEnd, []):
                        q.put((a, x, y, z))
                        with total_lock:
                            total_iters += 1
    print(f"PRODUCER FINISHED WITH {total_iters:,}")


def worker():
    global counter
    while True:
        try:
            item = q.get(timeout=1)
            a, x, y, z = item
        except queue.Empty:
            break
        else:
            xMid = x[2]
            yMid = y[2]
            zMid = z[2]
            xEnd = x[4]
            yEnd = y[4]
            zEnd = z[4]

            bCands = set(starts.get(xMid, [])).intersection(mids.get(yMid, [])).intersection(ends.get(zMid, []))
            if len(bCands) == 0:
                continue
            cCands = set(starts.get(xEnd, [])).intersection(set(mids.get(yEnd, []))).intersection(set(ends.get(zEnd, [])))
            if len(cCands) == 0:
                continue

            with counter_lock:
                inc = len(bCands) * len(cCands)
                counter += inc


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
    print(f"Found per sec: {(cafter - cbefore):,}\n")


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

