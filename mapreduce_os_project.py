#!/usr/bin/env python3
from __future__ import annotations
import os, sys, time, math, random, argparse, statistics
from typing import List, Tuple
from queue import Queue
from threading import Thread, Lock

MEM_AVAILABLE = True
try:
    import resource
except Exception:
    MEM_AVAILABLE = False

def rss_mb() -> float:
    if not MEM_AVAILABLE:
        return -1.0
    ru = resource.getrusage(resource.RUSAGE_SELF)
    kb = ru.ru_maxrss
    if kb > 10_000_000:
        return kb / (1024**2)
    else:
        return kb / 1024.0


def make_data(n: int, seed: int = 42) -> List[int]:
    random.seed(seed)
    return [random.randint(-10**9, 10**9) for _ in range(n)]

def split_chunks(a: List[int], k: int) -> List[List[int]]:
    n = len(a)
    if k <= 0:
        return [a[:]]
    size = math.ceil(n / k)
    return [a[i:i+size] for i in range(0, n, size)]

# Merge Sort
def merge(left: List[int], right: List[int]) -> List[int]:
    out, i, j = [], 0, 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            out.append(left[i]); i += 1
        else:
            out.append(right[j]); j += 1
    if i < len(left): out.extend(left[i:])
    if j < len(right): out.extend(right[j:])
    return out

def mergesort(a: List[int]) -> List[int]:
    if len(a) <= 32:
        for i in range(1, len(a)):
            x, j = a[i], i - 1
            while j >= 0 and a[j] > x:
                a[j+1] = a[j]; j -= 1
            a[j+1] = x
        return a
    mid = len(a) // 2
    return merge(mergesort(a[:mid]), mergesort(a[mid:]))

# Part 1: Parallel Sorting
def reducer_merge(sorted_chunks: List[List[int]]) -> List[int]:
    curr = sorted_chunks
    while len(curr) > 1:
        nxt = []
        for i in range(0, len(curr), 2):
            if i + 1 < len(curr):
                nxt.append(merge(curr[i], curr[i+1]))
            else:
                nxt.append(curr[i])
        curr = nxt
    return curr[0] if curr else []

def sort_threaded(arr: List[int], workers: int) -> Tuple[List[int], float, float]:
    chunks = split_chunks(arr, workers)
    q: Queue[Tuple[int, List[int]]] = Queue()
    t0_mem = rss_mb()
    t0 = time.perf_counter()

    def mapper(idx: int, data: List[int]):
        q.put((idx, mergesort(data)))

    threads = [Thread(target=mapper, args=(i, c)) for i, c in enumerate(chunks)]
    for t in threads: t.start()
    for t in threads: t.join()

    # gather in order
    sorted_chunks = [None] * len(chunks)
    while not q.empty():
        i, chunk = q.get()
        sorted_chunks[i] = chunk

    out = reducer_merge(sorted_chunks)
    t1 = time.perf_counter()
    t1_mem = rss_mb()
    return out, (t1 - t0), (t1_mem - t0_mem if t1_mem >= 0 and t0_mem >= 0 else -1)

#top level helpers for multiprocessing
def _proc_sort_mapper(idx: int, data, q):
    q.put((idx, mergesort(data)))

def _proc_max_mapper(data, shared_max, lock):
    local_m = max(data) if data else -10**10
    # read -> compare -> conditional write under synchronization
    with lock:
        if local_m > shared_max.value:
            shared_max.value = local_m

def sort_process(arr: List[int], workers: int) -> Tuple[List[int], float, float]:
    import multiprocessing as mp
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        pass

    ctx = mp.get_context("spawn")
    chunks = split_chunks(arr, workers)
    q: mp.SimpleQueue = ctx.SimpleQueue()
    t0_mem = rss_mb()
    t0 = time.perf_counter()

    procs = [ctx.Process(target=_proc_sort_mapper, args=(i, c, q)) for i, c in enumerate(chunks)]
    for p in procs:
        p.start()

    sorted_chunks = [None] * len(chunks)
    for _ in range(len(chunks)):
        i, chunk = q.get()  # this unblocks children as we consume
        sorted_chunks[i] = chunk

    for p in procs:
        p.join()

    out = reducer_merge(sorted_chunks)
    t1 = time.perf_counter()
    t1_mem = rss_mb()
    return out, (t1 - t0), (t1_mem - t0_mem if t1_mem >= 0 and t0_mem >= 0 else -1)

# Part 2: Max with ONE-INT shared memory
def max_threaded(arr: List[int], workers: int) -> Tuple[int, float]:
    chunks = split_chunks(arr, workers)
    global_max = [-10**10]
    lock = Lock()

    t0 = time.perf_counter()

    def mapper_local_max(data: List[int]):
        local_m = max(data) if data else -10**10
        # critical section
        with lock:
            if local_m > global_max[0]:
                global_max[0] = local_m

    threads = [Thread(target=mapper_local_max, args=(c,)) for c in chunks]
    for t in threads: t.start()
    for t in threads: t.join()

    t1 = time.perf_counter()
    return global_max[0], (t1 - t0)

def max_process(arr: List[int], workers: int) -> Tuple[int, float]:
    from multiprocessing import Process, Value, Lock, set_start_method
    try:
        set_start_method("spawn")
    except RuntimeError:
        pass

    shared_max = Value('q', -10**10, lock=False)
    lock = Lock()

    chunks = split_chunks(arr, workers)
    t0 = time.perf_counter()

    procs = [Process(target=_proc_max_mapper, args=(c, shared_max, lock)) for c in chunks]
    for p in procs: p.start()
    for p in procs: p.join()

    t1 = time.perf_counter()
    return int(shared_max.value), (t1 - t0)

def is_sorted(a: List[int]) -> bool:
    return all(a[i] <= a[i+1] for i in range(len(a)-1))


def run_bench():
    sizes = [32, 131072]
    worker_sets = [1, 2, 4, 8]
    modes = [
        ("sort-thread", lambda a,w: sort_threaded(a,w)),
        ("sort-proc",   lambda a,w: sort_process(a,w)),
        ("max-thread",  lambda a,w: max_threaded(a,w)),
        ("max-proc",    lambda a,w: max_process(a,w)),
    ]

    print("mode,size,workers,time_sec,mem_delta_mb,extra")
    for n in sizes:
        data = make_data(n, seed=123)
        golden_sorted = sorted(data)
        golden_max = max(data) if data else None
        for workers in worker_sets:
            for name, fn in modes:
                # run 3 trials and take median time
                times, mems = [], []
                out_extra = ""
                for _ in range(3):
                    if name.startswith("sort"):
                        out, t, m = fn(data[:], workers)
                        assert is_sorted(out), f"{name} produced unsorted output"
                        assert out == golden_sorted, f"{name} mismatch vs golden sort"
                        times.append(t); mems.append(m)
                    else:
                        g, t = fn(data[:], workers)
                        assert g == golden_max, f"{name} max mismatch"
                        times.append(t); mems.append(-1.0)
                        out_extra = f"max={g}"
                tmed = statistics.median(times)
                mmed = statistics.median([x for x in mems if x >= 0]) if any(x>=0 for x in mems) else -1.0
                print(f"{name},{n},{workers},{tmed:.6f},{mmed:.3f},{out_extra}")

# CLI
def main():
    ap = argparse.ArgumentParser(description="MapReduce-style OS project (threads & processes)")
    ap.add_argument("mode", choices=["sort-thread","sort-proc","max-thread","max-proc","bench"])
    ap.add_argument("--size", type=int, default=131072, help="array length (default: 131072)")
    ap.add_argument("--workers", type=int, default=4, help="number of mapper workers (default: 4)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    if args.mode == "bench":
        run_bench()
        return

    data = make_data(args.size, args.seed)

    if args.mode == "sort-thread":
        out, t, m = sort_threaded(data, args.workers)
        print(f"time={t:.6f}s, mem_delta_mb={m:.3f}, ok_sorted={is_sorted(out)}")
    elif args.mode == "sort-proc":
        out, t, m = sort_process(data, args.workers)
        print(f"time={t:.6f}s, mem_delta_mb={m:.3f}, ok_sorted={is_sorted(out)}")
    elif args.mode == "max-thread":
        g, t = max_threaded(data, args.workers)
        print(f"time={t:.6f}s, global_max={g}")
    elif args.mode == "max-proc":
        g, t = max_process(data, args.workers)
        print(f"time={t:.6f}s, global_max={g}")

if __name__ == "__main__":
    main()
