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

# mergesort
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

    # gather
    sorted_chunks = [None] * len(chunks)
    while not q.empty():
        i, chunk = q.get()
        sorted_chunks[i] = chunk

    out = reducer_merge(sorted_chunks)
    t1 = time.perf_counter()
    t1_mem = rss_mb()
    return out, (t1 - t0), (t1_mem - t0_mem if t1_mem >= 0 and t0_mem >= 0 else -1)

# mp helpers
def _proc_sort_mapper(idx: int, data, q):
    q.put((idx, mergesort(data)))

def _proc_max_mapper(data, shared_max, lock):
    local_m = max(data) if data else -10**10
    # read/compare/write
    with lock:
        if local_m > shared_max.value:
            shared_max.value = local_m

# unsafe mapper
def _proc_max_mapper_unsafe(data, shared_max):
    local_m = max(data) if data else -10**10
    # race-prone
    if local_m > shared_max.value:
        shared_max.value = local_m

# unsafe stress mapper
def _proc_stress_mapper_unsafe(d, shared_max, jitter_prob: float, jitter_max_ms: float):
    import random, time as _time
    local_m = max(d) if d else -10**10
    if random.random() < jitter_prob:
        _time.sleep(random.uniform(0.0, jitter_max_ms) / 1000.0)
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
        i, chunk = q.get()  # drain queue
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

def max_threaded_unsafe(arr, workers):
    from threading import Thread
    chunks = split_chunks(arr, workers)
    global_max = [-10**10]  # no lock
    t0 = time.perf_counter()
    def mapper(data):
        local_m = max(data) if data else -10**10
        # race-prone
        if local_m > global_max[0]:
            global_max[0] = local_m
    threads = [Thread(target=mapper, args=(c,)) for c in chunks]
    for t in threads: t.start()
    for t in threads: t.join()
    t1 = time.perf_counter()
    return global_max[0], (t1 - t0)

def max_process_unsafe(arr, workers):
    from multiprocessing import Value, set_start_method, get_context
    try: set_start_method("spawn")
    except RuntimeError: pass
    ctx = get_context("spawn")
    shared_max = Value('q', -10**10, lock=False)  # no lock
    chunks = split_chunks(arr, workers)
    t0 = time.perf_counter()
    procs = [ctx.Process(target=_proc_max_mapper_unsafe, args=(c, shared_max)) for c in chunks]
    for p in procs: p.start()
    for p in procs: p.join()
    t1 = time.perf_counter()
    return int(shared_max.value), (t1 - t0)

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
                # median of 3
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

# sync stress test
def sync_stress(trials=100, size=32768, workers=32):
    import random, time as _time
    from multiprocessing import get_context, Value

    data = make_data(size, 123)
    correct = max(data)

    def run_thread_unsafe_once():
        # jitter + tiny chunks
        chunks = split_chunks(data, workers)
        global_max = [-10**10]
        from threading import Thread
        def mapper(d):
            local_m = max(d) if d else -10**10
            # jitter
            if random.random() < 0.7:
                _time.sleep(random.uniform(0, 0.002))
            if local_m > global_max[0]:
                global_max[0] = local_m
        ts = [Thread(target=mapper, args=(c,)) for c in chunks]
        for t in ts: t.start()
        for t in ts: t.join()
        return global_max[0]

    def run_proc_unsafe_once():
        # spawn-safe
        ctx = get_context("spawn")
        shared_max = Value('q', -10**10, lock=False)
        chunks = split_chunks(data, workers)
        ps = [ctx.Process(target=_proc_stress_mapper_unsafe,
                          args=(c, shared_max, 0.7, 2.0))
              for c in chunks]
        for p in ps: p.start()
        for p in ps: p.join()
        return int(shared_max.value)

    wrong_thread = wrong_proc = 0
    t0 = _time.perf_counter()
    for _ in range(trials):
        if run_thread_unsafe_once() != correct:
            wrong_thread += 1
    t1 = _time.perf_counter()
    for _ in range(trials):
        if run_proc_unsafe_once() != correct:
            wrong_proc += 1
    t2 = _time.perf_counter()
    print(f"[sync-stress] trials={trials}, size={size}, workers={workers}")
    print(f"  thread-UNSAFE wrong={wrong_thread}/{trials} (time={t1-t0:.3f}s)")
    print(f"  process-UNSAFE wrong={wrong_proc}/{trials} (time={t2-t1:.3f}s)")

# CLI
def main():
    ap = argparse.ArgumentParser(description="MapReduce-style OS project (threads & processes)")
    ap.add_argument("mode", choices=["sort-thread","sort-proc","max-thread","max-proc","bench","sync-test","sync-stress"])
    ap.add_argument("--size", type=int, default=131072, help="array length (default: 131072)")
    ap.add_argument("--workers", type=int, default=4, help="number of mapper workers (default: 4)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    if args.mode == "bench":
        run_bench()
        return

    if args.mode == "sync-test":
        data = make_data(args.size, args.seed)
        print("\n--- Synchronization Performance Comparison ---")
        print(f"(size={args.size}, workers=8 for max tests; threads/processes use same data)")
        for name, fn in [
            ("max-thread SAFE",   lambda: max_threaded(data, 8)),
            ("max-thread UNSAFE", lambda: max_threaded_unsafe(data, 8)),
            ("max-proc SAFE",     lambda: max_process(data, 8)),
            ("max-proc UNSAFE",   lambda: max_process_unsafe(data, 8)),
        ]:
            g, t = fn()
            print(f"{name:22s} | time={t:.6f}s | global_max={g}")
        return

    if args.mode == "sync-stress":
        # tweak as needed
        sync_stress(trials=100, size=32768, workers=32)
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
