# MapReduce OS Project

Implements parallel sorting and max-value aggregation using multithreading, multiprocessing, and synchronization in Python.

## Run Commands

### Parallel Sorting
```bash
python mapreduce_os_project.py sort-thread --size 131072 --workers 4
python mapreduce_os_project.py sort-proc --size 131072 --workers 4
```

### Max Value Aggregation
```bash
python mapreduce_os_project.py max-thread --size 131072 --workers 8
python mapreduce_os_project.py max-proc --size 131072 --workers 8
```

### Synchronization Test
```bash
python mapreduce_os_project.py sync-test
```

### Full Benchmark
```bash
python mapreduce_os_project.py bench > results.csv
```
