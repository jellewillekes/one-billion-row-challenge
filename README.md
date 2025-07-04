# One Billion Row Challenge — RoadMap

## The challenge

The original [One Billion Row Challenge](https://www.1brc.io/) asks you to write a program that processes a huge text file:

- **Input:** text file with up to 1 billion rows (>10GB), each with format:
  ```
  StationName;12.3
  City;-8.4
  ...
  ```

- **Goal:** compute the **min**, **mean**, and **max** temperatures per station, sorted alphabetically.

- **Constraints:**
  - Use only the standard library (no numpy, pandas, etc.)
  - Handle UTF-8 station names up to 100 bytes.
  - Max 10,000 unique station names.

---

## How we built it

We started from a straightforward approach and optimized step by step.

---

## `basic.py` — the initial approach

### How it works

- Reads the file using plain `open()` in binary mode, processes line by line.
- Uses Python's `float()` for each measurement, multiplied by 10 for fixed-point integer math to avoid float rounding issues.
- Aggregates `[min, max, sum, count]` per station in a standard Python dictionary.

### Parallelism

- Splits the file into byte chunks roughly by CPU count, adjusting to line breaks.
- Uses `multiprocessing.Pool` to process each chunk in parallel.
- After processing, merges all partial dictionaries into a final result.

### Performance

| File size | Time |
|-----------|------|
| 10 million rows (~1GB) | ~10.5 s |

### Why it’s slow

- Each process does explicit file reading and decoding, causing redundant I/O.
- Standard dictionaries require repeated key checks.
- Float parsing in Python is relatively slow.

---

## `jelle.py` — the optimized approach

### How it works

- Uses **`mmap`** to memory-map the entire file.  
  This means the file contents are loaded into virtual memory once, and each process simply reads bytes from shared memory — no repeated disk I/O.
- Calculates byte ranges for each process, aligned on `\n` so no line is split.
- Uses **`multiprocessing.Process` with `fork`** (on macOS/Linux), letting all child processes inherit the `mmap`. No memory is copied.
- Each process:
  - Parses its slice line by line.
  - Uses a **`defaultdict`** to maintain `[min, max, sum, count]` for each city, eliminating repeated `if key in dict` checks.
- Finally merges results in the parent process.

### Performance

| File size | Time |
|-----------|------|
| 10 million rows (~1GB) | ~1.23 s |

### Why it’s fast

- **No explicit reads:** `mmap` means all processes read from memory directly.
- **Zero-copy parallelism:** `fork` lets all processes share the same `mmap`.
- **Efficient aggregations:** `defaultdict` skips hash lookups for new keys.

---

## How to run

### Generate a test file

Use the provided script to generate a file with 10 million rows.

```bash
python create_measurements.py --rows 10000000 --output measurements_ten_million.txt
```

### Run the initial version

```bash
python non_entries/basic.py measurements_ten_million.txt
```

### Run the optimized version

```bash
python entries/jelle.py measurements_ten_million.txt
```

Both will print results like:

```
Amsterdam=-99.9/-0.1/99.9
Zurich=-98.2/0.2/98.5
...
```

and show processing time at the end.

---

## Hardware & results

- **Machine:** MacBook Pro 16-inch (2019), 2.4 GHz 8-Core Intel Core i9, 32 GB 2667 MHz DDR4 RAM
- **File:** 10 million rows (~1GB)
- **Runtime:**
  - `basic.py`: ~10.5 s
  - `jelle.py`: ~1.23 s (8x faster using all cores, `mmap` + `multiprocessing`)

---

## Key Python standard library techniques

| Component                  | What it does                                         |
|-----------------------------|-----------------------------------------------------|
| `mmap`                      | Memory-maps the file for zero-copy byte access.      |
| `multiprocessing` with `fork` | Spawns processes that inherit the same `mmap`, sharing memory with no copying. |
| `defaultdict`               | Maintains `[min, max, sum, count]` per city efficiently, avoids repeated `key in dict` checks. |

---

## Ground truth tests

Use `ground_truth.py` to compare your output to precomputed expected outputs.

```bash
python ground_truth.py measurements_ten_million.txt
```

This ensures your processing is mathematically correct.

---

## Profiling

Run `cProfile` with `profile.sh` to see where time is spent.  
In `jelle.py`, most time is in parsing floats and small process locks, with merging negligible.

---

## Summary

We transformed a simple Python script into an optimized multi-core pipeline using only the standard library:

- From explicit line reads and repeated key checks to zero-copy shared memory.
- From 10+ seconds to about 1.2 seconds on 10 million rows — purely by smarter I/O, multiprocessing, and efficient in-memory aggregation.

