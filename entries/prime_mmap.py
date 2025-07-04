import os
import mmap
import argparse
import time
import sys
import multiprocessing as mp
from collections import defaultdict

# Global mmap, inherited by fork
mm = None

def make_mmap_chunks(file_name: str, max_cpu: int = 8):
    global mm
    cpu_count = min(max_cpu, mp.cpu_count())
    size = os.path.getsize(file_name)
    chunk_size = size // cpu_count

    with open(file_name, 'rb') as f:
        mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)

    chunks = []
    start = 0
    for _ in range(cpu_count):
        end = start + chunk_size
        if end >= size:
            end = size
        else:
            while end < size and mm[end:end+1] != b'\n':
                end += 1
        chunks.append((start, end))
        start = end + 1
        if start >= size:
            break
    return cpu_count, chunks

def agg_initializer():
    return [float('inf'), float('-inf'), 0, 0]

def process_chunk_in_worker(args):
    start, end = args
    result = defaultdict(agg_initializer)
    data = mm[start:end]
    for line in data.splitlines():
        try:
            city, measurement = line.strip().split(b";")
            measurement = float(measurement) * 10
        except Exception:
            continue
        agg = result[city]
        if measurement < agg[0]:
            agg[0] = measurement
        if measurement > agg[1]:
            agg[1] = measurement
        agg[2] += measurement
        agg[3] += 1
    return result

def merge_results(chunk_results):
    result = defaultdict(agg_initializer)
    for chunk_result in chunk_results:
        for city, values in chunk_result.items():
            agg = result[city]
            if values[0] < agg[0]:
                agg[0] = values[0]
            if values[1] > agg[1]:
                agg[1] = values[1]
            agg[2] += values[2]
            agg[3] += values[3]
    return result

def format_val(val):
    return f"{val / 10:.1f}"

def process_file(cpu_count, chunks):
    with mp.Pool(cpu_count) as pool:
        chunk_results = pool.map(process_chunk_in_worker, chunks)

    final_result = merge_results(chunk_results)

    for city in sorted(final_result):
        min_val, max_val, total, count = final_result[city]
        mean_val = total / count
        print(f"{city.decode('utf-8')}={format_val(min_val)}/{format_val(mean_val)}/{format_val(max_val)}")

if __name__ == "__main__":
    mp.set_start_method('fork')  # important for mmap sharing
    parser = argparse.ArgumentParser(description="Process billion row temperatures (single mmap, inherited by fork).")
    parser.add_argument("filename", type=str, help="measurements.txt file")
    args = parser.parse_args()

    t0 = time.time()
    cpu_count, chunks = make_mmap_chunks(args.filename)
    process_file(cpu_count, chunks)
    t1 = time.time()
    print(f"\nProcessing took {t1 - t0:.2f} seconds", file=sys.stderr)
