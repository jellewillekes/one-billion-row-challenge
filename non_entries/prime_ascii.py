import os
import mmap
import multiprocessing as mp
from collections import defaultdict

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
            while end < size and mm[end:end + 1] != b'\n':
                end += 1
        chunks.append((start, end))
        start = end + 1
        if start >= size:
            break
    return chunks


def agg_initializer():
    return [float('inf'), float('-inf'), 0, 0]


def ascii_to_deciint(val_bytes):
    sign = 1
    i = 0
    if val_bytes[0:1] == b"-":
        sign = -1
        i = 1

    num = 0
    found_dot = False
    while i < len(val_bytes):
        c = val_bytes[i:i+1]
        if c == b".":
            found_dot = True
            i += 1
            continue
        num = num * 10 + (ord(c) - 48)
        i += 1
    if found_dot:
        return sign * num
    else:
        return sign * num * 10


def process_chunk(start, end, queue):
    result = defaultdict(agg_initializer)
    data = mm[start:end]
    for line in data.splitlines():
        sep = line.find(b";")
        if sep == -1:
            continue
        city = line[:sep]
        val_bytes = line[sep+1:]
        try:
            measurement = ascii_to_deciint(val_bytes)
        except Exception:
            continue
        agg = result[city]
        if measurement < agg[0]:
            agg[0] = measurement
        if measurement > agg[1]:
            agg[1] = measurement
        agg[2] += measurement
        agg[3] += 1
    queue.put(dict(result))  # convert defaultdict to dict for pickling


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


def process_file_from_path(filename):
    mp.set_start_method('fork', force=True)
    chunks = make_mmap_chunks(filename)
    queue = mp.Queue()

    processes = []
    for start, end in chunks:
        p = mp.Process(target=process_chunk, args=(start, end, queue))
        p.start()
        processes.append(p)

    chunk_results = []
    for _ in processes:
        chunk_results.append(queue.get())

    for p in processes:
        p.join()

    final_result = merge_results(chunk_results)
    for city in sorted(final_result):
        min_val, max_val, total, count = final_result[city]
        mean_val = total / count
        print(f"{city.decode('utf-8')}={format_val(min_val)}/{format_val(mean_val)}/{format_val(max_val)}")


if __name__ == "__main__":
    import argparse
    import time
    import sys

    parser = argparse.ArgumentParser(description="Process billion row temperatures (ASCII manual parse, tuned processes).")
    parser.add_argument("filename", type=str, help="measurements.txt file")
    args = parser.parse_args()

    t0 = time.time()
    process_file_from_path(args.filename)
    t1 = time.time()
    print(f"\nProcessing took {t1 - t0:.2f} seconds", file=sys.stderr)
