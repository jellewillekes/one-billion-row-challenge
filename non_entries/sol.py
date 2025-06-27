import os
import argparse
import time
import sys
from gc import disable as gc_disable, enable as gc_enable
import multiprocessing as mp


def get_file_chunks(file_name: str, max_cpu: int = 8):
    """Split file into chunks that start/end on newlines."""
    cpu_count = min(max_cpu, mp.cpu_count())
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count

    start_end = []
    with open(file_name, mode="rb") as f:

        def is_new_line(position):
            if position == 0:
                return True
            f.seek(position - 1)
            return f.read(1) == b"\n"

        def next_line(position):
            f.seek(position)
            f.readline()
            return f.tell()

        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)
            while not is_new_line(chunk_end) and chunk_end < file_size:
                chunk_end += 1

            start_end.append((file_name, chunk_start, chunk_end))
            chunk_start = chunk_end

    return cpu_count, start_end


def _process_file_chunk(file_name: str, chunk_start: int, chunk_end: int):
    """Process one chunk in a subprocess"""
    result = {}
    with open(file_name, mode="rb") as f:
        f.seek(chunk_start)
        gc_disable()
        for line in f:
            if f.tell() > chunk_end:
                break
            try:
                location, measurement = line.strip().split(b";")
                measurement = float(measurement)
            except Exception:
                continue  # skip malformed lines

            stats = result.get(location)
            if stats:
                stats[0] = min(stats[0], measurement)
                stats[1] = max(stats[1], measurement)
                stats[2] += measurement
                stats[3] += 1
            else:
                result[location] = [measurement, measurement, measurement, 1]
        gc_enable()
    return result


def process_file(cpu_count, start_end_chunks):
    """Combine chunk results into final output"""
    with mp.Pool(cpu_count) as pool:
        chunk_results = pool.starmap(_process_file_chunk, start_end_chunks)

    result = {}
    for chunk_result in chunk_results:
        for location, values in chunk_result.items():
            if location in result:
                existing = result[location]
                existing[0] = min(existing[0], values[0])
                existing[1] = max(existing[1], values[1])
                existing[2] += values[2]
                existing[3] += values[3]
            else:
                result[location] = values

    # Final print: match required benchmark output (no commas/brackets, sorted)
    for location in sorted(result):
        min_val, max_val, total, count = result[location]
        mean = total / count
        # Normalize -0.0 to 0.0
        def fmt(v): return f"{abs(v) if v == 0 else v:.1f}"
        print(f"{location.decode('utf-8')}={fmt(min_val)}/{fmt(mean)}/{fmt(max_val)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process temperature file in parallel")
    parser.add_argument("filename", type=str, help="Path to measurements.txt file")
    args = parser.parse_args()

    t0 = time.time()
    cpu_count, chunks = get_file_chunks(args.filename)
    process_file(cpu_count, chunks)
    t1 = time.time()
    print(f"\nProcessing took {t1 - t0:.2f} seconds", file=sys.stderr)
