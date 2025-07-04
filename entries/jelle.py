import os
import mmap
import multiprocessing as mp
from collections import defaultdict

# Global mmap object. Each process (due to fork) can directly access this.
mm = None


def make_chunks(file_name: str):
    """
    Memory-maps the entire input file and splits it into chunks based on line boundaries.

    Each chunk is defined by (start_byte, end_byte) to allow multiple processes to work
    on disjoint slices of the file in parallel without overlap or missing lines.

    Returns:
        List of (start, end) byte offsets for each process to work on.
    """
    global mm
    cpu_count = mp.cpu_count()
    size = os.path.getsize(file_name)
    chunk_size = size // cpu_count

    # Memory-map the entire file in read-only mode.
    with open(file_name, 'rb') as f:
        mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)

    chunks = []
    start = 0
    for _ in range(cpu_count):
        end = start + chunk_size
        # Adjust 'end' to the next newline to avoid splitting a line
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
    """
    Returns the initial aggregation state for a city:
    [min, max, sum, count]
    """
    return [float('inf'), float('-inf'), 0.0, 0]


def process_chunk(start, end, queue):
    """
    Processes a single chunk of the mmap (from start to end byte offset).

    Builds a defaultdict of aggregations:
    - min temperature
    - max temperature
    - sum of temperatures
    - count of measurements

    Finally puts the resulting dictionary on the multiprocessing queue
    for the parent process to collect.

    This function runs in its own process.
    """
    result = defaultdict(agg_initializer)
    data = mm[start:end]

    # Split data into lines
    for line in data.splitlines():
        city, measurement = line.split(b";")
        measurement = float(measurement) * 10  # multiply by 10 trick for integers

        # Retrieve the aggregation list for this city
        agg = result[city]
        m_min, m_max, m_sum, m_count = agg
        # Update the aggregation values
        if measurement < m_min:
            agg[0] = measurement
        if measurement > m_max:
            agg[1] = measurement
        agg[2] = m_sum + measurement
        agg[3] = m_count + 1

    # Convert defaultdict to regular dict
    queue.put(dict(result))


def merge_results(chunk_results):
    """
    Merges multiple partial aggregation dictionaries into a final one.

    This runs in the main process after all workers have completed.
    """
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


def process_file_from_path(filename):
    """
    Main entry point to process a file given by path.
    - Sets up mmap and chunks
    - Spawns processes to handle each chunk
    - Gathers results from processes
    - Merges results and prints final output
    """
    mp.set_start_method('fork', force=True)  # boosts Mac/Linux for mmap sharing
    chunks = make_chunks(filename)
    queue = mp.Queue()

    processes = []
    for start, end in chunks:
        p = mp.Process(target=process_chunk, args=(start, end, queue))
        p.start()
        processes.append(p)

    # Collect results
    chunk_results = []
    for _ in processes:
        chunk_results.append(queue.get())

    # Ensure processes have completed
    for p in processes:
        p.join()

    # Merge all partial results
    final_result = merge_results(chunk_results)

    # Print sorted results
    for city in sorted(final_result):
        min_val, max_val, total, count = final_result[city]
        mean_val = total / count
        print(f"{city.decode()}={min_val / 10:.1f}/{mean_val / 10:.1f}/{max_val / 10:.1f}")


if __name__ == "__main__":
    import argparse
    import time
    import sys

    parser = argparse.ArgumentParser(description="Process billion row temperatures with mmap, multiprocessing and aggregation.")
    parser.add_argument("filename", type=str, help="measurements.txt file")
    args = parser.parse_args()

    t0 = time.time()
    process_file_from_path(args.filename)
    t1 = time.time()
    print(f"\nProcessing took {t1 - t0:.2f} seconds", file=sys.stderr)
