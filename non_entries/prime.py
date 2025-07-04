import os
import argparse
import multiprocessing as mp
import time


def make_chunks(file_name: str):
    """Split file into chunks based on lines to prepare parallel processing."""
    cpu_count = mp.cpu_count()
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count

    chunks = []
    with open(file_name, mode="rb") as f:
        start = 0

        for _ in range(cpu_count):
            f.seek(start + chunk_size)
            f.readline()  # move to end of this line

            end = f.tell()

            # If end exceeds file size, this is the last chunk
            if end >= file_size:
                end = file_size

            chunks.append((file_name, start, end))
            start = end

            if end == file_size:
                break  # last chunk reached, end loop

    return cpu_count, chunks


def _process_file_chunk(file_name: str, chunk_start: int, chunk_end: int):
    """Process one chunk and calculate metrics"""
    result = {}
    with open(file_name, mode="rb") as f:
        f.seek(chunk_start)
        for line in f:
            if f.tell() > chunk_end:
                break
            try:
                city, measurement = line.strip().split(b";")
                measurement = float(measurement) * 10
            except Exception:
                continue

            metrics = result.get(city)
            if metrics:
                metrics[0] = min(metrics[0], measurement)
                metrics[1] = max(metrics[1], measurement)
                metrics[2] += measurement
                metrics[3] += 1
            else:
                result[city] = [measurement, measurement, measurement, 1]
    return result


def process_file(cpu_count, start_end_chunks):
    """Combine chunk from processors and map into final result"""
    with mp.Pool(cpu_count) as pool:
        chunk_results = pool.starmap(_process_file_chunk, start_end_chunks)

    result = {}
    for chunk_result in chunk_results:
        for city, values in chunk_result.items():
            if city in result:
                existing = result[city]
                existing[0] = min(existing[0], values[0])
                existing[1] = max(existing[1], values[1])
                existing[2] += values[2]
                existing[3] += values[3]
            else:
                result[city] = values

    def format_val(val):
        return f"{val / 10:.1f}"

    for city in sorted(result):
        min_val, max_val, total, count = result[city]
        mean_val = total / count
        print(f"{city.decode('utf-8')}={format_val(min_val)}/{format_val(mean_val)}/{format_val(max_val)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process billion row tempratures.")
    parser.add_argument("filename", type=str, help="Path to measurements.txt file")
    args = parser.parse_args()

    t0 = time.time()
    cpu_count, chunks = make_chunks(args.filename)
    process_file(cpu_count, chunks)
    t1 = time.time()
    print(f"\nProcessing took {t1 - t0:.2f} seconds", file=sys.stderr)
