import sys
import argparse
import time
import gc

def main():
    parser = argparse.ArgumentParser(description="Process temperature measurements.")
    parser.add_argument("filename", type=str, help="measurements.txt probably")
    args = parser.parse_args()

    t0 = time.time()
    gc.disable()  # Disable garbage collection

    results = {}
    get = results.get
    append = results.__setitem__

    with open(args.filename, "r", encoding="utf-8") as f:
        for line in f:
            sep_index = line.find(";")
            if sep_index == -1:
                continue
            name = line[:sep_index]
            try:
                temp = int(float(line[sep_index + 1:].strip()) * 10)
            except ValueError:
                continue
            res = get(name)
            if res is None:
                append(name, [1, temp, temp, temp])  # count, sum, min, max
            else:
                res[0] += 1
                res[1] += temp
                res[2] = min(res[2], temp)
                res[3] = max(res[3], temp)

    def f(val):
        return f"{0 if val == -0.0 else val:.1f}"

    for city in sorted(results):
        count, total, min_val, max_val = results[city]
        mean = total / count

        min_out = min_val / 10
        mean_out = mean / 10
        max_out = max_val / 10

        print(f"{city}={f(min_out)}/{f(mean_out)}/{f(max_out)}")

    t1 = time.time()
    print(f"\nTime {t1 - t0:.2f} seconds", file=sys.stderr)

if __name__ == "__main__":
    main()
