import itertools as it
import pathlib
import subprocess
from collections import defaultdict
from timeit import default_timer as timer
import sys


if len(sys.argv) == 1:
    measurements_file = "measurements.txt"
else:
    measurements_file = sys.argv[1]


def make_ground_truth():
    import polars as pl

    df = pl.scan_csv(
        measurements_file,
        separator=";",
        has_header=False,
        with_column_names=lambda _: ["station_name", "measurement"],
    )

    grouped = (
        df.with_columns((pl.col("measurement") * 10).alias("measurement"))
        .group_by("station_name")
        .agg(
            pl.min("measurement").alias("min_measurement"),
            pl.mean("measurement").alias("mean_measurement"),
            pl.max("measurement").alias("max_measurement"),
        )
        .sort("station_name")
        .collect(streaming=True)
    )
    result = []
    for data in grouped.iter_rows():
        result.append(f"{data[0]}={data[1]/10:.1f}/{data[2]/10:.1f}/{data[3]/10:.1f}")
    return result


entries = list(pathlib.Path("entries/").glob("*.py"))

ground_truth_path = pathlib.Path(f"ground_truth_{measurements_file}")
if ground_truth_path.exists():
    print(f"reading ground truth from {ground_truth_path}")
    ground_truth = open(ground_truth_path).read().splitlines()
else:
    print("Generating ground truths")
    ground_truth = make_ground_truth()
    with open(ground_truth_path, "w") as f:
        f.write("\n".join(ground_truth))

print("The following entries will be verified")
for entry in entries:
    print(f" - {entry}")


def compare(ground_truth, result):
    for l, r in it.zip_longest(ground_truth, result):
        if l != r:
            yield f"{l}  !=  {r}"


times = defaultdict(list)
for entry in entries:
    print(f"========== {entry} ==========")
    for i in range(3):
        try:
            tic = timer()
            res = subprocess.run(
                ["python", entry, measurements_file],
                encoding="utf-8",
                capture_output=True,
                text=True,
            )
            toc = timer()
            res.check_returncode()
        except Exception as e:
            print(f"entry {entry} failed to run succesfully: {e}")
        else:
            print("comparing result to ground truth")
            resultlines = res.stdout.splitlines()
            if len(resultlines) == 0:
                print("no output produced")
                continue
            diff = list(compare(ground_truth, resultlines))
            incorrect = False
            if len(diff) != 0:
                for idx, diff_entry in enumerate(diff):
                    try:
                        l, r = diff_entry.split("!=")
                        l_city, l_measurements = l.split("=")
                        r_city, r_measurements = r.split("=")
                        l_min, l_mean, l_max = l_measurements.split("/")
                        r_min, r_mean, r_max = r_measurements.split("/")
                        if (
                            float(l_min) != float(r_min)
                            or float(l_mean) != float(r_mean)
                            or float(l_max) != float(r_max)
                            or l_city != r_city
                        ):
                            incorrect = True
                            if idx < 10:
                                print(diff_entry)
                    except ValueError as e:
                        if idx < 10:
                            print(f"comparison error at line, {diff_entry}")
            if not incorrect:
                times[entry].append(toc - tic)

print()
print(f"========== leaderboard ==========")
print()
print()

picked_times = []
for entry_name, entry_times in times.items():
    picked_time = sorted(entry_times)[len(entry_times) // 2]
    picked_times.append((picked_time, entry_name))

idx = 1
for entry_time, entry_name in sorted(picked_times):
    print(f"#{idx}: {entry_name} with {entry_time}")
    idx += 1