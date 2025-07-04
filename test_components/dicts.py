import time
import random
from collections import defaultdict

cities = [b"CityA", b"CityB", b"CityC", b"CityD", b"CityE"]
data = [(random.choice(cities), random.randint(-1000, 1000)) for _ in range(10_000_000)]

# defaultdict approach
def run_defaultdict():
    agg = defaultdict(lambda: [float('inf'), float('-inf'), 0, 0])
    for city, measurement in data:
        metrics = agg[city]
        if measurement < metrics[0]:
            metrics[0] = measurement
        if measurement > metrics[1]:
            metrics[1] = measurement
        metrics[2] += measurement
        metrics[3] += 1
    return agg

# manual dict approach
def run_manual_dict():
    agg = {}
    for city, measurement in data:
        metrics = agg.get(city)
        if metrics is None:
            agg[city] = [measurement, measurement, measurement, 1]
        else:
            if measurement < metrics[0]:
                metrics[0] = measurement
            if measurement > metrics[1]:
                metrics[1] = measurement
            metrics[2] += measurement
            metrics[3] += 1
    return agg

# test defaultdict
t0 = time.time()
run_defaultdict()
t1 = time.time()
print(f"defaultdict took {t1 - t0:.3f} seconds")

# test manual dict
t0 = time.time()
run_manual_dict()
t1 = time.time()
print(f"manual dict took {t1 - t0:.3f} seconds")
