import timeit

s1 = b"12.3"
s2 = b"-9.8"

print("float():", timeit.timeit("float(s1) * 10", globals=globals(), number=1_000_000))
print("manual:", timeit.timeit("""
dot = s1.find(b".")
num = int(s1[:dot] + s1[dot+1:])
num * (10 if dot == -1 else 1)
""", globals=globals(), number=1_000_000))
