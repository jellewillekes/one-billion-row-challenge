import time
import random

def float_parse(val_bytes):
    """Existing approach: float() then x10"""
    return int(float(val_bytes) * 10)

def manual_ascii_parse(val_bytes):
    """New approach: manual ASCII parsing to deci-integers"""
    sign = -1 if val_bytes.startswith(b"-") else 1
    val_bytes = val_bytes.lstrip(b"-")
    dot = val_bytes.find(b".")
    if dot == -1:
        num = int(val_bytes) * 10
    else:
        num = int(val_bytes[:dot] + val_bytes[dot+1:])
    return sign * num


def test_correctness():
    """Quick tests to verify same result"""
    test_cases = [
        (b"12.3", 123),
        (b"-9.8", -98),
        (b"100", 1000),
        (b"-0.5", -5),
        (b"0.0", 0),
        (b"0", 0),
        (b"-10.0", -100),
    ]
    for val_bytes, expected in test_cases:
        assert float_parse(val_bytes) == expected, f"float_parse failed on {val_bytes}"
        assert manual_ascii_parse(val_bytes) == expected, f"manual_parse failed on {val_bytes}"


def test_speed():
    """Benchmark difference"""
    vals = [b"12.3", b"-9.8", b"100", b"-0.5", b"0.0", b"0", b"-10.0"] * 100_000

    t0 = time.time()
    for v in vals:
        float_parse(v)
    t1 = time.time()

    t2 = time.time()
    for v in vals:
        manual_ascii_parse(v)
    t3 = time.time()

    print(f"float() parse took: {(t1 - t0):.4f} seconds")
    print(f"manual ASCII parse took: {(t3 - t2):.4f} seconds")


if __name__ == "__main__":
    test_correctness()
    test_speed()
    print("All tests passed")
