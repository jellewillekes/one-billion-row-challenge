#!/bin/bash

set -e

echo "========================"
echo "Running cProfile on entries/prime.py"
echo "========================"

python -m cProfile -s tottime entries/1_prime.py measurements_ten_million.txt > cprofile_report.txt

echo ""
echo " cProfile complete, results saved in cprofile_report.txt"
echo ""

echo "========================"
echo "Running timeit micro benchmarks"
echo "========================"

python test_components/profile_prime.py

echo ""
echo " timeit micro tests complete"
echo ""

echo "========================"
echo "Finished!"
echo ""
echo "Open cprofile_report.txt."
