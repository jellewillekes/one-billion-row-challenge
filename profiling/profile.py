import cProfile
import pstats

if __name__ == "__main__":
    filename = "measurements_ten_million.txt"
    profile_file = "cprofile_report.txt"
    cProfile.run(f'process_file_from_path("{filename}")', filename=profile_file)

    p = pstats.Stats(profile_file)
    p.strip_dirs().sort_stats("tottime").print_stats(20)
