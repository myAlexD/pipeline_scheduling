import argparse
from common import utils


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True, help="Pipeline text file defining the pipeline")
    parser.add_argument("--cpu_cores", required=True, type=int, help="Number of CPU cores available")
    args = parser.parse_args()

    tasks = utils.read_pipeline(args.pipeline)
    if not tasks:
        raise Exception("Error: No valid tasks in file.")
    min_time, schedule = utils.get_min_execution_time(tasks, args.cpu_cores)
    print(f"Minimum execution time: {min_time}")
    utils.print_schedule(schedule)
