# Pipeline Scheduling

## Overview

This project is designed to schedule and manage tasks in a pipeline. It calculates the minimal execution time of a series of tasks with dependencies and different types.


## Prerequisites

This project requires Python 3.6+.

## Installation

1. Clone this repository to your local machine.

    ```sh
    git clone https://github.com/myAlexD/pipeline_scheduling.git
    cd pipeline_scheduling
    ```


## Usage

You can run the program with the following command:

python main.py --input-file <path-to-input-file> --cpu-cores <number-of-cores>

markdown


### Command Line Arguments

- `--input-file`: Path to the text file containing the tasks in the pipeline.
- `--cpu-cores`: Number of CPU cores to be used for the execution. Default is 1.

## Features

- Reads tasks and their properties from a text file.
- Checks for cyclic dependencies.
- Checks for undefined tasks.
- Calculates the minimum execution time for the whole pipeline.

## Tests

To run the tests, execute the following command:

python -m unittest discover tests

## Contributing

If you're interested in contributing to this project, please feel free to open a pull request. If you have any questions or run into any issues, don't hesitate to open an issue.

## License

This project is licensed under the terms of the MIT license.

## Contact

If you have any questions about this project, please feel free to reach out to me at <alexander.dimitrov123@gmail.com>.
