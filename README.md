# Customs Data Fetcher

This package fetches customs data from an API and saves it to a CSV file.

## Installation

To install and set up the project, follow these steps:

1. **Clone the repository:**

    ```bash
    git clone https://github.com/olegromanishen/customs_data_fetcher.git
    cd <repository_directory>
    ```

2. **Install Poetry:**

    Poetry is a dependency manager for Python. If you haven't installed Poetry yet, you can do so by following the instructions on the [official website](https://python-poetry.org/docs/#installation).

3. **Install the package using Poetry:**

    ```bash
    poetry install
    ```

## Usage

To fetch the data and save it to a CSV file, you can run the script provided in the package.

1. **Default Usage:**

    By default, the data will be saved to `customs_data.csv` in the current directory.

    ```bash
    poetry run fetch_customs_data
    ```

2. **Specify Output Path:**

    If you want to save the data to a specific file path, you can use the `--output` argument:

    ```bash
    poetry run fetch_customs_data --output /path/to/output.csv
    ```

