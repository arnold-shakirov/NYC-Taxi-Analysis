# NYC Taxi Data Analysis with PySpark

## Overview
This project performs data analysis on NYC taxi trip data stored in Parquet format. Using PySpark and Pandas-on-Spark (`pyspark.pandas`), the script calculates various statistics, such as the average fare by payment type and the number of trips per month. The dataset is read from Parquet files, making it efficient for handling large datasets.

## Features
- **Average Fare by Payment Type**: Calculates the average fare amount for each payment type (cash, card, etc.).
- **Trip Analysis**: Computes the total number of trips per month across all years.
- **Parquet File Processing**: Leverages Parquet for efficient reading and processing of large datasets.
- **Memory Configuration**: Configures Spark to optimize memory and CPU usage for large data processing.

## Project Structure
- **`nyc_taxi_analysis.py`**: The main Python script that uses PySpark to read, process, and analyze the NYC taxi data.
- **`nyctaxi-practice.py`**: An additional practice script for testing and further analysis on the NYC taxi data.

## How to Run the Project

### Prerequisites
- **Apache Spark**: Ensure Spark is installed on your system. You can follow the installation guide on the [Apache Spark website](https://spark.apache.org/downloads.html).
- **PySpark**: Install PySpark via pip if not already installed:
    ```bash
    pip install pyspark
    ```
- **PyArrow**: Install PyArrow for efficient Parquet file processing:
    ```bash
    pip install pyarrow
    ```

### Running the Script

1. Clone the repository:
    ```bash
    git clone https://github.com/arnold-shakirov/Nyc-Taxi-Analysis.git
    ```

2. Navigate to the project directory:

3. Ensure the NYC taxi data in Parquet format is stored in the correct path (e.g., `/data/nyctaxi/set1/*.parquet`). Adjust the path in the script if needed.

4. Run the PySpark script:
    ```bash
    python nyc_taxi_analysis.py
    ```

    Alternatively, you can also run the practice script:
    ```bash
    python nyctaxi-practice.py
    ```

### Output
The script performs the following operations:
1. **Prints Column Names**: Displays the names of the columns in the dataset.
2. **Prints Dataset Info**: Provides detailed information about the dataset, including memory usage and types.
3. **Average Fare by Payment Type**: Prints the average fare amount for each payment type.
4. **Trips per Month**: Calculates and displays the total number of trips per month across all years.

#### Sample Output:
```text
Index(['fare_amount', 'payment_type', 'tpep_pickup_datetime', ...])
<class 'pandas.core.frame.DataFrame'>
...
payment_type
1    13.47
2    15.23
Name: fare_amount, dtype: float64
```
### Dataset
The dataset consists of NYC taxi trip data stored in Parquet format. You can download the dataset from the NYC Taxi & Limousine Commission (TLC) website.

The dataset should contain at least the following columns:

fare_amount: The fare amount for each trip.
payment_type: The type of payment (e.g., cash, card).
tpep_pickup_datetime: The timestamp of when the trip started.

### Requirements
Python 3.x or higher
PySpark (install with pip install pyspark)
PyArrow (install with pip install pyarrow)

### Contact
For any questions or suggestions, feel free to reach out to me at [ashakirov@stetson.edu].
