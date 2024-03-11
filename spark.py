from pyspark import SparkConf, SparkContext
import pyspark.pandas as ps
import datetime
import glob
import pyarrow.parquet as pq

conf = SparkConf()
conf.set("spark.executor.memory", "20g")
conf.set("spark.executor.cores", "1")
conf.set("spark.driver.memory", "20g")
conf.set("spark.driver.cores", "30")
spark = SparkContext(conf = conf)
df = ps.read_parquet("/data/nyctaxi/set1/*.parquet")
print(df.columns)
print(df.info(verbose=True))
print(df.groupby("payment_type")['fare_amount'].mean())
#average ratio of trip cost that is tolls
#now total num of trips per month across all years
df['date'] = df['tpep_pickup_datetime'].dt.date
df['month'] = pd.DatetimeIndex(df['date']).month #get the date time value and then dot month
print(df.groupby("month").value_counts())
