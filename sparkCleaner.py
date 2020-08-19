from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

# SETUP
spark = SparkSession.builder.appName('cleaner').getOrCreate()

df = spark \
  .read \
  .format('csv') \
  .options(delimiter = ',') \
  .load('datafeed') \
  .toDF('control_area', 'unit', 'scp', 'raw_station', 'lines', 'division', 'raw_date', 'time', 'description', 'raw_entries', 'raw_exits')

# CLEAN AND CAST RELEVANT VARIABLES
df = df \
  .withColumn('date', to_timestamp('raw_date', 'MM/dd/yyyy').cast(DateType())) \
  .withColumn('hour', split('time', ':').getItem(0).cast(IntegerType())) \
  .withColumn('total_entries', df['raw_entries'].cast(LongType())) \
  .withColumn('total_exits', split('raw_exits', ' ').getItem(0).cast(LongType()))

# CALCULATE HOURLY ENTRIES AND EXITS
df = df.withColumn('dummy', lit(1))
window = Window.partitionBy('dummy').orderBy('control_area', 'unit', 'scp', 'date', 'hour')

df = df \
  .withColumn('last_entries', lag('total_entries', 1).over(window)) \
  .withColumn('last_exits', lag('total_exits', 1).over(window))

df = df \
  .withColumn('entries', when(df['total_entries'] >= df['last_entries'], df['total_entries'] - df['last_entries']).otherwise(df['total_entries'])) \
  .withColumn('exits', when(df['total_exits'] >= df['last_exits'], df['total_exits'] - df['last_exits']).otherwise(df['total_exits']))

# FILTER FOR OUTLIERS AND RELEVANT TIME PERIOD 3/1/2015 - 4/30/2020
df = df \
  .filter(df['entries'] <= 88000) \
  .filter(df['exits'] <= 88000) \
  .filter(df['date'] >= '2015-03-01') \
  .filter(df['date'] <= '2020-04-30')

# CREATE FIELDS FOR SEASON AND TIME OF DAY
df = df \
  .withColumn('season', when(df['date'].between('2020-03-01', '2020-04-30'), 'coronavirus') \
    .when(month(df['date']).between(3, 5), 'spring') \
    .when(month(df['date']).between(6, 8), 'summer') \
    .when(month(df['date']).between(9, 11), 'fall') \
    .otherwise('winter')) \
  .withColumn('time_of_day', when(df['hour'].between(3, 8), 'early-day') \
    .when(df['hour'].between(11, 16), 'day') \
    .otherwise('night'))

# MERGE ON LATITUDE AND LONGITUDE COORDINATES
coordinates = spark \
  .read \
  .format('csv') \
  .options(delimiter = ',') \
  .load('StationLocations.txt') \
  .toDF('raw_station', 'station', 'raw_latitude', 'raw_longitude')

coordinates = coordinates \
  .withColumn('latitude', coordinates['raw_latitude'].cast(DoubleType())) \
  .withColumn('longitude', coordinates['raw_longitude'].cast(DoubleType()))

df = df.join(coordinates, df['raw_station'] == coordinates['raw_station'])

# AGGREGATE BY STATION, SEASON, AND TIME OF DAY
df = df \
  .groupBy('station', 'latitude', 'longitude', 'season', 'time_of_day', 'date') \
  .sum('entries', 'exits') \
  .withColumnRenamed('sum(entries)', 'sum_entries') \
  .withColumnRenamed('sum(exits)', 'sum_exits')

df = df \
  .groupBy('station', 'latitude', 'longitude', 'season', 'time_of_day') \
  .mean('sum_entries', 'sum_exits') \
  .withColumnRenamed('avg(sum_entries)', 'avg_entries') \
  .withColumnRenamed('avg(sum_exits)', 'avg_exits')

df.select(format_string('%s,%f,%f,%s,%s,%f,%f', 'station', 'latitude', 'longitude', 'season', 'time_of_day', 'avg_entries', 'avg_exits')).write.save('turnstile_parts', format = 'text')

spark.stop()
