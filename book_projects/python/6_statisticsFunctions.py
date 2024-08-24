from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F


spark = (SparkSession
            .builder
            .appName("RowDataFrame-3_6")
            .getOrCreate()
)

# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
 StructField('UnitID', StringType(), True),
 StructField('IncidentNumber', IntegerType(), True),
 StructField('CallType', StringType(), True),
 StructField('CallDate', StringType(), True),
 StructField('WatchDate', StringType(), True),
 StructField('CallFinalDisposition', StringType(), True),
 StructField('AvailableDtTm', StringType(), True),
 StructField('Address', StringType(), True),
 StructField('City', StringType(), True),
 StructField('Zipcode', IntegerType(), True),
 StructField('Battalion', StringType(), True),
 StructField('StationArea', StringType(), True),
 StructField('Box', StringType(), True),
 StructField('OriginalPriority', StringType(), True),
 StructField('Priority', StringType(), True),
 StructField('FinalPriority', IntegerType(), True),
 StructField('ALSUnit', BooleanType(), True),
 StructField('CallTypeGroup', StringType(), True),
 StructField('NumAlarms', IntegerType(), True),
 StructField('UnitType', StringType(), True),
 StructField('UnitSequenceInCallDispatch', IntegerType(), True),
 StructField('FirePreventionDistrict', StringType(), True),
 StructField('SupervisorDistrict', StringType(), True),
 StructField('Neighborhood', StringType(), True),
 StructField('Location', StringType(), True),
 StructField('RowID', StringType(), True),
 StructField('Delay', FloatType(), True)])

sf_fire_file = "data/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
#(new_fire_df
# .select("ResponseDelayedinMins")
# .where(F.col("ResponseDelayedinMins") > 5)
# .show(5, False))


fire_ts_df = (new_fire_df
 .withColumn("IncidentDate", F.to_timestamp(F.col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", F.to_timestamp(F.col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", F.to_timestamp(F.col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm"))

(fire_ts_df
 .select(F.sum("NumAlarms").alias("SUMA_TOTAL"), F.avg("ResponseDelayedinMins"), F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show())

<<<<<<< HEAD

# • What were all the different types of fire calls in 2018?
# • What months within the year 2018 saw the highest number of fire calls?
# • Which neighborhood in San Francisco generated the most fire calls in 2018?
# • Which neighborhoods had the worst response times to fire calls in 2018?
# • Which week in the year in 2018 had the most fire calls?
# • Is there a correlation between neighborhood, zip code, and number of fire calls?
# • How can we use Parquet files or SQL tables to store this data and read it back?
=======
#%%
>>>>>>> b6dc5b100b8853b9ba696f8467a06030de0222c0
