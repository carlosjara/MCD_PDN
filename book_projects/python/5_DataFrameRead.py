# In Python, define a schema
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = (SparkSession
            .builder
            .appName("ReadFromCSV")
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

# In Python to save as a Parquet file
# parquet_path = "output/python/fire_df_data"
# fire_df.write.format("parquet").save(parquet_path)

# In Python .. using show using filter
few_fire_df = (fire_df
 .select("IncidentNumber", "AvailableDtTm", "CallType")
 .where(col("CallType") != "Medical Incident"))
#few_fire_df.show(5, truncate=False)

# # In Python, return number of distinct types of calls using countDistinct()
count_distincts = (fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 )
#count_distincts.show()


# In Python, filter for only distinct non-null CallTypes from all the rows
value_disintct = (fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 )
# value_disintct.show(10, False)


# In Python rename column
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5)
 #.show(10, False)
 )
# new_fire_df.show(5, truncate=True)

# In Python Modifying Content Column (to_date and to_timestamp)
fire_ts_df = (fire_df
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm"))
# Select the converted columns
(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 )
# fire_ts_df.show(5, False)

# Python make queries using format date or timestamp, extract year year()
fire_extract_df = (fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 )
# fire_extract_df.show()

# In Python make grouping from some 
# What were the most common type of fire calls?
fire_group_df =(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 
)
# fire_group_df.show(n=10, truncate=False)

|