#import the necessary libraries
#Sicne we are using pyhon, import the SparkSession ans ralted functions
#from the pyspark module

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

#Build a SparkSession using the SparkSession APIs.
#If one does not exist, then create an instance, There
#can only be one SparkSession per JVM

spark = (SparkSession
          .builder
          .appName("PythonMnMCount")
          .getOrCreate()
)
#Get the M&M data ser file from the comman-line arguments
mnm_file = sys.argv[1]
#Read the file into a Spark DataFrame using the CSV
#format by inferring the schema and specifying thah te
#file contains a header, which privedes column names for comma
#-separed files.

mnm_df = (spark.read.format("csv")
          .option("header","true")
          .option("inferSchema","true")
          .load(mnm_file))

#We use theDataFrame high-level APIs. Note 
# that we  don0t use RDDs at all. Because some of Spark's 
# functions return the same object, we can chain function calls. 
# 1. select from the DataFrame the fields "State", "Colr", and "Count" 
# 2.Since we want to group each state and its M&M color count,
#   we use grouoBy()
# 3. Aggregate counts og all colors and groupBy() State and Color
# 4. orderBy() in descending order

count_mnm_df = (mnm_df
                .select("State","Color","Count")
                .groupBy("State","Color")
                .agg(count("Count").alias("Total"))
                .orderBy("Total", ascending=True)
 )

#Show the resulting aggregations for all the states and colors;
# a total count of each color per state.
# Note show() is an ACTION, which will tregger the above
# query to be executed.
count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))   

#Whte the above code aggregated and counted for all
# the states, waht if we hust want to see the data for
# a singe state, eg. CA?
# 1. select from all rows in the DataFrame
# 2. Filter only CA state
# 3. groupBy() STate an Color as we did above
# 4. aggregate the counts for each color
# 5. orderBy() in descendin order
# Find the aggregate count for California by filtering
ca_count_mnm_df = (mnm_df
                   .select("State","Color","Count")
                   .where(mnm_df.State =="CA")
                   .groupBy("State","Color")
                   .agg(count("Count").alias("Total"))
                   .orderBy("Total",ascending=False)
)

#Show the resulting aggregation for California
# As above, show() is ac action that will trigger the execution of the
# entire computation
ca_count_mnm_df.show(n=10, truncate=False)
#Stop the SparkSession
spark.stop()    