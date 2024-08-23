from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

#create a DataFrame using SparkSession

spark = (SparkSession
         .builder
         .appName("AutorsAges")
         .getOrCreate()
)

#Create DataFrame
data_df = spark.createDataFrame([("Brooke",20),("Denny",31),("Jules",30),("TD",35),("Brooke",25)],["name","age"])

#Group the same names together. aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))
avg_df.show(10,truncate=False)