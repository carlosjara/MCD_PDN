# In Python, define a query
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.functions import col, desc

spark = (SparkSession
            .builder
            .appName("SparkSQLExampleApp")
            .getOrCreate()
)

# Path to data set
csv_file = "data/departuredelays.csv"

#Read and creaete a temporary view
#infer schema (note that for larger files you
# may want to specify the schema)

# schema = "`date` STRING, `delay` INT, `distance` INT,`origin` STRING, `destination` STRING"

df = (spark.read.format("csv")
    .option("inferSchmea","true")
    .option("header","true")
    .load(csv_file)
)

dfdated =(df
    .withColumn(
    "dateFormat",
    date_format(
        to_timestamp(
            concat(
                substring(col("date"), 1, 2), lit("-"),
                substring(col("date"), 3, 2), lit(" "),
                substring(col("date"), 5, 2), lit(":"),
                substring(col("date"), 7, 2)
            ),
            "MM-dd HH:mm"
        ),
        "MM-dd hh:mm a"
    )
   )
 .drop("date")
)
# (dfdated.select("dateFormat","origin","destination").where(col("destination") == "ORD").show(10, False))

dfdated.createOrReplaceTempView("us_delay_flights_tbl")
print("SQL\n")
spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC""").show(10)
print("DF APIs\n")
(dfdated.select("distance", "origin", "destination")
.where(col("distance") > 1000)
.orderBy(desc("distance"))).show(10)
print("SQL\n")
spark.sql("""SELECT dateFormat, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)
print("DF APIs\n")
(dfdated.select("dateFormat", "delay","origin", "destination")
.where(
        (col("delay") > 120)&
        (col("origin") == 'SFO')&
        (col("destination") == 'ORD')
    )
.orderBy(desc("delay"))).show(10)

print("SQL\n")
spark.sql("""SELECT delay, origin, destination,
        CASE
        WHEN delay > 360 THEN 'Very Long Delays'
        WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
        WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
        WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
        WHEN delay = 0 THEN 'No Delays'
        ELSE 'Early'
        END AS Flight_Delays
        FROM us_delay_flights_tbl
        ORDER BY origin, delay DESC""").show(10)
print("DF APIs\n")
(dfdated.select(
    "delay", "origin", "destination", 
     when(col("delay") > 360, "Very Long Delays")
    .when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
    .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
    .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
    .when((col("delay") == 0) , "No Delays")
    .otherwise("Early").alias("Flight_Delays")
)
.orderBy(asc("origin"),desc("delay"))).show(10,False)