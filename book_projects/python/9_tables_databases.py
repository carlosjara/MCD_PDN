# In Python, define a db
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.functions import col, desc
import os


spark = (SparkSession
            .builder
            .appName("SparkSQLExampleApp")
            .config("spark.local.dir", "/mnt/c/Users/CARLOS/OneDrive/Documentos_Pregado/Escritorio/ICESI/SPARK/spark-3.5.2-bin-hadoop3/repository/MCD_PDN/book_projects/tmp") \

            .getOrCreate()
)
current_dir = os.path.abspath(os.path.dirname(__file__))

# Path to data set
csv_file = os.path.abspath(os.path.join(current_dir, "..", "data", "departuredelays.csv"))

#Read and creaete a temporary view
#infer schema (note that for larger files you
# may want to specify the schema)

# schema = "`date` STRING, `delay` INT, `distance` INT,`origin` STRING, `destination` STRING"

df = (spark.read.format("csv")
    .option("inferSchema", "true")  # Corregido
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
# print("SQL\n")
# spark.sql("""SELECT distance, origin, destination
# FROM us_delay_flights_tbl WHERE distance > 1000
# ORDER BY distance DESC""").show(10)
# print("DF APIs\n")
# (dfdated.select("distance", "origin", "destination")
# .where(col("distance") > 1000)
# .orderBy(desc("distance"))).show(10)
# print("SQL\n")
# spark.sql("""SELECT dateFormat, delay, origin, destination
# FROM us_delay_flights_tbl
# WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
# ORDER by delay DESC""").show(10)

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# Managed tables

# spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,distance INT, origin STRING, destination STRING)")

# Eliminar la tabla gestionada si existe
# spark.sql("DROP TABLE IF EXISTS learn_spark_db.managed_us_delay_flights_tbl")

# schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
# flights_df = spark.read.csv(csv_file, schema=schema)
# flights_df.write.mode("overwrite").saveAsTable("learn_spark_db.managed_us_delay_flights_tbl")

# UnManaged tables

spark.sql(f"""
CREATE TABLE IF NOT EXISTS us_delay_flights_tbl(
    date STRING, 
    delay INT,
    distance INT, 
    origin STRING, 
    destination STRING
)
USING csv
OPTIONS (PATH '{csv_file}')
""")

# (flights_df
# .write
# .option("path", "/data/departuredelays")
# .saveAsTable("us_delay_flights_tbl_"))


#############VIEW CREATION

# In Python
df_sfo = spark.sql("SELECT dateFormat, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT dateFormat, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")
# Create a temporary and global temporary view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

spark.read.table("us_origin_airport_JFK_tmp_view")

spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")