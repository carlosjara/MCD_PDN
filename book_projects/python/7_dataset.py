# In Python, define a schema
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row

spark = (SparkSession
            .builder
            .appName("ReadFromCSV")
            .getOrCreate()
)

row = Row(350, True, "Learning Spark 2E" , None)

# In Python
print(row[0],row[1],row[2])

