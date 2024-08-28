package main.scala.chapter2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
* Usage: MnMcount <mnm_file_dataset>
*/
object MnMcount {
def main(args: Array[String]) {
val spark = SparkSession
    .builder
    .appName("MnMCount")
    .getOrCreate()
if (args.length < 1) {
print("Usage: MnMcount <mnm_file_dataset>")
sys.exit(1)
}
// Get the M&M data setfilename
val mnmFile = args(0)