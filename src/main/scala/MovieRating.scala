
import org.apache.spark.sql.SparkSession
import scala.util.{Success, Try}
import org.apache.spark.rdd.RDD
object MovieRating extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("MovieRating")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val movieData=spark.read.options(Map("header"->"true")).csv("movie_metadata.csv")
  val meanAndDeviation=Calculator.calculateMeanAndDeviation(movieData)
  meanAndDeviation.show()
}
