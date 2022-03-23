import MovieRating.{movieData, spark}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CalculatorTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  implicit var spark: SparkSession= _

  before{
      spark=SparkSession
        .builder()
        .appName("MovieRating")
        .master("local[*]")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
  }

  after{
      if(spark !=null){
        spark.stop()
      }
  }

  behavior of "Calculator"
  it should "MeanAndDeviation" in {
    /*
      movie_test.csv contain two identical data. So the avg should be 7.9 and deviation should be 0
     */
    val movieData=spark.read.options(Map("header"->"true")).csv("movie_test.csv")
    val meanAndDeviation=Calculator.calculateMeanAndDeviation(movieData)
    val result=meanAndDeviation.collectAsList()
    /*
      meanAndDeviation is a DataFrame with two column, avg and deviation
      collect it as a List will return a List[Row] contain only one element [7.9,0.0]
      so the first element of result's first and only element is the avg, which should be 7.9
      and the second element of result's first and only element is the deviation, which should be 0.0
     */
    result.get(0).get(0) shouldBe 7.9
    result.get(0).get(1) shouldBe 0.0
  }
}
