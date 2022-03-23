import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, stddev}

object Calculator {

  def calculateMeanAndDeviation(data: DataFrame): DataFrame= data.agg(avg(data("imdb_score")),stddev(data("imdb_score")))
}
