import UtilityPackage.Utility
import com.bridgelabz.PythonHandlerPackage.PythonHandler
import com.bridgelabz.TwitterSentimentAnalysisPackage.TwitterStreamingAnalysis
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
object Demo extends App {
  val spark = Utility.createSessionObject("Test")
  val pythonHandler = new PythonHandler(spark)
  val obj = new TwitterStreamingAnalysis(spark, pythonHandler)
  val df = obj.takingInputFromKafka("", "test")
  df.show(5)

}
