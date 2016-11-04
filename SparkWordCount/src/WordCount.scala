import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Logger

object WordCount {
  
  val log = Logger.getLogger(WordCount.getClass.getCanonicalName)
  
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: WordCount <input path> <output path>")
      System.exit(-1)
    }
    val conf = new SparkConf()

    conf.setAppName("sample demo")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.textFile(args(0)).
      flatMap(_.split("\\W+")).
      filter(_.length > 0).
      map((_, 1)).
      reduceByKey(_+_).
      saveAsTextFile(args(1))
    print("The program has finished")
    log.info("The program has finished")
    
  }
}