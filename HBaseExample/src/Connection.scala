import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object Connection {

  
  def createSparkConf(): SparkContext = {
    val conf = new SparkConf().
      setAppName("HBaseSensorStream").
      set("spark.files.overwrite", "true").
      setMaster("local[*]")

    val sc = new SparkContext(conf)    
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    sc
  }

  def createHBaseConf() = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "einext02:2181")
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf
    
    

  }

}