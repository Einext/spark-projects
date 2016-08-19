import org.apache.hadoop.hbase.client.{ HBaseAdmin, Result }
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes



object HBaseReader {

  def main(args: Array[String]) {

    if(args.size != 1){
      println("Usage: HBaseReader <table name>")
      System.exit(-1)
    }
    
    val tableName = args(0)
    val master = ""

    val sc = Connection.createSparkConf()
    val conf = Connection.createHBaseConf()

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }
    
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    println("Number of Records found : " + hBaseRDD.count())

    val stocksRdd = hBaseRDD.map {
      case (key: ImmutableBytesWritable, value: Result) =>
        Stock(
          new String(key.get),
          new String(value.getValue(Bytes.toBytes("prices"), Bytes.toBytes("Open"))),
          new String(value.getValue(Bytes.toBytes("prices"), Bytes.toBytes("Close"))),
          new String(value.getValue(Bytes.toBytes("prices"), Bytes.toBytes("High"))),
          new String(value.getValue(Bytes.toBytes("prices"), Bytes.toBytes("Low"))),
          new String(value.getValue(Bytes.toBytes("prices"), Bytes.toBytes("AdjClose"))),
          new String(value.getValue(Bytes.toBytes("volume"), Bytes.toBytes("vol"))))
    }
    stocksRdd.take(10).foreach(println)

  }
}