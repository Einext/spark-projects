import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat



object HBaseWriter {

  def toStock(rec: String): Stock = {
    val splits = rec.split(",")
    Stock(splits(0), splits(1), splits(2), splits(3), splits(4), splits(5), splits(6))
  }

  def toPut(stock: Stock): (ImmutableBytesWritable, Put) = {
    val rowKey = stock.pdate
    val put = new Put(Bytes.toBytes(rowKey))

    put.add(StockHBaseType.cfPrices, StockHBaseType.colOpen, Bytes.toBytes(stock.open))
    put.add(StockHBaseType.cfPrices, StockHBaseType.colHigh, Bytes.toBytes(stock.high))
    put.add(StockHBaseType.cfPrices, StockHBaseType.colLow, Bytes.toBytes(stock.low))
    put.add(StockHBaseType.cfPrices, StockHBaseType.colClose, Bytes.toBytes(stock.close))
    put.add(StockHBaseType.cfPrices, StockHBaseType.colAdjClose, Bytes.toBytes(stock.adjClose))
    put.add(StockHBaseType.cfVolume, StockHBaseType.colVol, Bytes.toBytes(stock.volume))

    (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
  }
  
  
  
  def main(args: Array[String]): Unit = {
    if(args.size != 3){
      println("Usage: HBaseConnector <data source> <table name> <number of thread>")
      System.exit(-1)
    }
    
    val sourceFile = args(0)
    val tableName = args(1)
    val numThread = args(2).toInt
    // This parameters controls how many parallel load occurs.
    // More is better, but more thread creates more pressure to HBase heap memory.
    
    val sc = Connection.createSparkConf()
    val hbaseConf = Connection.createHBaseConf()

    
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    
    sc.textFile(sourceFile).
        filter(line => line.split(",").size == 7 && !line.startsWith("Date")).
        repartition(numThread).
        map(toStock).
        map(toPut).
        saveAsNewAPIHadoopFile("/user/user01/out",
            classOf[ImmutableBytesWritable], 
            classOf[Put], 
            classOf[TableOutputFormat[Put]], 
            hbaseConf)
  }

}
