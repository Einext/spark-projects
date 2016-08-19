import org.apache.hadoop.hbase.util.Bytes

object StockHBaseType extends Serializable{
  final val cfPrices = Bytes.toBytes("prices")
  final val cfVolume = Bytes.toBytes("volume")
  final val colOpen = Bytes.toBytes("Open")
  final val colHigh = Bytes.toBytes("High")
  final val colLow = Bytes.toBytes("Low")
  final val colClose = Bytes.toBytes("Close")
  final val colAdjClose = Bytes.toBytes("AdjClose")
  final val colVol = Bytes.toBytes("Vol")
}

