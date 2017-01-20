# Tested for Spark 2.0.2
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[*]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

# lines is a DStream
lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split())
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
