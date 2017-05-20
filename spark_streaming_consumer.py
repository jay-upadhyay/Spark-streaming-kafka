from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)
sc = SparkContext("local[5]",appName="SparkStreamingCountBuys")
#filestream= ssc.textFileStream("hdfs:///user/cloudera/hw5/input")
ssc = StreamingContext(sc,10)
brokers, topic = sys.argv[1:]
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
from datetime import datetime
def parseOrder(line):
  #print(line)
  s = line.strip().split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3],"amount": int(s[4]), "price": float(s[5]), "buy": s[6]}]
  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []
lines = kvs.map(lambda x: str(x[1]))
orders = lines.flatMap(parseOrder)
orders.count().pprint()
from operator import add

stocksWindow = orders.map(lambda x: (x['symbol'], x['amount'])).window(10,10)
stocksPerWindow = stocksWindow.reduceByKey(add)

#numPerType = orders.map(lambda o: (o['symbol'],o['amount'])).reduceByKey(add)
maxvolume = stocksPerWindow.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).zipWithIndex().filter(lambda x: x[1] < 1)).map(lambda x: x[0])
maxvolume.pprint()
maxvolume.repartition(1).saveAsTextFiles("hdfs:///user/cloudera/hw5/output_kafka/", "txt")

ssc.start()
ssc.awaitTermination()
# ssc.stop(False)

