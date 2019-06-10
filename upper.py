import sys
sys.path.insert(0,'.')
from pyspark import SparkContext,SparkConf
from util import Utils
if __name__=="__main__":
	conf=SparkConf().setAppName("airports").setMaster("local[*]")
	sc=SparkContext(conf=conf)
	airportsRDD=sc.textFile("/home/sapna/Downloads/airports.text")
	airportpair=airportsRDD.map(lambda line: (Utils.COMMA_DELIMITER.split(line)[1],Utils.COMMA_DELIMITER.split(line)[3]))
	airportupper=airportpair.mapValues(lambda country:country.upper())
	airportupper.coalesce(1).saveAsTextFile("/home/sapna/Documents/answer/ans1")
