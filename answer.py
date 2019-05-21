import sys
sys.path.insert(0,'.')
from pyspark import SparkConf,SparkContext
from util import Utils
def splitComma(line:str):
	splits=Utils.COMMA_DELIMITER.split(line)
	return "{},{}".format(splits[1],splits[6])
if __name__=="__main__":
	conf = SparkConf().setAppName("airports").setMaster("local[*]")
	sc = SparkContext(conf=conf)
	airports = sc.textFile("/home/sapna/Downloads/airports.text")
	airportsInUsa = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6])>40)
	airportsNameAndCityNames = airportsInUsa.map(splitComma)
	airportsNameAndCityNames.coalesce(1).saveAsTextFile("/home/sapna/Desktop/answ")
