import sys 
sys.path.insert(0,'.')
from pyspark import SparkContext,SparkConf
from util import Utils
def splitComma(line:str):
	splits=Utils.COMMA_DELIMITER.split(line)
	return "{},{}".format(splits[1],splits[2])
if __name__=="__main__":
	conf = SparkConf().setAppName("airports").setMaster("local[*]")
	sc = SparkContext(conf=conf)
	airports = sc.textFile("/home/sapna/Downloads/airports.text")
	airportsInIndia = airports.filter(lambda line:Utils.COMMA_DELIMITER.split(line)[3]=="\"India\"")
	airportsIndia = airportsInIndia.map(splitComma)
	airportsIndia.coalesce(1).saveAsTextFile("/home/sapna/Desktop/ans")	
