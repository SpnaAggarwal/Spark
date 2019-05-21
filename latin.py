import sys
sys.path.insert(0,'.')
from pyspark import SparkContext,SparkConf
from util import Utils

def splitComma(line:str):
	splits=Utils.COMMA_DELIMITER.split(line)
	return "{},{},{},{}".format(splits[1],splits[2],splits[3],splits[7])

if __name__ == "__main__":
	conf=SparkConf().setAppName("airports").setMaster("local[*]")
	sc=SparkContext(conf=conf)
	airports=sc.textFile("/home/sapna/Downloads/airports.text")
	airportsinindia=airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[7])>60)
	#a=airportsinindia.filter(lambda line:Utils.COMMA_DELIMITER.split(line)[3]=="\"India\"")
	airportlatin=airportsinindia.map(splitComma)
	airportlatin.saveAsTextFile("/home/sapna/Documents/answer/awn2")
	airports=sc.textFile("/home/sapna/Desktop/awn2/part-*")
	a=airportsinindia.filter(lambda line:Utils.COMMA_DELIMITER.split(line)[3]=="\"India\"")
	airportlatin1=a.map(splitComma)
	airportlatin1.saveAsTextFile("/home/sapna/Documents/answer/awn23c3")

