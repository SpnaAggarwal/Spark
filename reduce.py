import sys
sys.path.insert(0,'.')
from pyspark import SparkContext,SparkConf
from util import Utils
if __name__=='__main__':
	conf=SparkConf().setAppName('airports').setMaster('local[*]')
	sc=SparkContext(conf=conf)
	lines=sc.textFile("/home/sapna/airports.text")
	countryandairport=lines.map(lambda airport:(Utils.COMMA_DELIMITER.split(airport)[3],Utils.COMMA_DELIMITER.split(airport)[1]))
	airportsByCountry=countryandairport.reduceByKey(lambda x,y:x+y).collect()
	for country,airportname in airportsByCountry.items():
		print("{}:{}".format(country,list(airportname)))
