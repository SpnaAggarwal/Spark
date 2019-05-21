from pyspark import SparkContext,SparkConf
if __name__=="__main__":
	conf=SparkConf().setAppName("wordCount").setMaster("local[*]")
	sc=SparkContext(conf=conf)
	lines=sc.textFile("/home/sapna/a.txt")
	words=lines.flatMap(lambda line:line.split(" "))
	wordCounts=words.countByValue()
	for x,y in wordCounts.items():
		print("{}:{}".format(x,y))	

