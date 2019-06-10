from pyspark import SparkContext,SparkConf
if __name__=="__main__":
	conf=SparkConf().setAppName("Groupbyvsreduceby").setMaster("local[*]")
	sc=SparkContext(conf=conf)
	words=['one','two','two','three','three','three']
	wordsRdd=sc.parallelize(words).map(lambda word:(word,1))
	wordCountswithReduceBy=wordsRdd.reduceByKey(lambda x,y:x+y).collect()
	print("wordCountswithReduceByKey: {}".format(list(wordCountswithReduceBy)))
	
	wordCountswithGroupBy=wordsRdd.groupByKey().mapValues(len).collect()
	print("wordCountswithGroupByKey: {}".format(list(wordCountswithGroupBy)))
