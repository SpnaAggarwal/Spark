from pyspark import SparkContext,SparkConf
if __name__=="__main__":
	conf=SparkConf().setAppName("collect").setMaster("local[*]")
	sc=SparkContext(conf=conf)
	input1=['spark','hadoop','spark vs hadoop','spark','ml']
	input1.sort()
	words=sc.parallelize(input1)
	coll=words.collect()
	for a in coll:
		print(a)

