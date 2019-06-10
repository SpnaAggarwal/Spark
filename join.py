from pyspark import SparkContext,SparkConf
if __name__=="__main__":
	conf=SparkConf().setAppName("join").setMaster("local[*]")
	sc=SparkContext(conf=conf)
	ages=sc.parallelize([('Tom',29),('John',22)])
	address=sc.parallelize([('James','USA'),('John','UK')])
	join=ages.join(address)
	join.coalesce(1).saveAsTextFile("/home/sapna/Documents/answer/joinout")
	leftouter=ages.leftOuterJoin(address)
	leftouter.coalesce(1).saveAsTextFile("/home/sapna/Documents/answer/leftouter")
	rightouter=ages.rightOuterJoin(address)
	rightouter.coalesce(1).saveAsTextFile("/home/sapna/Documents/answer/rightouter")
	fullouter=ages.fullOuterJoin(address)
	fullouter.coalesce(1).saveAsTextFile("/home/sapna/Documents/answer/fullouter") 
