from pyspark import SparkConf, SparkContext
    
conf = SparkConf().setMaster("local").setAppName("SuperHero.py")
sc = SparkContext(conf = conf)
textfile = sc.textFile("file:///C:/SparkCourse/Marvel-Graph.txt").map(lambda x: (int(x.split()[0]), len(x.split())-1)).reduceByKey(lambda x,y: x+y).map(lambda (x,y): (y,x)).max()
names = sc.textFile("file:///C:/SparkCourse/Marvel-Names.txt").map(lambda x: x.split('"')).map(lambda x: (int(x[0]),x[1])).lookup(textfile[1])[0]
print str(names) + " is the most popular SuperHero"

