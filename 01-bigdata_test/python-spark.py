import os
os.environ['SPARK_HOME'] = "/application/hadoop/app/spark_on_yarn/"
os.environ['JAVA_HOME'] = "/application/hadoop/app/jdk/"
os.environ['HADOOP_CONF_DIR'] = "/application/hadoop/app/hadoop/etc/hadoop"
import findspark
findspark.init()
from pyspark import SparkConf
from pyspark import SparkContext


if __name__ == "__main__":
    conf = SparkConf().setAppName("hu1").setMaster("yarn")
    sc = SparkContext(conf=conf)
    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data)
    sum_num = distData.reduce(lambda a, b: a + b)
    print(sum_num)

    distFile = sc.textFile("README.md")
    sum_num = distFile.map(lambda s: len(s)).reduce(lambda a, b: a + b)
    print(sum_num)
