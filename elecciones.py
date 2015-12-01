from pyspark import SparkContext
from pyspark import SparkConf

APP_NAME = 'elecciones'
conf = SparkConf().setAppName(APP_NAME)

sc = SparkContext(conf=conf)
registro = sc.textFile('hdfs:///user/root/hdfsdirectory/nacional.csv')
print registro.first()