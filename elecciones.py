from pyspark import SparkContext
from pyspark import SparkConf

APP_NAME = 'elecciones'
conf = SparkConf().setAppName(APP_NAME)

sc = SparkContext(conf=conf)
registro = sc.textFile('hdfs:///data/elecciones/nacional.csv')
centros = sc.textFile('hdfs:///data/elecciones/centros.csv')
print registro.first()
print centros.take(2)