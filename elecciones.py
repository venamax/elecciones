from pyspark import SparkContext
from pyspark import SparkConf
from pprint import pprint

APP_NAME = 'elecciones'
conf = SparkConf().setAppName(APP_NAME)

sc = SparkContext(conf=conf)
registro = sc.textFile('hdfs:///data/elecciones/nacional.csv')
centros = sc.textFile('hdfs:///data/elecciones/centros.csv')
ama = sc.textFile('hdfs:///data/elecciones/AMA.csv')
pprint (registro.first())
print '-----------------------------------------------------'
pprint (centros.take(2))
print '-----------------------------------------------------'
pprint (ama.take(2))