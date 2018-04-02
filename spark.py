#import findspark
#findspark.init()

import os
import shutil

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext

SparkContext.setSystemProperty('spark.executor.memory', '8g')
SparkContext.setSystemProperty('spark.python.worker.memory', '8g')
SparkContext.setSystemProperty('spark.driver.memory', '16g')
ss = SparkSession\
     .builder\
     .master("local[20]") \
     .appName("testing") \
     .getOrCreate()

sc = ss.sparkContext
sql = pyspark.sql.SQLContext(sc)

print("spark version = " + sc.version)

for row in (sc._conf.getAll()):
        print (row)

sparkSession = ss
sparkContext = sc
sparkSQL = sql

def load_csv(filename, sep=","):
    return ss.read.csv(filename,\
                       inferSchema=True,\
                       sep=sep,\
                       header=True,\
                       )

def save_csv(filename, df, sep=","):
    if (os.path.exists(filename)): shutil.rmtree(filename)
    return df.write.csv(filename,\
                        sep=sep,\
                        header=True,\
                        )

