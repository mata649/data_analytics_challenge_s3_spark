import findspark
findspark.init()
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import sum, count
from cfg import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

spark = SparkSession.builder.appName('ETL cultural places Argentina').getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.sparkContext.setLogLevel("WARN")