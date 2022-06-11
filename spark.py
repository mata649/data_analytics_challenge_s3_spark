import findspark
findspark.init()
from pyspark.sql import SparkSession
from cfg import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

spark = SparkSession.builder.appName('ETL cultural places Argentina').getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
