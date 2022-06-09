from cfg import REDSHIFT_URL, TEMP_DATA_DIR

def load_to_redshift(df, table_name): 
    df.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", REDSHIFT_URL) \
        .option("dbtable", table_name) \
        .option("tempdir", TEMP_DATA_DIR) \
        .option("forward_spark_s3_credentials", True)\
        .mode("error").save()