import logging
from typing import List
from spark import spark, StructType, StructField, StringType, IntegerType, sum, count, dataframe

from cfg import DB_URL, DB_USER, DB_PASSWORD

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_to_database(df: dataframe, table_name: str):
    """Load the DataFrame to AWS RDS

    Args:
        df (dataframe): DataFrame that will be loaded to AWS RDS
        table_name (str): Table name in the database where the information will be loaded
    """
    logger.info(f'Loading {table_name} DataFrame')
    df.write \
        .format('jdbc') \
        .option('url', DB_URL) \
        .option('dbtable', table_name)\
        .option('user', DB_USER)\
        .option('password', DB_PASSWORD)\
        .mode('overwrite')\
        .save()


def drop_null(subset: List, df: dataframe) -> dataframe:
    """Drops the nulls values in the selected columns

    Args:
        subset (List): List of columns where the nulls values will be deleted
        df (dataframe): DataFrame where the transformation will be applied

    Returns:
        dataframe: Returns a new DataFrame with the normalization applied
    """
    return df.na.drop(subset=subset)


def load_size_by_source(sources: List):
    """Gets the size by source and load the information to AWS RDS

    Args:
        sources (List[dataframe]): List of DataFrames to get the size by source
    """
    df_size_by_source_schema = StructType(fields=[
        StructField(name='source', dataType=StringType(), nullable=False),
        StructField(name='size', dataType=IntegerType(), nullable=False),
    ])
    df_size_by_source = spark.createDataFrame(
        data=[], schema=df_size_by_source_schema)

    for name, source in sources.items():
        df_size_by_source = df_size_by_source.union(
            spark.createDataFrame([(f'records per {name}', source.count())]))
    load_to_database(df_size_by_source, 'size_by_source')


def load_size_by_category(df: dataframe):
    """Gets the size by category and load the information to AWS RDS

    Args:
        df (dataframe): DataFrame to get the size by category
    """
    df_size_by_category = df.groupBy('categoria').count()
    df_size_by_category = drop_null(['categoria'], df_size_by_category)
    load_to_database(df_size_by_category, 'size_by_category')


def load_size_by_category_and_province(df: dataframe):
    """
    Gets the size by category and province and loads the information to AWS RDS
    Args:
        df (dataframe): DataFrame to get the size by category and province
    """
    df_size_by_category_and_province = df.groupBy(
        ['categoria', 'provincia']).count()
    df_size_by_category_and_province = drop_null(
        ['categoria'], df_size_by_category_and_province)
    load_to_database(df_size_by_category_and_province,
                     'size_by_category_and_province')


def load_cinema_insights(df_cinemas_raw: dataframe):
    """
    Gets the cinema insights and load the information to AWS RDS
    Args:
        df_cinemas_raw (dataframe): DataFrame to get the cinema insights
    """
    df_cinemas_new = df_cinemas_raw.select(
        ['provincia', 'Pantallas', 'Butacas', 'espacio_INCAA'])

    df_cinema_insights = df_cinemas_new.groupBy('provincia').agg(
        sum('Pantallas').alias('Cantidad de Pantallas'),
        sum('Butacas').alias('Cantidad de Butacas'),
        count('espacio_INCAA').alias('Cantidad de espacios INCAA')
    ).orderBy('provincia')
    df_cinema_insights = drop_null(['provincia'], df_cinema_insights)
    load_to_database(df_cinema_insights, 'cinema_insights')
