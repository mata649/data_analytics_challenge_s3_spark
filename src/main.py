import logging
from cfg import BUCKET_NAME, s3, RUN_DATE, MUSEUMS_URL, CINEMAS_URL, LIBRARIES_URL
from load import load_cinema_insights, load_size_by_category, load_size_by_category_and_province, load_size_by_source, load_to_database
from source import Cinema, Library, Museum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
sources = {
    'museums': Museum(MUSEUMS_URL, 'museums'),
    'cinemas': Cinema(CINEMAS_URL, 'cinemas'),
    'libraries': Library(LIBRARIES_URL, 'libraries')
}


def extract():
    """
    Starts the extraction process for each source and loads the information to S3, 
    separating the information into different folders depending on the source, month, and year
    """
    logger.info('Starting extraction process')
    for name, source in sources.items():
        logger.info(f'Getting csv information {name}')
        content = source.content
        logger.info(f'Loading csv information in S3 for {name}')
        object = s3.Object(bucket_name=BUCKET_NAME, key=source.filepath)
        object.put(Body=content)


def transform():
    """
    Starts the transformation process in each source to normalize the source information
    """
    logger.info('Starting transformation process')
    for name, source in sources.items():
        logger.info(f'transforming {name}')
        source.transform()


def load():
    """Starts the upload process to AWS RDS for the different tables
    """
    logger.info('Starting load process')

    logger.info('Getting cleaned DataFrames')
    cinemas_df = sources['cinemas'].select_cleanded_df()
    museums_df = sources['museums'].select_cleanded_df()
    libraries_df = sources['libraries'].select_cleanded_df()
    cinemas_raw_df = sources['cinemas'].df

    logger.info('Unifying cleaned DataFrames')
    df_unified = cinemas_df.union(libraries_df).union(museums_df)

    load_to_database(df_unified, 'unified_table')
    load_size_by_source(
        {'cinemas': cinemas_df, 'museums': museums_df, 'libraries': libraries_df})
    load_size_by_category(df_unified)
    load_size_by_category_and_province(df_unified)
    load_cinema_insights(cinemas_raw_df)


def run():
    """ ETL to populate a database with cultural information about museums, cinemas and libraries from Argentina.
    """
    logger.info('Starting ETL process')
    extract()
    transform()
    load()


if __name__ == '__main__':
    run()
