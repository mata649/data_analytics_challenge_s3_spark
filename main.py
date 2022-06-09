import logging
from cfg import BUCKET_NAME, s3, RUN_DATE, MUSEUMS_URL, CINEMAS_URL, LIBRARIES_URL
from load import load_to_redshift
from source import Cinema, Library, Museum
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
sources = {
    'museums': Museum(MUSEUMS_URL, 'museums'),
    'cinemas': Cinema(CINEMAS_URL, 'cinemas'),
    'libraries': Library(LIBRARIES_URL, 'libraries')
}

def extract():
    logger.info('Starting extraction process')
    for name, source  in sources.items():
        logger.info(f'Getting csv information {name}')
        content = source.content
        logger.info(f'Loading csv information in S3 for {name}')
        object = s3.Object(bucket_name=BUCKET_NAME, key=source.filepath)
        object.put(Body=content)

def transform():
    logger.info('Starting transformation process')
    for name, source in sources.items():
        logger.info(f'transforming {name}')
        source.transform()

def load():
    logger.info('Starting load process')
    
    logger.info('Getting cleaned DataFrames')
    cinemas_df = sources['cinemas'].select_cleanded_df()
    museums_df = sources['museums'].select_cleanded_df()
    libraries_df = sources['libraries'].select_cleanded_df()
    cinemas_raw_df = sources['cinemas'].df
    
    logger.info('Unifying cleaned DataFrames')
    df_unified = cinemas_df.union(libraries_df).union(museums_df)

    logger.info('Loading unified DataFrame')
    load_to_redshift(df_unified, 'unified_table')

    



def run():
    """ ETL to populate a database with cultural information about museums, cinemas and libraries from Argentina.
    """
    logger.info('Starting ETL process')
    extract()
    transform()
    load()

if __name__ == '__main__':
    run()