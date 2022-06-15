import requests
from cfg import RUN_DATE, BUCKET_NAME
from spark import spark, dataframe
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Source:

    def __init__(self, url: str, name: str) -> None:
        """ Object which represents a Source

        Args:
            url (str): url where is the source information
            name (str): name of the source
        """
        self._url = url
        self.name = name
        self.filepath = f'{name}/{RUN_DATE.strftime("%Y-%B")}/{name}-{RUN_DATE.strftime("%d-%m-%Y")}.csv'
        self.df = None
        self.df_cleaned = None

    @property
    def content(self) -> bytes:
        """ Does a HTTP GET request to get the csv content
        Returns:
            bytes: CSV content represented in bytes
        """
        req = requests.get(self._url)
        return req.content

    def _load_df(self):
        """Loads the information in a Spark DataFrame from S3, based
        in the source url
        """
        logger.info(f'Loading {self.name} DataFrame from S3')
        self.df = spark.read.format('csv').option('header', True).option('inferSchema', True).csv(
            f's3a://{BUCKET_NAME}/{self.filepath}')

    def select_cleanded_df(self) -> dataframe:
        """Selects only the needed columns from the DataFrame

        Returns:
            dataframe: Spark DataFrame with only the needed columns
        """
        logger.info(f'Selecting only the necessary columns from {self.name}')
        cols_to_select = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoria', 'provincia',
                          'localidad', 'nombre', 'domicilio', 'codigo_postal', 'numero_de_telefono', 'mail', 'web']
        return self.df.select(cols_to_select)

    def _rename_columns(self):
        """
        Renames the columns to normalize the source information. The transformation is applied to 
        the source DataFrame
        """
        pass
    
    def transform(self):
        """ Transforms the source to normalize the information
        """


class Library(Source):

    def _rename_columns(self):
        logger.info(f'Renaming {self.name} the columns')
        cols_renamed = {
            'Cod_Loc': 'cod_localidad',
            'IdProvincia': 'id_provincia',
            'IdDepartamento': 'id_departamento',
            'Categoría': 'categoria',
            'Provincia': 'provincia',
            'Localidad': 'localidad',
            'Nombre': 'nombre',
            'Domicilio': 'domicilio',
            'CP': 'codigo_postal',
            'Teléfono': 'numero_de_telefono',
            'Mail': 'mail',
            'Web': 'web',
        }
        for old_col, new_col in cols_renamed.items():
            self.df = self.df.withColumnRenamed(old_col, new_col)

    def transform(self):
        self._load_df()
        self._rename_columns()


class Cinema(Source):
    def _rename_columns(self):
        logger.info(f'Renaming {self.name} the columns')
        cols_renamed = {
            'Cod_Loc': 'cod_localidad',
            'IdProvincia': 'id_provincia',
            'IdDepartamento': 'id_departamento',
            'Categoría': 'categoria',
            'Provincia': 'provincia',
            'Localidad': 'localidad',
            'Nombre': 'nombre',
            'Dirección': 'domicilio',
            'CP': 'codigo_postal',
            'Teléfono': 'numero_de_telefono',
            'Mail': 'mail',
            'Web': 'web',
        }
        for old_col, new_col in cols_renamed.items():
            self.df = self.df.withColumnRenamed(old_col, new_col)

    def transform(self):
        self._load_df()
        self._rename_columns()


class Museum(Source):
    def _rename_columns(self):
        logger.info(f'Renaming {self.name} the columns')
        cols_renamed = {
            'Cod_Loc': 'cod_localidad',
            'IdProvincia': 'id_provincia',
            'IdDepartamento': 'id_departamento',
            'direccion': 'domicilio',
            'CP': 'codigo_postal',
            'telefono': 'numero_de_telefono',
            'Mail': 'mail',
            'Web': 'web',
        }
        for old_col, new_col in cols_renamed.items():
            self.df = self.df.withColumnRenamed(old_col, new_col)

    def transform(self):
        self._load_df()
        self._rename_columns()
