import os

from pydatasource import DataSource

from pyzure import AzureDBStream

azure_dbstream = AzureDBStream("MH", client_id=1)


path_to_datasource_folder = os.environ["ORIGIN_PATH"] + 'humanitas/datasource/'

humanitas_datasource = DataSource(dbstream=azure_dbstream,
                                               path_to_datasource_folder=path_to_datasource_folder,
                                               schema_prefix='humanitas')
