import time
from string import Template
import datetime
import yaml
from dbstream import DBStream
from pydatasource.core.snippet import treat_all_snippet


class DataSource:
    def __init__(self, dbstream: DBStream, path_to_datasource_folder, schema_prefix=None):
        """

        :param dbstream:
        :param path_to_datasource_folder: absolute path containing the last '/'
        """
        self.dbstream = dbstream
        self.path_to_datasource_folder = path_to_datasource_folder
        self.schema_prefix = schema_prefix

    def _build_folder_path(self, layer_name):
        return self.path_to_datasource_folder + 'layers/' + layer_name + "/"

    def _create_view_gds(self, layer_name, table_name):
        pass

    def compute(self, layer_name, query_name=None):
        folder_path = self._build_folder_path(layer_name)
        config = yaml.load(open(folder_path + "config.yaml"), Loader=yaml.FullLoader)
        queries = config.get("query")
        schema_suffix = config.get("schema_suffix") if config.get("schema_suffix") else layer_name
        schema_name = self.schema_prefix + '_' + schema_suffix if self.schema_prefix else schema_suffix
        if not queries:
            print("No queries set up in config file")
            return 0
        query_list = [query_name] if query_name else list(queries.keys())
        for query in query_list:
            query_config = queries[query]
            if query_config.get("template"):
                query_template_file_name = query_config.get("template")
            else:
                query_template_file_name = query
            query_path = folder_path + "query/" + query_template_file_name + ".sql"
            query_params = query_config.get("query_params")
            query_create_view_gds = query_config.get('gds')
            query_template = Template(open(query_path).read())
            table_name = query
            dict_params = dict()
            dict_params["TABLE_NAME"] = "%s.%s" % (schema_name, table_name)
            dict_params["TABLE_NAME_TEMP"] = schema_name + "_" + table_name + "_temp"
            if query_params:
                for params in query_params.keys():
                    value = query_params[params]
                    if value == "now":
                        value = "'" + str(datetime.datetime.now())[:19] + "'"
                    dict_params.update({params.upper(): value})
            dict_params.update(treat_all_snippet(datasource_path=self.path_to_datasource_folder, query_path=query_path,
                                                 layer=layer_name, dict_params=dict_params))
            filled_query = query_template.substitute(dict_params)
            try:
                self.dbstream.execute_query(filled_query)
            except Exception as e:
                sec_before_retry = 15
                print(str(e))
                print("Retry in %s s..." % str(sec_before_retry))
                time.sleep(sec_before_retry)
                self.dbstream.execute_query(filled_query)
            print(dict_params["TABLE_NAME"] + " created")
            if query_create_view_gds:
                self._create_view_gds(layer_name, table_name)
        return 0

        pass

    def function_compute(self, layer_name):
        def f():
            self.compute(layer_name)

        return f
