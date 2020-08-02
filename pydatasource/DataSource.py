import time
from string import Template
import datetime
import yaml
from dbstream import DBStream
from pydatasource.core.documentation import document_treat_query
from pydatasource.core.snippet import treat_all_snippet
from dacktool import log_info, log_error

from pydatasource.core.staging_comparison import compute_staging_comparison


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

    def _create_beautiful_view(self, table_name, schema_name):

        result = self.dbstream.get_data_type(table_name=table_name, schema_name=schema_name)

        columns_list = []
        for c in result:
            columns_list.append(""" "%s" as "%s" """ % (c["column_name"], c["column_name"].replace("_", " ")))

        view_name = '%s.%s_%s' % (schema_name, table_name, 'beautiful')
        columns = ','.join(columns_list)
        self.dbstream.create_view_from_columns(view_name, columns, schema_name, table_name)
        log_info(view_name + " view created")

    def _create_gds_view(self, table_name, schema_name):

        result = self.dbstream.get_data_type(table_name=table_name, schema_name=schema_name)

        columns_list = []
        for c in result:
            if c["data_type"] in ('timestamp without time zone',):
                columns_list.append("TO_CHAR(%s,'YYYYMMDD') as %s" % (c["column_name"], c["column_name"]))
            else:
                columns_list.append(c["column_name"])

        view_name = '%s.%s_%s' % (schema_name, table_name, 'gds')
        columns = ','.join(columns_list)
        self.dbstream.create_view_from_columns(view_name, columns, schema_name, table_name)
        log_info(view_name + " view created")

    def _get_query_list(self, layer_name, query_name=None):
        folder_path = self._build_folder_path(layer_name)
        config = yaml.load(open(folder_path + "config.yaml"), Loader=yaml.FullLoader)
        queries = config.get("queries")
        schema_name = config.get("schema_name") if config.get("schema_name") else ("datasource_" + layer_name)
        schema_name = self.schema_prefix + '_' + schema_name if self.schema_prefix else schema_name
        if not queries:
            log_info("No queries set up in config file")
            return 0
        query_list = [query_name] if query_name else list(queries.keys())
        return query_list, queries, schema_name, folder_path

    def _filled_query(self, queries, query, folder_path, schema_name, layer_name, staging):
        query_config = queries[query]
        if query_config.get("template"):
            query_template_file_name = query_config.get("template")
        else:
            query_template_file_name = query
        query_path = folder_path + "query/" + query_template_file_name + ".sql"
        query_params = query_config.get("query_params")
        query_template = Template(open(query_path).read())
        table_name = query
        dict_params = dict()
        dict_params["TABLE_NAME"] = "%s.%s" % (schema_name, table_name)
        dict_params["TABLE_NAME_TEMP"] = "%s_%s_temp" % (schema_name, table_name)

        if query_params:
            for params in query_params.keys():
                value = query_params[params]
                if isinstance(value, dict):
                    if staging:
                        value = value["staging"]
                    else:
                        value = value["production"]
                if value == "now":
                    value = str(datetime.datetime.now())[:10]
                dict_params.update({params.upper(): value})
        dict_params.update(
            treat_all_snippet(
                datasource_path=self.path_to_datasource_folder,
                query_path=query_path,
                layer=layer_name,
                dict_params=dict_params)
        )
        filled_query = query_template.substitute(dict_params)
        return filled_query

    def compute(self, layer_name, query_name=None, staging=False):
        query_list, queries, schema_name, folder_path = self._get_query_list(layer_name, query_name=query_name)

        for query in query_list:
            log_info("Layer: %s | Query started: %s | Staging mode: %s" % (layer_name, query, staging))
            filled_query = self._filled_query(
                queries=queries,
                query=query,
                folder_path=folder_path,
                schema_name=schema_name if not staging else (schema_name + "_staging"),
                layer_name=layer_name,
                staging=staging
            )
            table_name = query

            try:
                self.dbstream.execute_query(filled_query)
            except Exception as e:
                if "schema" in str(e):
                    self.dbstream.create_schema(schema_name if not staging else (schema_name + "_staging"))
                    self.dbstream.execute_query(filled_query)
                else:
                    sec_before_retry = 15
                    log_error(str(e))
                    log_info("Retry in %s s..." % str(sec_before_retry))
                    time.sleep(sec_before_retry)
                    self.dbstream.execute_query(filled_query)

            # LOG NORMAL
            if not staging:
                log_info(table_name + " created")

            if not staging and queries[query].get("beautiful_view"):
                self._create_beautiful_view(table_name, schema_name)
            if not staging and queries[query].get("gds"):
                self._create_gds_view(table_name, schema_name)

        if staging:
            compute_staging_comparison(self.dbstream, schema_name, query_list)

        return 0

    def function_compute(self, layer_name, staging=False):
        def f():
            self.compute(layer_name, staging)

        return f

    def print_filled_query(self, layer_name, query_name=None):
        query_list, queries, schema_name, folder_path = self._get_query_list(layer_name, query_name=query_name)
        for query in query_list:
            filled_query, dict_params, table_name = self._filled_query(queries, query,
                                                                       folder_path, schema_name,
                                                                       layer_name)
            with open('sandbox_' + layer_name + '_' + query_name + '.sql', 'w') as f:
                f.write(filled_query)

    def doc(self, layer_name, query_name=None):
        r = []
        query_list, queries, schema_name, folder_path = self._get_query_list(layer_name, query_name=query_name)
        for query in query_list:
            filled_query, dict_params, table_name = self._filled_query(queries, query,
                                                                       folder_path,
                                                                       schema_name,
                                                                       layer_name)

            r = r + document_treat_query(filled_query, schema_name, table_name)
        return r
