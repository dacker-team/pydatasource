import re
import time
from string import Template
import datetime
import yaml
from dbstream import DBStream
from pydatasource.core.snippet import treat_all_snippet


def _doc_treat_query(filled_query, schema_name, table_name):
    filled_query = filled_query.replace('\n', ' ')
    a = re.split('from | join ', filled_query.lower())
    all_tuple = []
    for i in a[1:]:
        aa = i.split(' ')
        n = aa[0]
        if '.' in n:
            all_tuple.append({'schema_output': schema_name, 'table_output': table_name,
                              'schema_input': n.split('.')[0].replace(')', ''),
                              'table_input': n.split('.')[1].replace(')', ''),
                              "schema_table_output": schema_name + '.' + table_name,
                              "schema_table_input": n.replace(')', '')})
    return all_tuple


class DataSource:
    def __init__(self, dbstream: DBStream, path_to_datasource_folder, schema_prefix=None, config_name='config'):
        """

        :param dbstream:
        :param path_to_datasource_folder: absolute path containing the last '/'
        """
        self.dbstream = dbstream
        self.path_to_datasource_folder = path_to_datasource_folder
        self.schema_prefix = schema_prefix
        self.config_name = config_name

    def _build_folder_path(self, layer_name):
        return self.path_to_datasource_folder + 'layers/' + layer_name + "/"

    def _create_redshift_beautiful_view(self, table_name, schema_name):
        query = '''
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name='%s' and table_schema='%s'
            ''' % (table_name, schema_name)
        result = self.dbstream.execute_query(query)
        columns_list = [""" "%s" as "%s" """ % (c["column_name"], c["column_name"].replace("_", " ")) for c in result]
        view_name = '%s.%s_%s' % (schema_name, table_name, 'beautiful')
        columns = ','.join(columns_list)
        view_query = '''DROP VIEW IF EXISTS %s ;CREATE VIEW %s as (SELECT %s FROM %s.%s)''' \
                     % (view_name, view_name, columns, schema_name, table_name)
        self.dbstream.execute_query(view_query)

    def _create_gds_view(self, table_name, schema_name):
        query = '''
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name='%s' and table_schema='%s'
        ''' % (table_name, schema_name)
        result = self.dbstream.execute_query(query)
        columns_list = []
        for c in result:
            if c["data_type"] in ('timestamp without time zone',):
                columns_list.append("TO_CHAR(%s,'YYYYMMDD') as %s" % (c["column_name"], c["column_name"]))
            else:
                columns_list.append(c["column_name"])
        view_name = '%s.%s_%s' % (schema_name, table_name, 'gds')
        columns = ','.join(columns_list)
        view_query = '''DROP VIEW IF EXISTS %s ;CREATE VIEW %s as (SELECT %s FROM %s.%s)''' \
                     % (view_name, view_name, columns, schema_name, table_name)
        self.dbstream.execute_query(view_query)
        print(view_name + " view created")

    def _get_query_list(self, layer_name, query_name=None):
        folder_path = self._build_folder_path(layer_name)
        config = yaml.load(open(folder_path + self.config_name + ".yaml"), Loader=yaml.FullLoader)
        queries = config.get("query")
        schema_suffix = config.get("schema_suffix") if config.get("schema_suffix") else layer_name
        schema_name = self.schema_prefix + '_' + schema_suffix if self.schema_prefix else schema_suffix
        if not queries:
            print("No queries set up in config file")
            return 0
        query_list = [query_name] if query_name else list(queries.keys())
        return query_list, queries, schema_name, folder_path

    def _filled_query(self, queries, query, folder_path, schema_name, layer_name):
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
        dict_params["TABLE_NAME_TEMP"] = schema_name + "_" + table_name + "_temp"
        if query_params:
            for params in query_params.keys():
                value = query_params[params]
                if value == "now":
                    value = str(datetime.datetime.now())[:10]
                dict_params.update({params.upper(): value})
        dict_params.update(treat_all_snippet(datasource_path=self.path_to_datasource_folder, query_path=query_path,
                                             layer=layer_name, dict_params=dict_params))
        filled_query = query_template.substitute(dict_params)
        return filled_query, dict_params, table_name

    def compute(self, layer_name, query_name=None):
        query_list, queries, schema_name, folder_path = self._get_query_list(layer_name, query_name=query_name)
        for query in query_list:
            filled_query, dict_params, table_name = self._filled_query(queries, query,
                                                                       folder_path, schema_name,
                                                                       layer_name)
            try:
                self.dbstream.execute_query(filled_query)
            except Exception as e:
                sec_before_retry = 15
                print(str(e))
                print("Retry in %s s..." % str(sec_before_retry))
                time.sleep(sec_before_retry)
                self.dbstream.execute_query(filled_query)

            print(dict_params["TABLE_NAME"] + " created")
            if queries[query].get("redshift_beautiful_view"):
                self._create_redshift_beautiful_view(table_name, schema_name)
            if queries[query].get("gds"):
                self._create_gds_view(table_name, schema_name)
        return 0

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

            r = r + _doc_treat_query(filled_query, schema_name, table_name)
        return r

    pass

    def function_compute(self, layer_name):
        def f():
            self.compute(layer_name)

        return f
