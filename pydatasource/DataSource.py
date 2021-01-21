import time
import datetime

import jinja2
import yaml
from dbstream import DBStream
from tabulate import tabulate

from pydatasource.core.documentation import document_treat_query
from pydatasource.core.snippet import treat_all_snippet
from dacktool import log_info, log_error

from pydatasource.core.environment_comparison import launch_test


def get_destination_tables_with_schema(query_config, query_name, schema_name, environment):
    result = {"production": schema_name + "." + query_name}
    if environment != "production":
        result[environment] = schema_name + "_" + environment + "." + query_name
    if query_config.get("query_params"):
        table_destination = query_config.get("query_params").get("table_destination")
        if table_destination:
            if environment == "production":
                if isinstance(table_destination, dict):
                    result["production"] = table_destination["production"]
                else:
                    result["production"] = table_destination
            else:
                result["production"] = table_destination["production"]
                result[environment] = table_destination[environment]
    return result


class DataSource:
    def __init__(self, dbstream: DBStream, path_to_datasource_folder,
                 schema_prefix=None,
                 layer_type='datasource',
                 loader_function=None):
        """

        :param dbstream:
        :param path_to_datasource_folder: absolute path containing the last '/'
        """
        self.dbstream = dbstream
        self.path_to_datasource_folder = path_to_datasource_folder
        self.schema_prefix = schema_prefix
        self.layer_type = layer_type
        self.loader_function = loader_function
        self.jinja_env = jinja2.Environment()

    def _build_folder_path(self, layer_name):
        return self.path_to_datasource_folder + 'layers/' + layer_name + "/"

    def load_file(self, path):
        if self.loader_function is None:
            return open(path).read()
        return self.loader_function(path)

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
        config = yaml.load(self.load_file(folder_path + "config.yaml"), Loader=yaml.FullLoader)
        queries = config.get("queries")
        schema_name = config.get("schema_name") if config.get("schema_name") else (self.layer_type + "_" + layer_name)
        schema_name = self.schema_prefix + '_' + schema_name if self.schema_prefix else schema_name
        if not queries:
            log_info("No queries set up in config file")
            return 0
        query_list = [query_name] if query_name else list(queries.keys())
        return query_list, queries, schema_name, folder_path

    def _filled_query(self, query_config, query, folder_path, schema_name, layer_name, environment="production"):
        if query_config.get("template"):
            query_template_file_name = query_config.get("template")
        else:
            query_template_file_name = query
        query_path = folder_path + "query/" + query_template_file_name + ".sql"
        query_params = query_config.get("query_params")
        table_name = query
        dict_params = dict()
        dict_params["table_name"] = "%s.%s" % (schema_name, table_name)
        dict_params["table_name_temp"] = "%s_%s_temp" % (schema_name, table_name)
        if query_params:
            for params in query_params.keys():
                value = query_params[params]
                if isinstance(value, dict):
                    if environment != 'production':
                        value = value[environment]
                    else:
                        value = value["production"]
                if value == "now":
                    value = "'" + str(datetime.datetime.now())[:10] + "'"
                dict_params.update({params: value})
        dict_params.update(
            treat_all_snippet(
                datasource_instance=self,
                query_path=query_path,
                layer=layer_name,
                dict_params=dict_params)
        )
        template = self.jinja_env.from_string(self.load_file(query_path))
        return template.render(dict_params)

    def compute(self,
                layer_name,
                query_name=None,
                environment="production",
                comparison_test=True,
                return_result=False):
        query_list, queries_config, schema_name, folder_path = self._get_query_list(
            layer_name=layer_name,
            query_name=query_name
        )
        result_dict = {}
        result_dict[layer_name] = {}
        for query in query_list:
            result_dict[layer_name][query] = {}
            result_dict[layer_name][query]["started_at"] = str(datetime.datetime.now())
            log_info("Layer: %s | Query started: %s | Environment: %s" % (layer_name, query, environment))
            query_config = queries_config[query]
            filled_query = self._filled_query(
                query_config=query_config,
                query=query,
                folder_path=folder_path,
                schema_name=schema_name if environment == 'production' else (schema_name + "_" + environment),
                layer_name=layer_name,
                environment=environment
            )
            destination_tables_with_schema = get_destination_tables_with_schema(
                schema_name=schema_name,
                query_config=query_config,
                query_name=query,
                environment=environment
            )
            if environment != "production":
                log_info(filled_query)
            try:
                query_result = self.dbstream.execute_query(filled_query)
            except Exception as e:
                if "schema" in str(e).lower() or "dataset" in str(e).lower():
                    self.dbstream.create_schema(destination_tables_with_schema[environment].split(".")[0])
                    query_result = self.dbstream.execute_query(filled_query)
                else:
                    sec_before_retry = 15
                    log_error(str(e))
                    log_info("Retry in %s s..." % str(sec_before_retry))
                    time.sleep(sec_before_retry)
                    query_result = self.dbstream.execute_query(filled_query)

            # LOG NORMAL
            if environment == 'production':
                log_info(destination_tables_with_schema["production"] + " created")

            if environment == 'production' and queries_config[query].get("beautiful_view"):
                self._create_beautiful_view(
                    schema_name=destination_tables_with_schema["production"].split(".")[0],
                    table_name=destination_tables_with_schema["production"].split(".")[1],
                )
            if environment == 'production' and queries_config[query].get("gds"):
                self._create_gds_view(
                    schema_name=destination_tables_with_schema["production"].split(".")[0],
                    table_name=destination_tables_with_schema["production"].split(".")[1],
                )

            if return_result:
                if query_config.get("result_type") == "values":
                    if not query_result:
                        result_dict[layer_name][query]["data"] = {}
                    else:
                        result_dict[layer_name][query]["data"] = query_result[0]
                else:
                    if not query_result:
                        result_dict[layer_name][query]["data"] = []
                    else:
                        result_dict[layer_name][query]["data"] = query_result
            result_dict[layer_name][query]["ended_at"] = str(datetime.datetime.now())

        if environment != 'production' and comparison_test:
            self.compute_comparison(
                schema_name=schema_name,
                tables_list=query_list,
                queries_config=queries_config,
                environment=environment)

        return result_dict

    def compute_comparison(self, schema_name, tables_list, environment, queries_config):
        print("=============================")
        print("ENVIRONMENTS COMPARISON RESULTS")
        print("=============================")
        for table in tables_list:
            query_config = queries_config[table]
            destination_tables_with_schema = get_destination_tables_with_schema(
                schema_name=schema_name,
                query_config=query_config,
                query_name=table,
                environment=environment
            )

            print("Result of %s" % destination_tables_with_schema[environment])

            test_where_clause = queries_config[table].get("test_where_clause")

            # Prod
            prod_data_types = self.dbstream.get_data_type(
                table_name=destination_tables_with_schema["production"].split(".")[1],
                schema_name=destination_tables_with_schema["production"].split(".")[0]
            )
            if prod_data_types is None:
                print("table %s is new" % destination_tables_with_schema["production"])
                print("=============================")
                continue
            prod_result_values = launch_test(
                dbstream=self.dbstream,
                schema_name=destination_tables_with_schema["production"].split(".")[0],
                table=destination_tables_with_schema["production"].split(".")[1],
                data_types=prod_data_types,
                test_where_clause=test_where_clause
            )

            # Other Environment
            env_data_types = self.dbstream.get_data_type(
                table_name=destination_tables_with_schema[environment].split(".")[1],
                schema_name=destination_tables_with_schema[environment].split(".")[0]
            )
            env_result_values = launch_test(
                dbstream=self.dbstream,
                schema_name=destination_tables_with_schema[environment].split(".")[0],
                table=destination_tables_with_schema[environment].split(".")[1],
                data_types=env_data_types,
                test_where_clause=test_where_clause
            )

            main_dict = {}
            for p in prod_result_values[0].keys():
                main_dict[p] = {"prod_value": prod_result_values[0][p]}

            for s in env_result_values[0].keys():
                if main_dict.get(s):
                    main_dict[s]["env_value"] = env_result_values[0][s]
                else:
                    main_dict[s] = {"env_value": env_result_values[0][s]}

            headers = ["Metric", "Prod", environment]
            values = []
            for key in main_dict:
                val = [key]
                if main_dict[key].get("prod_value"):
                    val.append(main_dict[key].get("prod_value"))
                else:
                    val.append("===NEW METRIC===")
                if main_dict[key].get("env_value") and main_dict[key].get("prod_value"):
                    diff = main_dict[key].get("env_value") - main_dict[key].get("prod_value")
                    if diff == 0:
                        val.append("OK SAME VALUE")
                    else:
                        val.append("DIFF (env-prod) : %s" % str(diff))
                elif main_dict[key].get("env_value"):
                    val.append(main_dict[key].get("env_value"))
                else:
                    val.append("===DISAPPEARED===")
                values.append(val)

            print(tabulate(values, headers=headers, tablefmt="fancy_grid", floatfmt=".2f"))
            print("=============================")

    def function_compute(self, layer_name, environment='production', comparison_test=True):
        def f():
            self.compute(layer_name=layer_name, environment=environment, comparison_test=comparison_test)

        return f

    def print_filled_query(self, layer_name, query_name=None, environment="production"):
        query_list, queries, schema_name, folder_path = self._get_query_list(layer_name, query_name=query_name)
        for query in query_list:
            filled_query = self._filled_query(
                query_config=query_config,
                query=query,
                folder_path=folder_path,
                schema_name=schema_name,
                layer_name=layer_name,
                environment=environment
            )
            with open('sandbox_' + layer_name + '_' + query_name + '.sql', 'w') as f:
                f.write(filled_query)

    def doc(self, layer_name, query_name=None, environment="production"):
        r = []
        query_list, queries_config, schema_name, folder_path = self._get_query_list(layer_name, query_name=query_name)
        for query in query_list:
            query_config = queries_config[query]
            filled_query = self._filled_query(
                query_config=query_config,
                query=query,
                folder_path=folder_path,
                schema_name=schema_name,
                layer_name=layer_name,
                environment=environment
            )
            r = r + document_treat_query(
                filled_query=filled_query,
                schema_name=schema_name,
                table_name=get_destination_tables_with_schema(
                    query_config=query_config,
                    query_name=query,
                    schema_name=schema_name,
                    environment=environment
                ))
        return r
