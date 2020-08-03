from dbstream import DBStream
from tabulate import tabulate

METRIC_TYPES_LIST = ["integer", "int", "bigint", "double precision", "float", "numeric", "real", "tinyint"]


def launch_test(dbstream, schema_name, table, data_types):
    columns_test_aggregate = []
    for c in data_types:
        if c["data_type"] in METRIC_TYPES_LIST:
            columns_test_aggregate.append("SUM(%s) as %s" % (c["column_name"], c["column_name"]))

    query = """SELECT %s FROM %s.%s""" % (",".join(columns_test_aggregate), schema_name, table)
    result_values = dbstream.execute_query(query)
    return result_values


def compute_comparison(dbstream: DBStream, schema_name, tables_list, environment):
    print("=============================")
    print("ENVIRONMENTS COMPARISON RESULTS")
    print("=============================")
    for table in tables_list:
        print("Result of %s" % table)
        # Prod
        prod_data_types = dbstream.get_data_type(table_name=table, schema_name=schema_name)
        if prod_data_types is None:
            print("table %s is new" % table)
            print("=============================")
            continue
        prod_result_values = launch_test(dbstream, schema_name, table, prod_data_types)

        # Other Environment
        env_data_types = dbstream.get_data_type(table_name=table, schema_name=schema_name + "_" + environment)
        env_result_values = launch_test(dbstream, schema_name + "_" + environment, table, env_data_types)

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
