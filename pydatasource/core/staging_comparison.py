from dbstream import DBStream
from dacktool import log_info
from tabulate import tabulate


def launch_test(dbstream, schema_name, table, data_types):
    columns_test_aggregate = []
    for c in data_types:
        if c["data_type"] in ["integer", "bigint", "double precision"]:
            columns_test_aggregate.append("SUM(%s) as %s" % (c["column_name"], c["column_name"]))

    query = """SELECT %s FROM %s.%s""" % (",".join(columns_test_aggregate), schema_name, table)
    result_values = dbstream.execute_query(query)
    return result_values


def compute_staging_comparison(dbstream: DBStream, schema_name, tables_list):
    print("=============================")
    print("STAGING COMPARISON RESULTS")
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

        # Staging
        stg_data_types = dbstream.get_data_type(table_name=table, schema_name=schema_name + "_staging")
        stg_result_values = launch_test(dbstream, schema_name + "_staging", table, stg_data_types)

        main_dict = {}
        for p in prod_result_values[0].keys():
            main_dict[p] = {"prod_value": prod_result_values[0][p]}

        for s in stg_result_values[0].keys():
            if main_dict.get(s):
                main_dict[s]["stg_value"] = stg_result_values[0][s]
            else:
                main_dict[s] = {"stg_value": stg_result_values[0][s]}

        headers = ["Metric", "Prod", "Staging"]
        values = []
        for key in main_dict:
            val = [key]
            if main_dict[key].get("prod_value"):
                val.append(main_dict[key].get("prod_value"))
            else:
                val.append("===NEW METRIC===")
            if main_dict[key].get("stg_value") and main_dict[key].get("prod_value"):
                diff = main_dict[key].get("stg_value") - main_dict[key].get("prod_value")
                if diff == 0:
                    val.append("OK SAME VALUE")
                else:
                    val.append("DIFF (stg-prod) : %s" % str(diff))
            elif main_dict[key].get("stg_value"):
                val.append(main_dict[key].get("stg_value"))
            else:
                val.append("===DISAPPEARED===")
            values.append(val)

        print(tabulate(values, headers=headers, tablefmt="fancy_grid", floatfmt=".2f"))
        print("=============================")
