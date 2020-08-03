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

