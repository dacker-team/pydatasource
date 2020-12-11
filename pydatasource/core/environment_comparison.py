
METRIC_TYPES_LIST = ["integer", "int", "bigint", "double precision", "float", "numeric", "real", "tinyint", "int64"]


def launch_test(dbstream, schema_name, table, data_types, test_where_clause):
    columns_test_aggregate = []
    for c in data_types:
        if c["data_type"].lower() in METRIC_TYPES_LIST:
            columns_test_aggregate.append("SUM(%s) as %s" % (c["column_name"], c["column_name"]))

    query = """SELECT %s FROM %s.%s %s""" % (
        ",".join(columns_test_aggregate),
        schema_name,
        table,
        test_where_clause if test_where_clause else ""
    )
    result_values = dbstream.execute_query(query)
    return result_values

