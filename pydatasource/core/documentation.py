import re


def document_treat_query(filled_query, schema_name, table_name):
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
