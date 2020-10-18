import re
from string import Template
from dacktool import log_info


def detect_snippet(datasource_instance, query_path):
    query = datasource_instance.load_file(query_path)
    m = re.findall(r"\$(.*?)( |$)", query)
    return [k[0] for k in m]


def treat_snippet(datasource_instance, layer, snippet):
    datasource_path = datasource_instance.path_to_datasource_folder
    snippet_name = (snippet.split('__')[0]).lower()
    path = datasource_path + 'layers/' + layer + '/sql_snippet/' + snippet_name + '.sql'
    try:
        snippet_template = Template(datasource_instance.load_file(path))
    except FileNotFoundError:
        path = datasource_path + 'sql_snippet/' + snippet_name + '.sql'
        snippet_template = Template(datasource_instance.load_file(path))
    snippet_str = datasource_instance.load_file(path)
    mi = re.findall(r"\$(.*?)( |$)", str(snippet_str))
    m = [k[0] for k in mi]
    dict_params = {}
    for i in range(len(m)):
        if len(snippet.split('__')) > 1:
            try:
                dict_params.update({m[i].upper(): "'" + snippet.split('__')[i + 1] + "'"})
            except IndexError:
                log_info('No params specify in query')
                break
        else:
            log_info(m[i].upper())
            dict_params.update({
                m[i].upper(): treat_snippet(
                    datasource_instance=datasource_instance,
                    layer=layer,
                    snippet=m[i].upper()
                )
            })
    return snippet_template.substitute(dict_params)


def treat_all_snippet(datasource_instance, query_path, layer, dict_params):
    dict_result = {}
    for snippet in detect_snippet(datasource_instance=datasource_instance, query_path=query_path):
        if snippet in dict_params or "." in snippet or "[0" in snippet:
            continue
        snippet_code = treat_snippet(datasource_instance=datasource_instance, layer=layer, snippet=snippet)
        dict_result.update({snippet: snippet_code})
    return dict_result
