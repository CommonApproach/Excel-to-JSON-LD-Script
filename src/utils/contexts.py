import urllib.request
import re, warnings, json
def rename_prefixes(contexts, prefixes):
    for k, v in contexts.items():
        for kk,vv in v.items():
            vv_match = re.match(r'^(.+):(.+)$', vv)
            if vv_match:
                nm,val = vv_match.groups()
                if prefixes.get(nm):
                    expanded_nm = prefixes[nm]
                    v[kk] = expanded_nm+val
    return contexts

def load_context_mapping(path, replace_prefixes=False):
    context_dict = {}
    prefix_dict = {}
    with urllib.request.urlopen(path) as url:
        tmp_dict = json.load(url)
        if isinstance(tmp_dict['@context'], dict):
            tmp_entries = list(tmp_dict['@context'].items())
        elif isinstance(tmp_dict['@context'], list):
            tmp_entries = list(tmp_dict['@context'])
        for entry in tmp_entries:
            if isinstance(entry, dict):
                # procesisng a dictionary of values to merge
                for k,v in entry.items():
                    v['source_url'] = path
                entry_dict = {}
                for k,v in entry.items():
                    entry_dict[v.get('@id')] = [k,v]
                context_dict = dict(list(entry_dict.items()) + list(context_dict.items()))
            elif isinstance(entry, tuple) and isinstance(entry[1], str):
                # processing namespace prefixes to rename shortcuts
                prefix_dict = dict(list(dict([entry]).items()) + list(prefix_dict.items()))
            elif isinstance(entry, tuple) and isinstance(entry[1], dict):
                entry = dict([entry])
                for k,v in entry.items():
                    v['source_url'] = path
                entry_dict = {}
                for k,v in entry.items():
                    entry_dict[v.get('@id')] = [k,v]
                # processing a single entry to merge
                context_dict = dict(list(entry_dict.items()) + list(context_dict.items()))
            elif isinstance(entry, str):
                # processing a single entry to merge
                new_dict, new_prefix = load_context_mapping(path=entry, replace_prefixes=replace_prefixes)
                context_dict = dict(list(new_dict.items()) + list(context_dict.items()))
                prefix_dict = dict(list(new_prefix.items()) + list(prefix_dict.items()))
            else:
                warnings.warn(f"Found invalid context entry in {path}") # BB
                warnings.warn(f"Skipping context entry: {entry}") # BB
    if replace_prefixes:
        context_dict = rename_prefixes(contexts=context_dict, prefixes=prefix_dict)
    return context_dict, prefix_dict

