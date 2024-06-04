import csv, re, datetime, collections, pickle, tqdm, unicodedata, io, os, math
from operator import truediv
import datetime, json, urllib
from copy import copy
import warnings
from urllib.parse import urlparse
import urllib.request

from owlready2 import Thing, ThingClass, DataPropertyClass 
from .namespaces import PREFIX, BASE_PREFIX, TURTLE_PREAMBLE, namespaces, prop_ranges_preset, cids
# importing namespaces as variables allows them to be used as owlready2 variables, with additional validation
exec(f"from .namespaces import {','.join([nm for nm in namespaces.keys() if nm != ''])}")

import uuid
import collections

from __main__ import LOG_FILE, CONTEXT_PATH, MAPPING_CONTEXT, REPLACE_PREFIX

# global variable that stores the working dict of data.
# It is used in place of the owlready2 sqllite3, as its more efficient.
global_db = {}
global_db_index_names = {}
global_db_indexed = collections.defaultdict(lambda:{})

def logger(text, filename=LOG_FILE):
    """
    logs errors and warnings to the file at filename
    :param text : string value of text to write to log
    :param filename : string value of log's filename
    """
    # Open a file with access mode 'a'
    file_object = open(filename, 'a')
    # Append 'hello' at the end of file
    file_object.write(str(datetime.datetime.today()) + "\t" + str(text) + "\n")
    # Close the file
    file_object.close()

def escape_str(s: str, lower=True):
    """
    Generate an individual/class name that complies the ontology format.
    :param s : string value of string to escape.
    :param lower : whetehr to convert to lower case or not.
    :return s : formatted string
    """
    if lower:
        s = s.lower()
    # string.replace('+', '_plus_')
    s.replace('<', '_lt_')
    s.replace('>', '_gt_')
    s = re.sub(r'[^-_0-9a-zA-Z]', '_', s)
    return re.sub(r'_-_|-_-|_+', '_', s)


def is_bom(file_path):
    """
    check if file contains BOM characters.
    :param file_path: filename of file to check BOM for
    :return True/False whetehr fiel is BOM formatted.
    """
    f = open(file_path, mode='r', encoding='utf-8')
    chars = f.read(4)[0]
    f.close()
    return chars[0] == '\ufeff'


def read_csv(csv_path: str, encoding=None):
    """
    read CSV file, with error handling.
    :param csv_path : string with csv file path
    :param encoding : string with file nencoding to use, if any
    :return data : list of data read from CSV file
    """
    data = []
    if not encoding:
        encoding = 'utf-8-sig' if is_bom(csv_path) else 'utf-8'
    print(f'Loaded CSV "{csv_path}"; Encoding: {encoding}')
    with open(csv_path, encoding=encoding, newline='') as file:
        reader = csv.DictReader(file, quotechar='"', delimiter=',',  quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            data.append(row)
    return data


def write_csv(csv_path, data: list):
    """
    write CSV file
    :param csv_path : string with CSV file name to write to
    :param data : list with records to wrote to CSV file
    """
    with open(csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

def format_strptime(d):
    """
    parse a date string into formatted Timestamp value
    :param d : string with date value to format
    :return : string formatted as Timestamp type
    """
    try:
        return datetime.datetime.strptime(d, "%Y-%m-%d 00:00:00")
    except ValueError:
        try:
            return datetime.datetime.strptime(d, "%Y-%m-%d 00:00")
        except ValueError:
            return datetime.datetime.strptime(d, "%Y-%m-%d")


###########################################################
# Methods for handling instance creation
# Uses internal glbol_db dict for storage
###########################################################
def get_instance_label(klass, uuid_inst=None):
    """
    return instance label with class and UUID
    :param klass: type of instance
    :param uuid_inst: uuid instance, e.g. from uuid.uuid4()
    :return string value of instance label
    """
    # if uuid_inst is None:
    #     warnings.warn(f"No ID provided: {klass}") # BB
    return None

def build_index(batch_count=os.cpu_count()):
    print('Building global_db index...')
    global global_db_index_names
    global global_db_indexed
    from concurrent.futures import ThreadPoolExecutor
    props = global_db_index_names.copy()

    search_args = []
    batch_size = len(global_db.keys())//batch_count
    for i,chunk in tqdm.tqdm(enumerate(chunked_dict(global_db.items(), batch_size)), leave=False, desc='loading...', total=batch_count):
        search_args.append([props, i,chunk])

    with ThreadPoolExecutor() as p:
        res_sub = p.map(lambda args, f=build_index_raw: f(*args), search_args)
    res = list(res_sub)
    global_db_indexed = {}
    for prop in props:
        global_db_indexed[prop] = ObjectDict(lambda:PropertyList())

    for d in res:
        for k,v in d.items():
            global_db_indexed[k].update(v)

def build_index_raw(props, i,sub_global_db):
    global_db_index_tmp = {}
    for prop in props:
        global_db_index_tmp[prop] = ObjectDict(lambda:PropertyList())

    for prop in props:
        for inst_id,inst in tqdm.tqdm(sub_global_db, leave=False, desc=f"build {prop} idx({i}"):
            if prop in inst.keys():
                found = None
                if isinstance(inst[prop],(PropertyList, list)):
                    found = inst[prop]
                else:
                    found = [inst[prop]]
                if found:
                    for v in found:
                        global_db_index_tmp[prop][v].append(inst_id)
    
    return global_db_index_tmp



def search_indexed_instances(klass=None, props={}, how='all'):
    global global_db_indexed
    ps = dict([(p,v) for p,v in props.items() if p in global_db_indexed.keys()])
    ps_other = dict([(p,v) for p,v in props.items() if p not in global_db_indexed.keys()])
    inst_ids = set()
    for prop,v in ps.items():
        if len(inst_ids) == 0:
            inst_ids = set(global_db_indexed[prop][v])
        else:
            inst_ids_tmp = set(global_db_indexed[prop][v])
            inst_ids = inst_ids.intersection(inst_ids_tmp)

    if how == 'first' and len(inst_ids)>0:
        return get_instance(inst_id=list(inst_ids)[0])
    elif how == 'all' and len(ps_other)>0:
        sub_global_db = dict([(k,v) for k,v in global_db.items() if k in inst_ids])
        return search_instances(klass=klass, props=ps_other, sub_global_db=sub_global_db, how=how)


def search_instances(klass=None, nm=PREFIX, props={}, how='all', sub_global_db = {}):
    """ 
    return instances based on criteria in klass and props
    if klass is passed, the search is faster
    if single property in props is passed, the search is faster
    """
    if sub_global_db == {}:
        sub_global_db = global_db

    if klass: 
        sub_global_db_tmp = {}
        for inst_id,inst in tqdm.tqdm(sub_global_db.items(), leave=False, desc='klass search'):
            prop = 'is_a'
            v = klass
            if prop in inst.keys():
                if (isinstance(inst[prop],(PropertyList, list)) and v in inst[prop]) or v == inst[prop]:
                    sub_global_db_tmp[inst_id] = inst
    else:
        sub_global_db_tmp = sub_global_db

    res = []
    for inst_id,inst in tqdm.tqdm(sub_global_db_tmp.items(), leave=False, desc='prop search'):
        found = []
        for prop,v in props.items():
            if prop in inst.keys():
                if isinstance(inst[prop],(PropertyList, list)):
                    found.append(v in inst[prop])
                else:
                    found.append(v == inst[prop])
        if len([f for f in found if f]) == len(props.values()):
            res.append(inst_id)
            if how=='first':
                break
    if how=='first':
        if res == []:
            return None
        else:
            return get_instance(inst_id=res[0])
    else:
        return [get_instance(inst_id=r) for r in res]


def update_db_index(inst):
    global global_db_indexed
    global global_db_index_names
    if inst is not None and 'ID' in inst.keys():
        inst_id = inst['ID']
        for prop in inst.keys():
            if prop in global_db_index_names:
                if isinstance(inst[prop], (PropertyList, list)):
                    indexed_vals = inst[prop]
                else:
                    indexed_vals = [inst[prop]]
                for indexed_val in indexed_vals:
                    if indexed_val not in global_db_indexed[prop]:
                        global_db_indexed[prop][indexed_val] = []
                    if inst_id not in global_db_indexed[prop][indexed_val]:
                        global_db_indexed[prop][indexed_val].append(inst_id)

class ObjectDict(collections.defaultdict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._snapshot = None

    def snapshot(self):
        self._snapshot = copy(self)

    def ischanged(self):
        return self._snapshot is not None and self._snapshot != self

class PropertyList(list):
    def __iadd__(self,values):
        for value in values:
            self.append(value)
        return self

    def append(self,value):
        if value not in self:
            super().append(value)

def get_instance(klass=None,nm=PREFIX, inst_id=None, props={}):
    global global_db
    '''
    return an instance with matching properties, or creates a new instance and returns that
    :param klass: type of the instance
    :param nm: namespace to use, defaults to PREFIX
    :param props: properties used to find or create the instance. 
            Includes namespace in property name (e.g. "time.hasEnd")
            Namespace is removed when runing seaarch, but used when creating a new instance, using exec().
    :return inst
    '''
    inst = None
    uuid_inst = None
    key = None
    props = dict([(k,v,) for k,v in props.items() if k==k and v==v])
    key_list = [key for key in props.keys() if key==key and 'ID' == key]

    if klass is None and inst_id is not None and inst_id in global_db.keys():
        # was created/found before under the inst_id
        # just return without changing anything
        # get any instance and get ID field, in case the first is the properties label
        inst_id = global_db[inst_id]['ID']
        # now get the final one with main inst_id value
        inst = global_db[inst_id]

        return inst

    elif inst_id is not None and inst_id in global_db.keys():
        # was created/found before under the inst_id
        # pased a klass, so process as normal
        inst = global_db[inst_id]
        inst.snapshot()

    elif len(key_list)>0:
        # the UUID was passed, let's use it to build key and find the instance
        key = props[key_list[0]]
        uuid_inst = key
        inst_id = get_instance_label(klass=klass, uuid_inst=uuid_inst)
        if inst_id and inst_id in global_db.keys():
            # was created/found before under the inst_id
            inst = global_db[inst_id]
            inst.snapshot()

    
    properties = dict(collections.OrderedDict(sorted(props.items())))


    if inst is not None:
        inst.snapshot()
        if inst_id is None and inst['ID']==[]:
            raise("No inst_id provided") # BB

        inst.snapshot()

        for prop,val in properties.items():
            if val is None: continue
            if not isinstance(val, (PropertyList,list)):
                inst[prop] = val
            else:
                for v in val: 
                    inst[prop].append(v)
    else:
        inst = ObjectDict(lambda:PropertyList(), properties)
        inst['is_a'] = klass
        if inst_id is None and 'ID' in inst.keys():
            inst_id = inst['ID']
        # if inst_id is None:
        #     warnings.warn(f"No ID provided: {inst}") # BB

        for prop,val in properties.items():
            if val is None: continue       
            if not isinstance(val, (PropertyList, list)):
                inst[prop] = val
            else:
                inst[prop] = PropertyList()
                for v in val:
                    inst[prop].append(v)
        inst.snapshot()

    if inst_id and inst['ID'] == []:
        inst['ID'] = inst_id

    if key:  global_db[key] = inst
    if inst_id: global_db[inst_id] = inst
    tmp = inst

    for prop,val in tmp.items():
        if isinstance(val,(PropertyList, list)):
            try:
                inst[prop] = PropertyList(set(val))
            except TypeError  as e:
                print(inst_id, prop, val)
                print(e)
                raise(e)
    # if inst_id is None:
    #     # Something went wrong. Display data and throw exception.
    #     warnings.warn(f"No ID provided: {inst}") # BB

    update_db_index(inst)
    return inst


def get_blank_instance(klass, inst_id, nm=PREFIX):
    global global_db
    '''
    return new an instance without any properites
    :param klass: type of the instance
    :param nm: namespace to use, defaults to PREFIX
    :return inst: instance label
    '''
    uuid_inst = uuid.uuid4()
    # if inst_id is None:
    #     warnings.warn(f"No ID provided: {inst}") # BB

    inst = ObjectDict(lambda:PropertyList())
    if inst_id: inst['ID'] = inst_id
    inst['is_a'] = klass

    inst.snapshot()

    global_db[inst_id] = inst
    update_db_index(inst)
    return inst

def encode_inst(val):
    """
    encode OWL instance name to remove any non-OWL characters (punctuation)
    return val : string value of instance label
    """
    puncts = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~ '
    val = unicodedata.normalize('NFD', val).encode('ascii', 'ignore').decode()
    val = re.sub('|'.join([re.escape(s) for s in puncts]), '_',val)
    val = re.sub(r'_+','_',val)
    return val

url_regex = re.compile("(?i)\\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\\s()<>{}\\[\\]]+|\\([^\\s()]*?\\([^\\s()]+\\)[^\\s()]*?\\)|\\([^\\s]+?\\))+(?:\\([^\\s()]*?\\([^\\s()]+\\)[^\\s()]*?\\)|\\([^\\s]+?\\)|[^\\s`!()\\[\\]{};:'\\\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\\b/?(?!@)))")
def resolve_nm(val, drop_prefix=True):
    """
    Resolve the namespace on a property range value. Uses rules and look up table for predefined property range types.
    return res : string value for property value
    """
    res = ''
    if isinstance(val, str):
        matched_uri = url_regex.match(val)
        if matched_uri:
            parsed_uri = urlparse(matched_uri[0])
            if not parsed_uri.scheme:
                res = 'http://'+val
            else:
                res = val
        else:
            match = re.findall(r'(.*)\.([^\.]+)', val)
            if len(match) == 0:
                res = f"{PREFIX}:{encode_inst(val)}"
            else:
                if match[0][0] == '': res = f"{PREFIX}:{encode_inst(match[0][1])}"
                else: res = f"{match[0][0]}:{encode_inst(match[0][1])}"

        res = re.sub(f"^{re.escape(PREFIX)}", BASE_PREFIX, res)


    elif isinstance(val, datetime.datetime):
        res = '"'+val.strftime("%Y-%m-%dT%H:%M:%S")+'"'
    else:
        res = val

    # replace DEFAULT PREFIX
    if drop_prefix:
        if isinstance(res, (ThingClass, DataPropertyClass)):
            iri = res.iri
        else:
            iri = str(res)

        if REPLACE_PREFIX == 'context_only':
            if MAPPING_CONTEXT.get(iri):
                pref,_ = MAPPING_CONTEXT.get(iri)
                res = pref
        elif REPLACE_PREFIX == 'label_only':
            iri_label = re.sub(r'^(.*):', '', iri)
            matches = [(k,v) for k,v in MAPPING_CONTEXT.items() if v[0] == iri_label]
            if len(matches)>0:
                _,mappings = matches[0]
                res = mappings[0]
        elif REPLACE_PREFIX == 'all':
            matched_uri = url_regex.match(iri)
            if not matched_uri:
                res = re.sub(r'^(.*):', '', iri)

    return res

def default_to_regular(d):
    """convert collections.defalutdict to dict"""
    if isinstance(d, ObjectDict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d

def save_global_db(filename='global_db.pickle'):
    """
    save dictionary in global_db to a pickle file.
    value vcan be used later to generate .ttl file without loading all data, or process can be restarted.
    """
    print(f"Saving global_db to pickle file... ({filename})")
    global global_db
    tmp = {}
    for key,val in tqdm.tqdm(global_db.items(), total=len(global_db.keys())):
        tmp[key] = default_to_regular(val)

    with open(filename, 'wb') as handle:
        pickle.dump(tmp, handle, protocol=pickle.HIGHEST_PROTOCOL)

def load_global_db(filename='global_db.pickle'):
    """load global_db from pickle file"""
    print(f"Loading global_db from pickle file... ({filename})")
    global global_db

    with open(filename, 'rb') as handle:
        global_db = pickle.load(handle)
    
    for key,val in tqdm.tqdm(global_db.items(), total=len(global_db.keys())):
        global_db[key] = ObjectDict(lambda: PropertyList(),val)


from rdflib import Literal
def row_to_jsonld(inst, prop_ranges={}, context=True):
    """convert global_db value to JSON-LD format"""
    object = ObjectDict(lambda:PropertyList())
    if context: object["@context"] = CONTEXT_PATH
    object["@type"] = resolve_nm(inst['is_a'],drop_prefix=False)
    if inst.get('ID') and inst.get('ID') != []: object["@id"] = inst['ID']


    for prop,vals in inst.items():
        if prop in ['is_a', 'ID', 'category']:
            continue
        vals_raw = []
        vals_dict = []
        org_list = isinstance(vals,(PropertyList,list))
        if not org_list:
            vals = [vals]

        for val in vals:
            if isinstance(val,ObjectDict):
                vals_dict.append(val)
            else:
                vals_raw.append(val)

        for val in vals_dict + list(set(vals_raw)):
            if isinstance(val, float) and math.isnan(val):
                continue
            try:
                prop_eval = eval(prop)
            except (NameError,SyntaxError):
                prop_eval = str(prop)
            ranges = []
            if prop_eval in prop_ranges.keys() and len(prop_ranges[prop_eval])>0:
                ranges = prop_ranges[prop_eval]
            if str in ranges:       o = re.sub(r'^"|"$','',Literal(val).n3())
            elif int in ranges:     o = f'{val}'
            elif float in ranges:   o = f'{val}'
            elif Thing in ranges and isinstance(val, (PropertyList, dict)): o = row_to_jsonld(val, prop_ranges=prop_ranges, context=False)
            elif isinstance(val, (PropertyList, dict)): o = row_to_jsonld(val, prop_ranges=prop_ranges, context=False)
            else:                   o = str(val)
            if org_list:
                object[resolve_nm(prop)].append(o)
            else:
                object[resolve_nm(prop)] = o
    return object








def db_to_json(dict_db=None) -> dict:
    """
    Generate dictionary for JSON.
    :param dict_db: If not None, use this dictionary as input instead of th global dict global_db.
    :type  dict_db: dict, optional
    :return: objects: dict
    """
    global global_db
    if dict_db is None:
        dict_db = global_db

    # writer.write(TURTLE_PREAMBLE.encode(encoding='UTF-8'))
    objects = []
    records = [inst for inst_id,inst in dict_db.items() if inst_id == inst['ID']]
    for inst in tqdm.tqdm(records, total=len(records)):
        klass = eval(inst['is_a'])
        # print(99,klass, inst['is_a'])
        prop_ranges = dict([(prop,prop.range) for prop in list(klass.INDIRECT_get_class_properties())])
        d = row_to_jsonld(inst, prop_ranges=prop_ranges|prop_ranges_preset)
        
        objects.append(d)
    return objects

def save_db_as_json(filename='global_db.json', dict_db=None) -> None:
    """
    Save global_db as .json file.
    :param filename: Filename where the josn will be stored.
    :type  filename: str
    :param dict_db: If not None, use this dictionary as input instead of th global dict global_db.
    :type  dict_db: dict, optional
    :return: None
    """
    objects = db_to_json(dict_db=dict_db)
    with open(filename,"w") as f:
        json.dump(objects,f)

