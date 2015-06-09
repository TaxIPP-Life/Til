from __future__ import print_function

import os.path

import numpy as np

try:
    from bcolz import carray
except ImportError:
    from tables import carray

import rpy  # import com
import tables
import yaml


from liam2.utils import validate_dict, fields_yaml_to_type

from liam2.importer import array_to_disk_array, complete_path, load_def, stream_to_table, to_bool, to_int, to_float


def to_time(v):
    if isinstance(v, np.int32):
        return v
    if isinstance(v, int):
        return np.int32(v)


from liam2.importer import converters
converters.update({
    bool: to_bool,
    int: to_int,
    float: to_float,
    np.int32: to_time
    })


def file2h5(fpath, input_dir='',
                  buffersize=10 * 2 ** 20):
    with open(fpath) as f:
        content = yaml.load(f)

    yaml_layout = {
        '#output': str,
        'compression': str,
        'globals': {
            'periodic': {
                'path': str,
                'fields': [{
                    '*': str
                }],
                'oldnames': {
                    '*': str
                },
                'newnames': {
                    '*': str
                },
                'invert': [str],
                'transposed': bool
            },
            '*': {
                'path': str,
                'type': str,
                'fields': [{
                    '*': str
                }],
                'oldnames': {
                    '*': str
                },
                'newnames': {
                    '*': str
                },
                'invert': [str],
                'transposed': bool
            }
        },
        '#entities': {
            '*': {
                'path': str,
                'fields': [{
                    '*': str
                }],
                'oldnames': {
                    '*': str
                },
                'newnames': {
                    '*': str
                },
                'invert': [str],
                'transposed': bool,
                'files': None,
#                {
#                    '*': None
#                }
                'interpolate': {
                    '*': str
                }
            }
        }
    }

    validate_dict(content, yaml_layout)
    localdir = os.path.dirname(os.path.abspath(fpath))

    h5_filename = content['output']
    compression = content.get('compression')
    h5_filepath = complete_path(localdir, h5_filename)
    print("Importing in", h5_filepath)
    h5file = None
    try:
        h5file = tables.open_file(h5_filepath, mode="w", title="CSV import")

        globals_def = content.get('globals', {})
        if globals_def:
            print()
            print("globals")
            print("-------")
            const_node = h5file.create_group("/", "globals", "Globals")
            for global_name, global_def in globals_def.iteritems():
                print()
                print(" %s" % global_name)
                req_fields = ([('PERIOD', int)] if global_name == 'periodic'
                                                else [])

                kind, info = load_def(localdir, global_name,
                                      global_def, req_fields)
                if kind == 'ndarray':
                    array_to_disk_array(h5file, const_node, global_name, info,
                                        title=global_name,
                                        compression=compression)
                else:
                    assert kind == 'table'
                    fields, numlines, datastream, csvfile = info
                    stream_to_table(h5file, const_node, global_name, fields,
                                    datastream, numlines,
                                    title="%s table" % global_name,
                                    buffersize=buffersize,
                                    #FIXME: handle invert
                                    compression=compression)
                    if csvfile is not None:
                        csvfile.close()

        print()
        print("entities")
        print("--------")
        ent_node = h5file.create_group("/", "entities", "Entities")
        for ent_name, entity_def in content['entities'].iteritems():
            print()
            print(" %s" % ent_name)
            input_filename = entity_def.get('path', input_dir + ent_name + ".csv")
            if input_filename[-4:]=='.csv':
                kind, info = load_def(localdir, ent_name,
                                      entity_def, [('period', int), ('id', int)])
                assert kind == "table"
                fields, numlines, datastream, csvfile = info

                stream_to_table(h5file, ent_node, ent_name, fields,
                                datastream, numlines,
                                title="%s table" % ent_name,
                                invert=entity_def.get('invert', []),
                                buffersize=buffersize, compression=compression)
                if csvfile is not None:
                    csvfile.close()

            if input_filename[-6:]=='.Rdata':

                files_def = entity_def.get('files')
                if files_def is None:
                    files_def = ent_name
                print(" - reading", input_filename, ",file", files_def)
                rpy.set_default_mode(rpy.NO_CONVERSION)
                msg, filters = compression_str2filter(compression)

                try:
                    rpy.r.load(input_dir + input_filename)
                except:
                    rpy.r.load(input_filename)
                print(" - storing %s..." % msg)

                array_pandas = com.load_data(files_def)
                fields_def = entity_def.get('fields')
                if fields_def is not None:
                    for fdef in fields_def:
                        if isinstance(fdef, basestring):
                            raise SyntaxError("invalid field declaration: '%s', you are "
                                  "probably missing a ':'" % fdef)
                    fields = fields_yaml_to_type(fields_def)
                    columns = [col[0] for col in fields] +['id','period']
                else:
                    fields = None
                    columns = array_pandas.columns

                array_pandas = array_pandas.loc[:,columns]
                dtype = np.dtype(fields)
                #TODO: gerer les conflits

                dtype = array_pandas.to_records(index=False).dtype
                filters=None
                table = h5file.createTable(ent_node, ent_name, dtype,
                                           title="%s table" % ent_name, filters=filters)
                table.append(array_pandas.to_records(index=False))
                table.flush()
    finally:
        h5file.close()
    print()
    print("done.")
