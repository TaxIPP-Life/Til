from __future__ import print_function

import argparse
import os
from os.path import splitext
import platform
import warnings

# this is needed for vitables and needs to happen BEFORE matplotlib is
# imported (and imports PyQt)
import sip
sip.setapi('QString', 2)
sip.setapi('QVariant', 2)


import config
from liam2.console import Console
from liam2.context import EvaluationContext
from liam2.data import entities_from_h5, H5Data
from liam2.importer import csv2h5
from til.importer import file2h5
from til.simulation import TilSimulation
from til.upgrade import upgrade
from til.utils import AutoFlushFile
from til.view import viewhdf


from liam2.main import __version__, passthrough, eat_traceback, PrintVersionsAction


def simulate(args):
    print("Using simulation file: '%s'" % args.file)

    simulation = TilSimulation.from_yaml(args.file,
                                      input_dir=args.input_path,
                                      input_file=args.input_file,
                                      output_dir=args.output_path,
                                      output_file=args.output_file)
    simulation.run(args.interactive)
#    import cProfile as profile
#    profile.runctx('simulation.run(args.interactive)', vars(), {},
#                   'c:\\tmp\\simulation.profile')
    # to use profiling data:
    # import pstats
    # p = pstats.Stats('c:\\tmp\\simulation.profile')
    # p.strip_dirs().sort_stats('cum').print_stats(30)


def explore(fpath):
    _, ext = splitext(fpath)
    ftype = 'data' if ext in ('.h5', '.hdf5') else 'simulation'
    print("Using %s file: '%s'" % (ftype, fpath))
    if ftype == 'data':
        globals_def, entities = entities_from_h5(fpath)
        data_source = H5Data(None, fpath)
        h5in, _, globals_data = data_source.load(globals_def, entities)
        h5out = None
        simulation = TilSimulation(globals_def, None, None, None, None,
                                entities.values(), None)
        period, entity_name = None, None
    else:
        simulation = TilSimulation.from_yaml(fpath)
        h5in, h5out, globals_data = simulation.load()
        period = simulation.start_period + simulation.periods - 1
        entity_name = simulation.default_entity
    entities = simulation.entities_map
    if entity_name is None and len(entities) == 1:
        entity_name = entities.keys()[0]
    if period is None and entity_name is not None:
        entity = entities[entity_name]
        period = max(entity.output_index.keys())
    eval_ctx = EvaluationContext(simulation, entities, globals_data, period,
                                 entity_name)
    try:
        c = Console(eval_ctx)
        c.run()
    finally:
        h5in.close()
        if h5out is not None:
            h5out.close()


def display(fpath):
    print("Launching ViTables...")
    _, ext = splitext(fpath)
    if ext in ('.h5', '.hdf5'):
        files = [fpath]
    else:
        ds = TilSimulation.from_yaml(fpath).data_source
        files = [ds.input_path, ds.output_path]
    print("Trying to open:", " and ".join(str(f) for f in files))
    viewhdf(files)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--versions', action=PrintVersionsAction, nargs=0,
                        help="display versions of dependencies")
    parser.add_argument('--debug', action='store_true', default=False,
                        help="run in debug mode")
    parser.add_argument('--input-path', dest='input_path',
                        help='override the input path')
    parser.add_argument('--input-file', dest='input_file',
                        help='override the input file')
    parser.add_argument('--output-path', dest='output_path',
                        help='override the output path')
    parser.add_argument('--output-file', dest='output_file',
                        help='override the output file')

    subparsers = parser.add_subparsers(dest='action')

    # create the parser for the "run" command
    parser_run = subparsers.add_parser('run', help='run a simulation')
    parser_run.add_argument('file', help='simulation file')
    parser_run.add_argument('-i', '--interactive', action='store_true',
                            help='show the interactive console after the '
                                 'simulation')

    # create the parser for the "import" command
    parser_import = subparsers.add_parser('import', help='import data')
    parser_import.add_argument('file', help='import file')

    # create the parser for the "explore" command
    parser_explore = subparsers.add_parser('explore', help='explore data of a '
                                                           'past simulation')
    parser_explore.add_argument('file', help='explore file')

    # create the parser for the "upgrade" command
    parser_upgrade = subparsers.add_parser('upgrade',
                                           help='upgrade a simulation file to '
                                                'the latest syntax')
    parser_upgrade.add_argument('input', help='input simulation file')
    out_help = "output simulation file. If missing, the original file will " \
               "be backed up (to filename.bak) and the upgrade will be " \
               "done in-place."
    parser_upgrade.add_argument('output', help=out_help, nargs='?')

    # create the parser for the "view" command
    parser_import = subparsers.add_parser('view', help='view data')
    parser_import.add_argument('file', help='data file')

    parsed_args = parser.parse_args()
    if parsed_args.debug:
        config.debug = True

    # this can happen via the environment variable too!
    if config.debug:
        warnings.simplefilter('default')
        wrapper = passthrough
    else:
        wrapper = eat_traceback

    action = parsed_args.action
    if action == 'run':
        args = simulate, parsed_args
    elif action == "import":
        args = csv2h5, parsed_args.file
    elif action == "import_bis":
        args = file2h5, parsed_args.file
    elif action == "explore":
        args = explore, parsed_args.file
    elif action == "upgrade":
        args = upgrade, parsed_args.input, parsed_args.output
    elif action == "view":
        args = display, parsed_args.file
    else:
        raise ValueError("invalid action: %s" % action)
    wrapper(*args)


if __name__ == '__main__':
    import sys

    sys.stdout = AutoFlushFile(sys.stdout)
    sys.stderr = AutoFlushFile(sys.stderr)

    print("LIAM2 %s (%s)" % (__version__, platform.architecture()[0]))
    print()

    main()
