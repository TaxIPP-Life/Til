# -*- coding:utf-8 -*-

from __future__ import print_function, division


from collections import defaultdict
import functools
import os.path
import random
import time
import warnings


import numpy as np
from pandas import DataFrame, HDFStore
import tables
import yaml


# Monkey patching liam2
try:
    from liam2.exprtools import functions
except ImportError:
    try:
        from liam2.src.exprtools import functions
    except ImportError:
        import sys
        sys.path.append('C:\Users\m.benjelloul\Documents\GitHub\liam2')
        from src.exprtools import functions

from til import exprmisc
functions.update(exprmisc.functions)

try:
    from liam2.expr import type_to_idx
except ImportError:
    from src.expr import type_to_idx
type_to_idx.update({
    np.int8: 1,
    np.int16: 1,
    np.int32: 1
    })


# TilSimulation specific import
try:
    from liam2 import config, console
    from liam2.context import EvaluationContext
    from liam2.data import H5Data, Void
    from liam2.entities import Entity, global_symbols
    from liam2.utils import (
        field_str_to_type, fields_yaml_to_type, gettime, time2str, timed, validate_dict
        )
    from liam2.simulation import expand_periodic_fields, handle_imports, show_top_processes, Simulation
except ImportError:
    from src import config, console
    from src.context import EvaluationContext
    from src.data import H5Data, Void
    from src.entities import Entity, global_symbols
    from src.utils import (
        field_str_to_type, fields_yaml_to_type, gettime, time2str, timed, validate_dict
        )
    from src.simulation import expand_periodic_fields, handle_imports, show_top_processes, Simulation


from til.utils import addmonth, time_period

from til.process import ExtProcess


class TilSimulation(Simulation):
    index_for_person_variable_name_by_entity_name = {
        'individus': 'id',
        'foyers_fiscaux': 'idfoy',
        'menages': 'idmen',
        }  # TODO: should be imporved

    input_store = None
    output_store = None

    weight_column_name_by_entity_name = {
        'menages': 'wprm',
        }  # TODO should be elsewhere

    uniform_weight = None

    yaml_layout = {
        'import': None,
        'globals': {
            'periodic': None,  # either full-blown (dict) description or list
                               # of fields
            '*': {
                'path': str,
                'type': str,
                'fields': [{
                    '*': None  # Or(str, {'type': str, 'initialdata': bool, 'default': type})
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
                'fields': [{
                    '*': None
                    }],
                'links': {
                    '*': {
                        '#type': str,  # Or('many2one', 'one2many', 'one2one')
                        '#target': str,
                        '#field': str
                        }
                    },
                'macros': {
                    '*': None
                    },
                'processes': {
                    '*': None
                    }
                }
            },
        '#simulation': {
            'init': [{
                '*': [None]  # Or(str, [str, int])
                }],
            '#processes': [{
                '*': [None]  # Or(str, [str, int])
                }],
            'random_seed': int,
            'input': {
                'path': str,
                'file': str,
                'method': str
                },
            'output': {
                'path': str,
                'file': str
                },
            'legislation': {
                '#ex_post': bool,
                '#annee': int
                },
            'final_stat': bool,
            'time_scale': str,
            'retro': bool,
            'logging': {
                'timings': bool,
                'level': str,  # Or('periods', 'procedures', 'processes')
                },
            '#periods': int,
            'start_period': int,
            'init_period': int,
            'skip_shows': bool,
            'timings': bool,      # deprecated
            'assertions': str,
            'default_entity': str,
            'autodump': None,
            'autodiff': None
            }
        }

    def __init__(self, globals_def, periods, start_period, init_processes,
                 processes, entities, data_source, default_entity=None,
                 legislation = None, final_stat = False,
                 time_scale = 'year0', retro = False, uniform_weight = None):
        # time_scale year0: default liam2
        # time_scale year: Alexis
        # FIXME: what if period has been declared explicitly?
        if 'periodic' in globals_def:
            globals_def['periodic']['fields'].insert(0, ('PERIOD', int))

        self.globals_def = globals_def
        self.periods = periods
        print(self.periods)
        # TODO: work on it for start with seme
        assert(isinstance(start_period, int))
        if time_scale == 'year0':
            assert 0 <= start_period <= 9999, "{} is a non valid start period".format(start_period)
        if time_scale == 'year':
            assert 0 <= start_period <= 999999, "{} is a non valid start period".format(start_period)
            assert (start_period % 100) in range(1, 12), "{} is a non valid start period".format(start_period)

        self.start_period = start_period
        # init_processes is a list of tuple: (process, 1)
        self.init_processes = init_processes
        # processes is a list of tuple: (process, periodicity, start)
        self.processes = processes
        self.entities = entities
        self.data_source = data_source
        self.default_entity = default_entity
        self.legislation = legislation
        self.final_stat = final_stat
        self.time_scale = time_scale
        self.longitudinal = {}
        self.retro = retro
        self.stepbystep = False

        if uniform_weight is not None:
            self.uniform_weight = uniform_weight

    @classmethod
    def from_yaml(cls, fpath, input_dir = None, input_file = None, output_dir = None, output_file = None):
        simulation_path = os.path.abspath(fpath)
        simulation_dir = os.path.dirname(simulation_path)
        with open(fpath) as f:
            content = yaml.load(f)

        expand_periodic_fields(content)
        content = handle_imports(content, simulation_dir)

        validate_dict(content, cls.yaml_layout)

        # the goal is to get something like:
        # globals_def = {'periodic': [('a': int), ...],
        #                'MIG': int}
        globals_def = content.get('globals', {})
        for k, v in content.get('globals', {}).iteritems():
            if "type" in v:
                v["type"] = field_str_to_type(v["type"], "array '%s'" % k)
            else:
                #TODO: fields should be optional (would use all the fields
                # provided in the file)
                v["fields"] = fields_yaml_to_type(v["fields"])
            globals_def[k] = v

        simulation_def = content['simulation']
        seed = simulation_def.get('random_seed')
        if seed is not None:
            seed = int(seed)
            print("using fixed random seed: %d" % seed)
            random.seed(seed)
            np.random.seed(seed)

        periods = simulation_def['periods']
        time_scale = simulation_def.get('time_scale', 'year0')
        retro = simulation_def.get('retro', False)

        start_period = simulation_def.get('start_period', None)
        init_period = simulation_def.get('init_period', None)

        if start_period is None and init_period is None:
            raise Exception("Either start_period either init_period should be given.")
        if start_period is not None:
            if init_period is not None:
                raise Exception("Start_period can't be given if init_period is.")
            step = time_period[time_scale] * (1 - 2 * (retro))
            print(time_scale)
            if time_scale == 'year0':
                init_period = addmonth(start_period, step)
            else:
                init_period = start_period
            print('init_period')
            print(init_period)

        config.skip_shows = simulation_def.get('skip_shows', config.skip_shows)
        # TODO: check that the value is one of "raise", "skip", "warn"
        config.assertions = simulation_def.get('assertions', config.assertions)

        logging_def = simulation_def.get('logging', {})
        config.log_level = logging_def.get('level', config.log_level)
        if 'timings' in simulation_def:
            warnings.warn("simulation.timings is deprecated, please use "
                          "simulation.logging.timings instead",
                          DeprecationWarning)
            config.show_timings = simulation_def['timings']
        config.show_timings = logging_def.get('timings', config.show_timings)

        autodump = simulation_def.get('autodump', None)
        if autodump is True:
            autodump = 'autodump.h5'
        if isinstance(autodump, basestring):
            # by default autodump will dump all rows
            autodump = (autodump, None)
        config.autodump = autodump

        autodiff = simulation_def.get('autodiff', None)
        if autodiff is True:
            autodiff = 'autodump.h5'
        if isinstance(autodiff, basestring):
            # by default autodiff will compare all rows
            autodiff = (autodiff, None)
        config.autodiff = autodiff

        legislation = simulation_def.get('legislation', None)
        final_stat = simulation_def.get('final_stat', None)

        input_def = simulation_def.get('input')
        if input_def is not None or input_dir is not None:
            input_directory = input_dir if input_dir is not None else input_def.get('path', '')
        else:
            input_directory = ''

        if not os.path.isabs(input_directory):
            input_directory = os.path.join(simulation_dir, input_directory)
        config.input_directory = input_directory

        output_def = simulation_def.get('output')
        if output_def is not None:
            output_directory = output_dir if output_dir is not None else output_def.get('path', '')
        else:
            output_directory = ''
        if not os.path.isabs(output_directory):
            output_directory = os.path.join(simulation_dir, output_directory)
        if not os.path.exists(output_directory):
            print("creating directory: '%s'" % output_directory)
            os.makedirs(output_directory)
        config.output_directory = output_directory

        if output_file is None:
            output_file = output_def.get('file')
            assert output_file is not None

        output_path = os.path.join(output_directory, output_file)

        if input_def is not None:
            method = input_def.get('method', 'h5')
        else:
            method = 'h5'

        # need to be before processes because in case of legislation, we need input_table for now.
        if method == 'h5':
            if input_file is None:
                assert input_def is not None
                input_file = input_def['file']
            assert input_file is not None
            input_path = os.path.join(input_directory, input_file)
            data_source = H5Data(input_path, output_path)
        elif method == 'void':
            input_path = None
            data_source = Void(output_path)
        else:
            print(method, type(method))

        entities = {}
        for k, v in content['entities'].iteritems():
            entities[k] = Entity.from_yaml(k, v)

        for entity in entities.itervalues():
            entity.attach_and_resolve_links(entities)

        global_context = {'__globals__': global_symbols(globals_def),
                          '__entities__': entities}
        parsing_context = global_context.copy()
        parsing_context.update((entity.name, entity.all_symbols(global_context))
                               for entity in entities.itervalues())
        for entity in entities.itervalues():
            parsing_context['__entity__'] = entity.name
            entity.parse_processes(parsing_context)
            entity.compute_lagged_fields()
            # entity.optimize_processes()

        # for entity in entities.itervalues():
        #     entity.resolve_method_calls()
        used_entities = set()
        init_def = [d.items()[0] for d in simulation_def.get('init', {})]
        init_processes = []
        for ent_name, proc_names in init_def:
            if ent_name != 'legislation':
                if ent_name not in entities:
                    raise Exception("Entity '%s' not found" % ent_name)

                entity = entities[ent_name]
                used_entities.add(ent_name)
                init_processes.extend([(entity.processes[proc_name], 1, 1)
                                       for proc_name in proc_names])
            else:
                boum1
                proc = ExtProcess('of_on_liam', ['simulation', 2009, 'period'])
                init_processes.append((proc, 1, 1))

        processes_def = [d.items()[0] for d in simulation_def['processes']]
        processes = []
        for ent_name, proc_defs in processes_def:
            if ent_name != 'legislation':
                entity = entities[ent_name]
                used_entities.add(ent_name)
                for proc_def in proc_defs:
                    # proc_def is simply a process name
                    if isinstance(proc_def, basestring):
                        # use the default periodicity of 1
                        proc_name, periodicity, start = proc_def, 1, 1
                    else:
                        if len(proc_def) == 3:
                            proc_name, periodicity, start = proc_def
                        elif len(proc_def) == 2:
                            proc_name, periodicity = proc_def
                            start = 1
                    processes.append((entity.processes[proc_name], periodicity, start))
            else:
                pass
                # proc = ExtProcess('of_on_liam', ['simulation', proc_defs[0], 'period'])
                # processes.append((proc, 'year', 12))

        entities_list = sorted(entities.values(), key=lambda e: e.name)
        declared_entities = set(e.name for e in entities_list)
        unused_entities = declared_entities - used_entities
        if unused_entities:
            suffix = 'y' if len(unused_entities) == 1 else 'ies'
            print("WARNING: entit%s without any executed process:" % suffix,
                  ','.join(sorted(unused_entities)))

        if method == 'h5':
            if input_file is None:
                input_file = input_def['file']
            input_path = os.path.join(input_directory, input_file)
            data_source = H5Data(input_path, output_path)
        elif method == 'void':
            data_source = Void(output_path)
        else:
            raise ValueError("'%s' is an invalid value for 'method'. It should "
                             "be either 'h5' or 'void'")

        default_entity = simulation_def.get('default_entity')
        # processes[2][0].subprocesses[0][0]
        return TilSimulation(
            globals_def, periods,
            init_period,
            init_processes,
            processes,
            entities_list,
            data_source,
            default_entity,
            legislation,
            final_stat,
            time_scale,
            retro
            )

    def load(self):
        return timed(self.data_source.load, self.globals_def, self.entities_map)

    @property
    def entities_map(self):
        return {entity.name: entity for entity in self.entities}

    def run(self, run_console=False):
        start_time = time.time()
        h5in, h5out, globals_data = timed(self.data_source.run,
                                          self.globals_def,
                                          self.entities_map,
                                          self.start_period)

        if config.autodump or config.autodiff:
            if config.autodump:
                fname, _ = config.autodump
                mode = 'w'
            else:  # config.autodiff
                fname, _ = config.autodiff
                mode = 'r'
            fpath = os.path.join(config.output_directory, fname)
            h5_autodump = tables.open_file(fpath, mode=mode)
            config.autodump_file = h5_autodump
        else:
            h5_autodump = None

#        input_dataset = self.data_source.run(self.globals_def,
#                                             entity_registry)
#        output_dataset = self.data_sink.prepare(self.globals_def,
#                                                entity_registry)
#        output_dataset.copy(input_dataset, self.init_period - 1)
#        for entity in input_dataset:
#            indexed_array = buildArrayForPeriod(entity)

        # tell numpy we do not want warnings for x/0 and 0/0
        np.seterr(divide='ignore', invalid='ignore')

        process_time = defaultdict(float)
        period_objects = {}

        eval_ctx = EvaluationContext(self, self.entities_map, globals_data)
        eval_ctx.periodicity = time_period[self.time_scale] * (1 - 2 * (self.retro))
        eval_ctx.format_date = self.time_scale

        def simulate_period(period_idx, period, periods, processes, entities,
                            init=False):
            period_start_time = time.time()

            # period_idx: index of current computed period
            # periods list of all periods
            # period = periods[period_idx]

            # set current period
            eval_ctx.period = period
            eval_ctx.periods = periods
            eval_ctx.period_idx = period_idx + 1
            print(eval_ctx.period_idx)
#            # build context for this period:
#            const_dict = {'period_idx': period_idx + 1,
#                          'longitudinal': self.longitudinal,
#                          'pension': None,
#            assert(periods[period_idx + 1] == period)

            if config.log_level in ("procedures", "processes"):
                print()
            print("period", period,
                  end=" " if config.log_level == "periods" else "\n")
            if init and config.log_level in ("procedures", "processes"):
                for entity in entities:
                    print("  * %s: %d individuals" % (entity.name,
                                                      len(entity.array)))
            else:
                if config.log_level in ("procedures", "processes"):
                    print("- loading input data")
                    for entity in entities:
                        print("  *", entity.name, "...", end=' ')
                        timed(entity.load_period_data, period)
                        print("    -> %d individuals" % len(entity.array))
                else:
                    for entity in entities:
                        entity.load_period_data(period)
            for entity in entities:
                entity.array_period = period
                entity.array['period'] = period

            # Longitudinal
            person_name = 'individus'
            person = [x for x in entities if x.name == person_name][0]
            var_id = person.array.columns['id']
            # Init
            use_longitudinal_after_init = any(
                varname in self.longitudinal for varname in ['salaire_imposable', 'workstate']
                )
            if init:
                for varname in ['salaire_imposable', 'workstate']:
                    self.longitudinal[varname] = None
                    var = person.array.columns[varname]
                    fpath = self.data_source.input_path
                    input_file = HDFStore(fpath, mode="r")
                    if 'longitudinal' in input_file.root:
                        input_longitudinal = input_file.root.longitudinal
                        if varname in input_longitudinal:
                            self.longitudinal[varname] = input_file['/longitudinal/' + varname]
                            if period not in self.longitudinal[varname].columns:
                                table = DataFrame({'id': var_id, period: var})
                                self.longitudinal[varname] = self.longitudinal[varname].merge(
                                    table, on='id', how='outer')
                    if self.longitudinal[varname] is None:
                        self.longitudinal[varname] = DataFrame({'id': var_id, period: var})

            # maybe we have a get_entity or anything nicer than that # TODO: check
            elif use_longitudinal_after_init:
                for varname in ['salaire_imposable', 'workstate']:
                    var = person.array.columns[varname]
                    table = DataFrame({'id': var_id, period: var})
                    if period in self.longitudinal[varname]:
                        import pdb
                        pdb.set_trace()
                    self.longitudinal[varname] = self.longitudinal[varname].merge(table, on='id', how='outer')

            eval_ctx.longitudinal = self.longitudinal

            if processes:
                num_processes = len(processes)
                for p_num, process_def in enumerate(processes, start=1):
                    process, periodicity, start = process_def

                    # set current entity
                    if process.entity:
                        eval_ctx.entity_name = process.entity.name

                    if config.log_level in ("procedures", "processes"):
                        print("- %d/%d" % (p_num, num_processes), process.name,
                              end=' ')
                        print("...", end=' ')
                    # TDOD: change that
                    print('periodicity: {}'.format(periodicity))
                    if isinstance(periodicity, int):
                        if period_idx % periodicity == 0:
                            elapsed, _ = gettime(process.run_guarded, eval_ctx)
                        else:
                            elapsed = 0
                            if config.log_level in ("procedures", "processes"):
                                print("skipped (periodicity)")
                    else:
                        assert periodicity in time_period
                        periodicity_process = time_period[periodicity]
                        periodicity_simul = time_period[self.time_scale]
                        month_idx = period % 100
                        # first condition, to run a process with start == 12
                        # each year even if year are yyyy01
                        # modify start if periodicity_simul is not month
                        start = int(start / periodicity_simul - 0.01) * periodicity_simul + 1

                        if (periodicity_process <= periodicity_simul and self.time_scale != 'year0') or (
                                month_idx % periodicity_process == start % periodicity_process):

                            elapsed, _ = gettime(process.run_guarded, eval_ctx)

                        else:
                            elapsed = 0
                            if config.log_level in ("procedures", "processes"):
                                print("skipped (periodicity)")

                    process_time[process.name] += elapsed
                    if config.log_level in ("procedures", "processes"):
                        if config.show_timings:
                            print("done (%s elapsed)." % time2str(elapsed))
                        else:
                            print("done.")
                    self.start_console(eval_ctx)

            if config.log_level in ("procedures", "processes"):
                print("- storing period data")
                for entity in entities:
                    print("  *", entity.name, "...", end=' ')
                    timed(entity.store_period_data, period)
                    print("    -> %d individuals" % len(entity.array))
            else:
                for entity in entities:
                    entity.store_period_data(period)
#            print " - compressing period data"
#            for entity in entities:
#                print "  *", entity.name, "...",
#                for level in range(1, 10, 2):
#                    print "   %d:" % level,
#                    timed(entity.compress_period_data, level)
            period_objects[period] = sum(len(entity.array)
                                         for entity in entities)
            period_elapsed_time = time.time() - period_start_time
            if config.log_level in ("procedures", "processes"):
                print("period %d" % period, end=' ')
            print("done", end=' ')
            if config.show_timings:
                print("(%s elapsed)" % time2str(period_elapsed_time), end="")
                if init:
                    print(".")
                else:
                    main_elapsed_time = time.time() - main_start_time
                    periods_done = period_idx + 1
                    remaining_periods = self.periods - periods_done
                    avg_time = main_elapsed_time / periods_done
                    # future_time = period_elapsed_time * 0.4 + avg_time * 0.6
                    remaining_time = avg_time * remaining_periods
                    print(" - estimated remaining time: %s."
                          % time2str(remaining_time))
            else:
                print()

        print("""
=====================
 starting simulation
=====================""")
        try:
            assert(self.time_scale in time_period)
            month_periodicity = time_period[self.time_scale]
            time_direction = 1 - 2 * (self.retro)
            time_step = month_periodicity * time_direction
            if self.time_scale == 'year0':
                periods = [self.start_period + t for t in range(0, (self.periods + 1))]
            elif self.time_scale == 'year':
                periods = [
                    self.start_period + int(t / 12) * 100 + t % 12
                    for t in range(0, (self.periods + 1) * time_step, time_step)
                    ]

            print("simulated period are going to be: ", periods)

            init_start_time = time.time()
            print(self.start_period)
            print(periods[0])
            simulate_period(0, self.start_period, [None, periods[0]], self.init_processes,
                            self.entities, init=True)
            time_init = time.time() - init_start_time

            main_start_time = time.time()
            for period_idx, period in enumerate(periods[1:]):
                period_start_time = time.time()
                simulate_period(period_idx, period, periods,
                                self.processes, self.entities)

#                 if self.legislation:
#                     if not self.legislation['ex_post']:
#
#                         elapsed, _ = gettime(liam2of.main,period)
#                         process_time['liam2of'] += elapsed
#                         elapsed, _ = gettime(of_on_liam.main,self.legislation['annee'],[period])
#                         process_time['legislation'] += elapsed
#                         elapsed, _ = gettime(merge_leg.merge_h5,self.data_source.output_path,
#                                              "C:/Til/output/"+"simul_leg.h5",period)
#                         process_time['merge_leg'] += elapsed

                time_elapsed = time.time() - period_start_time
                print("period %d done" % period, end=' ')
                if config.show_timings:
                    print("(%s elapsed)." % time2str(time_elapsed))
                else:
                    print()

            total_objects = sum(period_objects[period] for period in periods)
            total_time = time.time() - main_start_time

#             if self.legislation:
#                 if self.legislation['ex_post']:
#
#                     elapsed, _ = gettime(liam2of.main)
#                     process_time['liam2of'] += elapsed
#                     elapsed, _ = gettime(of_on_liam.main,self.legislation['annee'])
#                     process_time['legislation'] += elapsed
#                     # TODO: faire un programme a part, so far ca ne marche pas pour l'ensemble
#                     # adapter n'est pas si facile, comme on veut economiser une table,
#                     # on ne peut pas faire de append directement parce qu on met 2010 apres 2011
#                     # a un moment dans le calcul
#                     elapsed, _ = gettime(merge_leg.merge_h5,self.data_source.output_path,
#                                          "C:/Til/output/"+"simul_leg.h5",None)
#                     process_time['merge_leg'] += elapsed

            if self.final_stat:
                elapsed, _ = gettime(start, period)
                process_time['Stat'] += elapsed

            total_time = time.time() - main_start_time
            time_year = 0
            if len(periods) > 1:
                nb_year_approx = periods[-1] / 100 - periods[1] / 100
                if nb_year_approx > 0:
                    time_year = total_time / nb_year_approx

            try:
                ind_per_sec = str(int(total_objects / total_time))
            except ZeroDivisionError:
                ind_per_sec = 'inf'

            print("""
==========================================
 simulation done
==========================================
 * %s elapsed
 * %d individuals on average
 * %s individuals/s/period on average

 * %s second for init_process
 * %s time/period in average
 * %s time/year in average
==========================================
""" % (
                time2str(time.time() - start_time),
                total_objects / self.periods,
                ind_per_sec,
                time2str(time_init),
                time2str(total_time / self.periods),
                time2str(time_year))
            )

            show_top_processes(process_time, 10)
#            if config.debug:
#                show_top_expr()

            if run_console:
                console_ctx = eval_ctx.clone(entity_name=self.default_entity)
                c = console.Console(console_ctx)
                c.run()

        finally:
            if h5in is not None:
                h5in.close()
            h5out.close()
            if h5_autodump is not None:
                h5_autodump.close()

    def close_output_sore(self):
        if self.output_store is not None:
            self.output_store.close()

    def get_output_store(self):
        if self.output_store is None:
            return HDFStore(self.data_source.output_path)
        elif not self.output_store.is_open:
            self.output_store.open()

        return self.output_store

    def get_variable(self, variables_name, fillna_value = None, function = None):
        assert self.uniform_weight is not None
        threshold = self.uniform_weight  # TODO
        assert isinstance(variables_name, list)
        output_store = self.get_output_store()
        entities = self.entities_map.keys()
        variable_by_name = dict(
            (variable.name, variable)
            for entity in entities
            for variable in self.entities_map[entity].variables.values()
            if hasattr(variable, 'name') and entity != 'register'
            )
        available_variables_name = variable_by_name.keys()
        assert len(available_variables_name) == len(set(available_variables_name))

        holding_entities = set([variable_by_name[variable_name].entity.name for variable_name in variables_name])
        print(holding_entities)
        assert len(holding_entities) == 1, 'Variables are dispatched on more {} entities {}: \n {}'.format(
            len(holding_entities),
            set(variable_by_name[variable_name].entity.name for variable_name in variables_name),
            dict(
                (variable_name, variable_by_name[variable_name].entity.name)
                for variable_name in variables_name
                )
            )

        entity = variable_by_name[variables_name[0]].entity
        entity.index_for_person_variable_name = self.index_for_person_variable_name_by_entity_name[entity.name]

        for variable_name in variables_name:
            assert variable_name in output_store['entities/{}'.format(entity)].columns

        columns = variables_name + ['id', 'period']
        if self.weight_column_name_by_entity_name.get(entity.name):
            columns.append(self.weight_column_name_by_entity_name.get(entity.name))
        df = output_store['entities/{}'.format(entity)][columns]
        df.period = df.period // 100
        panel = df.set_index(['id', 'period']).to_panel()

        weight_column_name = self.weight_column_name_by_entity_name.get(entity.name)
        if weight_column_name:
            panel[weight_column_name] = (panel[weight_column_name] != -1) * threshold
        else:
            weight_column_name = 'weights'
            panel[weight_column_name] = threshold

        if len(variables_name) == 1:
            variable_name = variables_name[0]
            if fillna_value:
                panel[variable_name].fillna(value = fillna_value, inplace = True)

            if function == 'count':
                return weighted_count(panel, variable_name, weights = weight_column_name)
            elif function == 'mean':
                return weighted_mean(panel, variable_name, weights = weight_column_name)
            elif function == 'sum':
                return weighted_sum(panel, variable_name, weights = weight_column_name)
            else:
                return panel
        else:
            return panel

    def start_console(self, context):
        if self.stepbystep:
            c = console.Console(context)
            res = c.run(debugger=True)
            self.stepbystep = res == "step"


def weighted_count(panel, variable, weights = None, group_by = None):
    weighted_count_func = lambda x, w: (x.notnull() * w).sum()
    return weighted_func(panel, variable, weighted_func = weighted_count_func, weights = weights, group_by = group_by)


def weighted_func(panel, variable, weighted_func = None, weights = None, group_by = None):
    if group_by is not None:
        NotImplementedError
    assert isinstance(variable, str)

    if isinstance(weights, str):
        assert weights in panel
        weight_name = weights
        weights = panel[weight_name]
        return weighted_func(panel[variable], weights)

    else:
        NotImplementedError  # TODO uniform weights
        weighted_func_partial = functools.partial(weighted_func, weights)
        lambda x: (x * weights).sum() / weights.sum()
        return panel[variable].apply(weighted_func_partial)


def weighted_mean(panel, variable, weights = None, group_by = None):
    weighted_mean_func = lambda x, w: (x * w).sum() / w.sum()
    return weighted_func(panel, variable, weighted_func = weighted_mean_func, weights = weights, group_by = group_by)


def weighted_sum(panel, variable, weights = None, group_by = None):
    weighted_sum_func = lambda x, w: (x * w).sum()
    return weighted_func(panel, variable, weighted_func = weighted_sum_func, weights = weights, group_by = group_by)
