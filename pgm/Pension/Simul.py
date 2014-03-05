# -*- coding: utf-8 -*-


# OpenFisca -- A versatile microsimulation software
# By: OpenFisca Team <contact@openfisca.fr>
#
# Copyright (C) 2011, 2012, 2013, 2014 OpenFisca Team
# https://github.com/openfisca
#
# This file is part of OpenFisca.
#
# OpenFisca is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# OpenFisca is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import collections
import copy
import datetime as dt
import gc
import itertools
import os
import pickle
import sys

from datetime import datetime
from xml.etree import ElementTree
from pandas import DataFrame, HDFStore

from pgm.Pension.Param import legislations_add_pension as legislations
from pgm.Pension.Param import legislationsxml_add_pension as  legislationsxml
from openfisca_core import conv, model
#from .columns import EnumCol, EnumPresta
#from .datatables import DataTable
#from .taxbenefitsystems import TaxBenefitSystem


class Simulation(object):
    """
    A simulation object contains all parameters to compute a simulation from a
    test-case household (scenario) or a survey-like dataset

    See also                                                                                         
    --------
    ScenarioSimulation, SurveySimulation
    """
    chunks_count = 1
    column_by_name = None
    datesim = None
    disabled_prestations = None
    input_table = None
    io_column_by_label = None
    io_column_by_name = None
    num_table = 1
    output_table = None
    output_table_default = None
    P = None
    P_default = None
    param_file = None
    prestation_by_name = None
    reforme = False  # Boolean signaling reform mode
    subset = None
    verbose = False

    def __init__(self):
        self.io_column_by_label = collections.OrderedDict()
        self.io_column_by_name = collections.OrderedDict()

    def __getstate__(self):
        def should_pickle(v):
            return v not in ['P_default', 'P']
        return dict((k, v) for (k, v) in self.__dict__.iteritems() if should_pickle(k))

    def _set_config(self, **kwargs):
        """
        Sets some general Simulation attributes
        """
        remaining = kwargs.copy()

        for key, val in kwargs.iteritems():
            if key == "year":
                date_str = str(val)+ '-05-01'
                self.datesim = dt.datetime.strptime(date_str ,"%Y-%m-%d").date()
                remaining.pop(key)

            elif key == "datesim":
                if isinstance(val, dt.date):
                    self.datesim = val
                else:
                    self.datesim = dt.datetime.strptime(val ,"%Y-%m-%d").date()
                remaining.pop(key)

            elif key in ['param_file', 'decomp_file']:
                if hasattr(self, key):
                    setattr(self, key, val)
                    remaining.pop(key)

        if self.param_file is None:
            print "Absence de paramètres législatifs"

        # Sets required country specific classes
        #self.column_by_name = model.column_by_name
        #self.prestation_by_name = model.prestation_by_name

        return remaining

    def set_param(self, param=None, param_default=None):
        """
        Set the parameters of the simulation

        Parameters
        ----------
        param : a socio-fiscal parameter object to be used in the microsimulation.
                By default, the method uses the one provided by the attribute param_file
        param_default : a socio-fiscal parameter object to be used
                in the microsimulation to compute some gross quantities not available in the initial data.
                parma_default is necessarily different from param when examining a reform
        """
        if param is None or param_default is None:
            legislation_tree = ElementTree.parse(self.param_file)      
            legislation_xml_json = conv.check(legislationsxml.xml_legislation_to_json)(legislation_tree.getroot(),
                state = conv.default_state)
            legislation_xml_json, _ = legislationsxml.validate_node_xml_json(legislation_xml_json,
                state = conv.default_state)
            _, legislation_json = legislationsxml.transform_node_xml_json_to_json(legislation_xml_json)
            dated_legislation_json = legislations.generate_dated_legislation_json(legislation_json, self.datesim)
            print dated_legislation_json
            compact_legislation = legislations.compact_dated_node_json(dated_legislation_json)

        if param_default is None:
            self.P_default = copy.deepcopy(compact_legislation)
        else:
            self.P_default = param_default

        if param is None:
            self.P = compact_legislation
        else:
            self.P = param

    def _initialize_input_table(self):
        self.input_table = DataTable(self.column_by_name, datesim=self.datesim, num_table = self.num_table,
            subset=self.subset, print_missing=self.verbose)

    def _compute(self, **kwargs):
        """
        Computes output_data for the Simulation

        Parameters
        ----------
        difference : boolean, default True
                     When in reform mode, compute the difference between actual and default
        Returns
        -------
        data, data_default : Computed data and possibly data_default according to decomp_file

        """
        # Clear outputs
        #self.clear()

        output_table, output_table_default = self.output_table, self.output_table_default

        for key, val in kwargs.iteritems():
            setattr(output_table, key, val)
            setattr(output_table_default, key, val)

        data = output_table.calculate()
        if self.reforme:
            output_table_default.reset()
            output_table_default.disable(self.disabled_prestations)
            data_default = output_table_default.calculate()
        else:
            output_table_default = output_table
            data_default = data

        self.data, self.data_default = data, data_default

        io_column_by_label = self.io_column_by_label
        io_column_by_name = self.io_column_by_name
        for column_name, column in output_table.column_by_name.iteritems():
            io_column_by_label[column.label] = column
            io_column_by_name[column_name] = column

        gc.collect()

    def clear(self):
        """
        Clear the output_table
        """
        self.output_table = None
        self.output_table_default = None
        gc.collect()

    def get_col(self, varname):
        '''
        Look for a column in input_table column_by_name, then in output_table column_by_name.
        '''
        if varname in self.input_table.column_by_name:
            return self.input_table.column_by_name[varname]
        if self.output_table is not None and varname in self.output_table.column_by_name:
            return self.output_table.column_by_name[varname]
        print "Variable %s is absent from both input_table and output_table" % varname
        return None



    def save_content(self, name, filename):
        """
        Saves content from the simulation in an HDF store.
        We save output_table, input_table, and the default output_table dataframes,
        along with the other attributes using pickle.
        TODO : we don't save attributes P, P_default for simulation
                neither _param, _default_param for datatables.
        WARNING : Be careful when committing, you may have created a .pk data file.

        Parameters
        ----------
        name : the base name of the content inside the store.

        filename : the name of the .h5 file where the table is stored. Created if not existant.
        """

        sys.setrecursionlimit(32000)
        # Store the tables
        if self.verbose:
            print 'Saving content for simulation under name %s' %name
        ERF_HDF5_DATA_DIR = os.path.join(model.DATA_DIR, 'erf')
        store = HDFStore(os.path.join(os.path.dirname(ERF_HDF5_DATA_DIR),filename+'.h5'))
        if self.verbose:
            print 'Putting output_table in...'
        store.put(name + '_output_table', self.output_table.table)
        if self.verbose:
            print 'Putting input_table in...'
        store.put(name + '_input_table', self.input_table.table)
        if self.verbose:
            print 'Putting output_table_default in...'
        store.put(name + '_output_table_default', self.output_table_default.table)

        store.close()

        # Store all attributes from simulation
        with open(filename + '.pk', 'wb') as output:
            if self.verbose:
                print 'Storing attributes for simulation (including sub-attributes)'
            pickle.dump(self, output)


class SurveySimulation(Simulation):
    """
    A Simulation class tailored to deal with survey data
    """
    descr = None
    survey_filename = None

    def set_config(self, **kwargs):
        """
        Configures the SurveySimulation

        Parameters
        ----------
        TODO:
        survey_filename
        num_table
        """
        # Setting general attributes and getting the specific ones
        specific_kwargs = self._set_config(**kwargs)

        for key, val in specific_kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, val)

        if self.survey_filename is None:
            if self.num_table == 1 :
                filename = os.path.join(model.DATA_DIR, 'survey.h5')
            else:
                filename = os.path.join(model.DATA_DIR, 'survey3.h5')

            self.survey_filename = filename

        if self.num_table not in [1,3] :
            raise Exception("OpenFisca can be run with 1 or 3 tables only, "
                            " please, choose between both.")

        if not isinstance(self.chunks_count, int):
            raise Exception("Chunks count must be an integer")

    def inflate_survey(self, inflators):
        """
        Inflate some variable of the survey data

        Parameters
        ----------
        inflators : dict or DataFrame
                    keys or a variable column should contain the variables to
                    inflate and values of the value column the value of the inflator
        """

        if self.input_table is None:
            self.initialize_input_table()
            self.input_table.load_data_from_survey(self.survey_filename,
                                               num_table = self.num_table,
                                               subset=self.subset,
                                               print_missing=self.verbose)

        if isinstance(inflators, DataFrame):
            for varname in inflators['variable']:
                inflators.set_index('variable')
                inflator = inflators.get_value(varname, 'value')
                self.input_table.inflate(varname, inflator)
        if isinstance(inflators, dict):
            for varname, inflator in inflators.iteritems():
                self.input_table.inflate(varname, inflator)


    def initialize_input_table(self):
        """
        Initialize the input_table for a survey based simulation
        """
        self.clear()
        self._initialize_input_table()

        io_column_by_label = self.io_column_by_label
        io_column_by_name = self.io_column_by_name
        io_column_by_label.clear()
        io_column_by_name.clear()
        for column_name, column in self.input_table.column_by_name.iteritems():
            io_column_by_label[column.label] = column
            io_column_by_name[column_name] = column

    def compute(self):
        """
        Computes the output_table for a survey based simulation
        """
        self.clear()
        if self.input_table is None:
            self.initialize_input_table()
            self.input_table.load_data_from_survey(self.survey_filename,
                                               num_table = self.num_table,
                                               subset=self.subset,
                                               print_missing=self.verbose)

        if self.chunks_count == 1:
            self._compute()
        # Note: subset has already be applied
        else:
            num = self.num_table
            #TODO: replace 'idmen' by something not france-specific : the biggest entity
            if num == 1:
                list_men = self.input_table.table['idmen'].unique()
            if num == 3:
                list_men = self.input_table.table3['ind']['idmen'].unique()

            len_tot = len(list_men)
            chunk_length = int(len_tot / self.chunks_count) + 1

            for chunk_index in range(0, self.chunks_count):
                start= chunk_index * chunk_length
                end = (chunk_index + 1) * chunk_length

                subsimu = SurveySimulation()
                subsimu.__dict__ = self.__dict__.copy()
                subsimu.subset = list_men[start:end]
                subsimu.chunks_count = 1
                subsimu.compute()
                simu_chunk = subsimu
                print("compute chunk %d / %d" %(chunk_index + 1, self.chunks_count))

                if self.output_table is not None:
                    self.output_table = self.output_table + simu_chunk.output_table
                else:
                    self.output_table = simu_chunk.output_table

            # as set_imput didn't run, we do it now
            self.output_table.index = self.input_table.index
            self.output_table._inputs = self.input_table
            self.output_table._nrows = self.input_table._nrows

if __name__ == '__main__':    
    Pension = Simulation()
    Pension.param_file = 'param.xml'
    Pension.datesim = datetime.strptime("2013-01-02", "%Y-%m-%d").date()
    
    # I - Chargement des paramètres de la législation -> stockage au format .json type OF
    Pension.set_param()
    _P =  Pension.P.ret_base.RG
    print "\n Exemple de variable type 'Paramètre' : "
    print _P.age_max
    print "\n Exemple de variable type 'Génération' :"
    print _P.age_min, _P.age_min.control
    print "\n Exemple de variable type 'Barème' :"
    print _P.deduc
    
    # II - Lancement des calculs