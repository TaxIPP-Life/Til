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
import numpy as np
import os
import pandas as pd


from xml.etree import ElementTree

from pgm.Pension.Param import legislations_add_pension as legislations
from pgm.Pension.Param import legislationsxml_add_pension as  legislationsxml
from openfisca_core import conv
#from .columns import EnumCol, EnumPresta
#from .taxbenefitsystems import TaxBenefitSystem

import openfisca_france
TaxBenefitSystem = openfisca_france.init_country()
tax_benefit_system = TaxBenefitSystem()


# TO DO : Add as parameters
chomage = 2
avpf = 8

class Simulation(object):
    """
    Class from OF
    A simulation object contains all parameters to compute a simulation from a
    test-case household (scenario) or a survey-like dataset

    See also                                                                                         
    --------
    ScenarioSimulation, SurveySimulation
    """
    chunks_count = 1
    datesim = None
    disabled_prestations = None
    input_table = None
    num_table = 1
    P = None
    P_default = None
    param_file = None
    reforme = False  # Boolean signaling reform mode
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
            compact_legislation = legislations.compact_dated_node_json(dated_legislation_json)
            compact_legislation_long = legislations.compact_long_dated_node_json(dated_legislation_json)
        if param_default is None:
            self.P_default = copy.deepcopy(compact_legislation)
        else:
            self.P_default = param_default
        if param is None:
            self.P = compact_legislation
            self.P_long = compact_legislation_long
        else:
            self.P = param


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


class PensionSimulation(Simulation):
    """
    A Simulation class tailored to compute pensions (deal with survey data )
    """
    descr = None
     
    def __init__(self, survey_filename = None):
        Simulation.__init__(self)
        self.survey_filename = survey_filename
                
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
                

        if self.num_table not in [1,3] :
            raise Exception("OpenFisca can be run with 1 or 3 tables only, "
                            " please, choose between both.")

        if not isinstance(self.chunks_count, int):
            raise Exception("Chunks count must be an integer")

    def compute(self):
        """
        Computes the output_table for a survey based simulation
        """
def interval_years(table):
    table = pd.DataFrame(table)
    if 'id' in table.columns:
        table = table.drop(['id'], axis = 1)
    table = table.reindex_axis(sorted(table.columns), axis=1)
    year_start = int(str(table.columns[0])[0:4])
    year_end = int(str(table.columns[-1])[0:4])
    return year_start, year_end + 1

def years_to_months(table, division = False):
    ''' 
    input : yearly-table 
    output: monthly-table with :
        - division == False : val[yyyymm] = val[yyyy]
        - division == True : val[yyyymm] = val[yyyy] / 12
    '''
    year_start, year_end = interval_years(table)
    for year in range(year_start, year_end) :
        for i in range(2,13):
            date = year * 100 + i
            table[date] = table[ year * 100 + 1 ]
    if 'id' in table.columns:
        table = table.drop(['id'], axis = 1)
    table = table.reindex_axis(sorted(table.columns), axis=1)
    table = np.array(table)
    if division == True:
        table = np.around(np.divide(table, 12), decimals = 3)
    return table

def months_to_years(table):
    year_start, year_end = interval_years(table)
    new_table = pd.DataFrame(index = table.index, columns = [(year * 100 + 1) for year in range(year_start, year_end)]).fillna(0)
    for year in range(year_start, year_end) :
        year_data = table[ year * 100 + 1 ]
        for i in range(2,13):
            date = year * 100 + i
            year_data += table[date]
        new_table.loc[:, year * 100 + 1] = year_data
    return new_table.astype(float)

def workstate_selection(table, code_regime = None, input_step = 'month', output_step = 'month', option = 'dummy'):
    ''' Input : monthly or yearly-table (lines: indiv, col: dates 'yyyymm') 
    Output : (0/1)-pandas matrix with 1 = indiv has worked at least one month during the civil year in this regime if yearly-table'''
    if not code_regime:
        print "Indiquer le code identifiant du régime"
    if input_step == output_step:
        selection = table.isin(code_regime).astype(int)
        table_code = table
    else: 
        year_start, year_end = interval_years(table)
        selected_dates = []
        for y in range(year_start, year_end): 
            #selected_dates += [(str(y * 100 + m), 'int') for m in range(1,13 * (output_step == 'month'))]
            selected_dates += [y * 100 + m for m in range(1,13 * (output_step == 'month'))]
        #selection = np.zeros((table.shape[0],nb_col_output), dtype = selected_dates)
        selection = pd.DataFrame(index = table.index, columns = selected_dates)
        table_code = pd.DataFrame(index = table.index, columns = selected_dates)
        for year in range(year_start, year_end) :
            code_selection = table[year * 100 + 1].isin(code_regime)
            table_code[year * 100 + 1] = table[year * 100 + 1]
            if input_step == 'month' and output_step == 'year': 
                for i in range(2,13):
                    date = year * 100 + i
                    code_selection = code_selection  * table[date].isin(code_regime)
                selection[year * 100 + 1] = code_selection.astype(int)
            elif input_step == 'year' and output_step == 'month':
                for i in range(1,13):
                    date = year * 100 + i
                    selection[date] = code_selection.astype(int) 
                    table_code[date] = table[year * 100 + 1]             
    if option == 'code':
        selection = table_code * selection
    return selection
    

def unemployment_trimesters(table, code_regime = None, input_step = 'month'):
    ''' Input : monthly or yearly-table (lines: indiv, col: dates 'yyyymm') 
    Output : vector with number of trimesters for unemployment'''
    if not code_regime:
        print "Indiquer le code identifiant du régime"
        
    def _select_unemployment(data, code_regime, option = 'dummy'):
        ''' Ne conserve que les périodes de chomage succédant directement à une période de cotisation au RG 
        TODO: A améliorer car boucle for très moche '''
        data_col = data.columns[1:]
        previous_col = data.columns[0]
        for col in data_col:
            data.loc[(data[previous_col] == 0) & (data[col] == chomage), col] = 0
            previous_col = col
        if option == 'code':
            data = data.replace(code_regime,0)
        else:
            assert option == 'dummy'
            data = data.isin([5]).astype(int)
        return data
    
    def _calculate_trim_unemployment(data, step, code_regime):
        ''' Détermination du vecteur d'output '''
        unemp_trim = _select_unemployment(table, code_regime)
        nb_trim = unemp_trim.sum(axis = 1)
        
        if step == 'month':
            return np.divide(nb_trim, 3).round()
        else:
            assert step == 'year'
            return 4 * nb_trim

    table = workstate_selection(table, code_regime = code_regime + [chomage], input_step = input_step, output_step = input_step, option = 'code')
    nb_trim_chom = _calculate_trim_unemployment(table, step = input_step, code_regime = code_regime)
    return nb_trim_chom

def calculate_trim_cot(sal_cot, salref):
    ''' fonction de calcul effectif des trimestres d'assurance à partir d'une matrice contenant les salaires annuels cotisés au régime
    salcot : table ne contenant que les salaires annuels cotisés au sein du régime (lignes : individus / colonnes : date)
    salref : vecteur des salaires minimum (annuels) à comparer pour obtenir le nombre de trimestre'''
    sal_cot = sal_cot.fillna(0)
    nb_trim_cot = np.minimum(np.divide(sal_cot,salref).astype(int), 4)
    nb_trim_cot = nb_trim_cot.sum(axis=1)
    return nb_trim_cot

if __name__ == '__main__':
    
    # Exemple d'utilisation d'unemployment_trimesters()
    table = pd.DataFrame(np.array([1,2,5,5,1,1,5, 1,0,5,5,1,1,5, 1,2,5,5,1,1,5, 1,2,5,5,1,1,5, 1,0,5,5,1,1,5, 1,2,5,5,1,1,5, 1,2,5,7,5,6,5,5,5, 1,2,5,5,1,1,5, 1,0,5,5,1,1,5, 1,2,5,5,1,1,5]).reshape((3,24)),
                      columns = [201201,201202,201203,201204,201205,201206,201207,201208,201209,201210,201211,201212,201301,201302,201303,201304,201305, 201306,201307,201308,201309,201310,201311,201312])
    unemp_trim = unemployment_trimesters(table, code_regime = [2], input_step = 'month')
    