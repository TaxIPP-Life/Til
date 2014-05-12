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
import pandas as pd
import sys

from pgm.CONFIG import path_pension
sys.path.append(path_pension)
from pension_functions import count_enf_born, count_enf_pac
from pgm.run_pension import run_pension
from utils_pension import calculate_age

def compare_til_pensipp(pensipp_input, pensipp_output, var_to_check, threshold):
    def _child_by_age(info_child, year, id_selected):
        info_child = info_child.loc[info_child['id_parent'].isin(id_selected),:]
        info_child['age'] = calculate_age(info_child['naiss'], datetime.date(year,1,1))
        nb_enf = info_child.groupby(['id_parent', 'age']).size().reset_index()
        nb_enf.columns = ['id_parent', 'age_enf', 'nb_enf']
        return nb_enf
        
    r.r("load('" + str(pensipp_input) + "')") 
    dates_to_col = [ year*100 + 1 for year in range(1901,2061)]
    statut = com.load_data('statut')
    statut.columns =  dates_to_col
    salaire = com.load_data('salaire')
    salaire.columns = dates_to_col
    info = com.load_data('ind')
    info['t_naiss'] = 1900 + info['t_naiss']
    info['naiss'] = [datetime.date(int(year),1,1) for year in info['t_naiss']]
    info['id'] = info.index
    id_enf = com.load_data('enf')
    id_enf.columns =  [ 'enf'+ str(i) for i in range(id_enf.shape[1])]
    info_child = build_info_child(id_enf,info) 
    r.r['load'](pensipp_output)
    result_pensipp = com.load_data('output1')
    result_til = pd.DataFrame(columns = var_to_check, index = result_pensipp.index)
    
    for year in range(2004,2005):
        print year
        col_to_keep = [date for date in dates_to_col if date < (year*100 + 1) and date >= 194901]
        info['agem'] =  (year - info['t_naiss'])*12
        select_id = (info['agem'] ==  63 * 12)
        id_selected = select_id[select_id == True].index
        sali = salaire.loc[select_id, col_to_keep]
        workstate = statut.loc[select_id, col_to_keep]
        info_child = _child_by_age(info_child, year, id_selected)
        nb_pac = count_enf_pac(info_child, info.index)
        nb_enf = count_enf_born(info_child, info.index)
        info_ind = info.loc[select_id,:]
        info_ind['nb_pac'] = nb_pac
        info_ind['nb_born'] = nb_enf
        pension_RG, result_til_year = run_pension(sali, workstate, info_ind, yearsim=year, time_step='year', to_check=True)
        result_til.loc[result_til_year.index, :] = result_til_year
        result_til.loc[result_til_year.index,'yearliq'] = year
    #result_pensipp.to_csv('rpensipp.csv')
    #result_til.to_csv('rtil.csv')
    var_conflict = []
    for var in var_to_check:
        til_var = result_til[var]
        pensipp_var = result_pensipp[var]
        conflict = ((til_var - pensipp_var).abs() > threshold)
        if conflict.any():
            var_conflict += [var]
            print u"Le calcul de {} pose problème pour {} personne(s) sur {}: ".format(var, sum(conflict), sum(result_til['yearliq'] == 2004))
            print pd.DataFrame({
                "TIL": til_var[conflict],
                "PENSIPP": pensipp_var[conflict],
                "diff.": til_var[conflict].abs() - pensipp_var[conflict].abs(),
                "year_liq": result_til.loc[conflict, 'yearliq']
                }).to_string()
            #relevant_variables = relevant_variables_by_var[var]
    no_conflict = [var for var in var_to_check if var not in var_conflict]  
    print( u"Avec un seuil de {}, le calcul pose problème pour les variables suivantes : {} \n Il ne pose aucun problème pour : {}").format(threshold, var_conflict, no_conflict)   

def build_info_child(enf, info_ind):
    '''
    Input tables :
        - 'enf' : pour chaque personne sont donnés les identifiants de ses enfants
        - 'ind' : table des infos perso (dates de naissances notamment)
    Output table :
        - info_child_father : identifiant du pere, ages possibles des enfants, nombre d'enfant ayant cet age
        - info_child_mother : identifiant de la mere, ages possibles des enfants, nombre d'enfant ayant cet age
    '''
    info_enf = enf.stack().reset_index()
    info_enf.columns =  ['id_parent', 'enf', 'id_enf']
    info_enf = info_enf.merge(info_ind[['sexe', 'id']], left_on='id_parent', right_on= 'id')
    info_enf = info_enf.merge(info_ind[['naiss', 'id']], left_on='id_enf', right_on= 'id').drop(['id_x', 'id_y', 'enf'], axis=1)
    return info_enf

if __name__ == '__main__':    
    # Comparaison des résultats avec PENSIPP
    import numpy as np
    import pandas.rpy.common as com
    import datetime
    from rpy2 import robjects as r
    input_pensipp ='Z:/PENSIPP vs. TIL/dataALL.RData'
    output_pensipp = 'Z:/PENSIPP vs. TIL/output1.RData'

    var_to_check = [ u'pliq_rg', u'sam', u'prorat', u'taux', u'surc', u'dec', u'DA', u'pts_ar', u'pts_ag', u'pliq_ar', u'pliq_ag'] #'sam', 'pliq_rg', 'pliq_ar', 'pts_ar','DA_RG', 'DA_RG_maj'
    threshold = 5
    
    compare_til_pensipp(input_pensipp, output_pensipp, var_to_check, threshold)

#    or to have a profiler : 
    import cProfile
    import re
    command = """compare_til_pensipp(input_pensipp, output_pensipp, var_to_check, threshold)"""
    cProfile.runctx( command, globals(), locals(), filename="profile_run_compare")