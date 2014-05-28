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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
import numpy as np
import pandas as pd
import sys
import datetime as dt
import time

from CONFIG import path_pension
sys.path.append(path_pension)

from Regimes.Fonction_publique import FonctionPublique
from Regimes.Regimes_complementaires_prive import AGIRC, ARRCO
from Regimes.Regime_general import RegimeGeneral 
from Regimes.Regime_social_independants import RegimeSocialIndependants
from time_array import TimeArray
from utils_pension import build_naiss, calculate_age, table_selected_dates, load_param
from pension_functions import count_enf_born, count_enf_pac, sum_from_dict, trim_maj_all
first_year_sal = 1949 

import cProfile

def til_pension(sali, workstate, info_ind, time_step='year', yearsim=2009, example=False):
    command = """run_pension(sali, workstate, info_ind, time_step, yearsim, example)"""
    cProfile.runctx( command, globals(), locals(), filename="profile_pension" + str(yearsim))
    
def clean_dict_timearray(dictionnary):
    ''' Cette fonction vérifie que les TimeArray ont bien une dénomination 'by_year' '''
    clean_dict = dictionnary.copy()
    for name, timearray_or_vector in dictionnary.iteritems():
        if isinstance(timearray_or_vector, TimeArray) and 'by_year' not in name:
            clean_dict['by_year_' + name] = clean_dict.pop(name)
    return clean_dict

def select_trim_regime(trimesters, code_regime):
    ''' Cette fonction sélectionne les TimeArray de trimesters par années + les vecteurs propres au régime '''
    trimesters = clean_dict_timearray(trimesters)
    trim_regime = dict(trim for trim in trimesters.items() if code_regime in trim[0])
    for key in trim_regime.keys():
        if code_regime in key:
            trim_regime[key.replace('_' + code_regime, '')] = trim_regime.pop(key)
    trim_regime['by_year_regime'] = sum_from_dict(trim_regime, key='by_year', plafond=4)
    trim_regime['by_year_tot'] = sum_from_dict(trimesters, key='by_year')
    trim_regime['maj_tot'] = trim_maj_all(trimesters)
    return trim_regime

def select_wage_regime(wages, code_regime):
    ''' Cette fonction sélectionne les TimeArray de salaires propres au régime'''
    wage_regime = dict(trim for trim in wages.items() if code_regime in trim[0])
    for key in wage_regime.keys():
        if code_regime in key:
            wage_regime[key.replace('_' + code_regime, '')] = wage_regime.pop(key)
    wage_regime['regime'] = sum_from_dict(wage_regime)
    return wage_regime

def select_trim_base(trimesters, code_regime_comp, correspondance):
    '''  Selectionne le vecteur du nombre de trimestres côtisés du régime de base 
    dans l'ensemble des vecteurs 
    
    Il faut expliquer. Fondamentalement c'est bizarre'''
    for base, comp in correspondance.iteritems():
        if code_regime_comp in comp:
            regime_base = base
    return trimesters['cot_' + regime_base]

def run_pension(sali, workstate, info_ind, time_step='year', yearsim=2009, to_check=False):
    if yearsim > 2009: 
        yearsim = 2009

    # I - Chargement des paramètres de la législation (-> stockage au format .json type OF) + des tables d'intéret
    # Pour l'instant on lance le calcul des retraites pour les individus ayant plus de 62 ans (sélection faite dans exprmisc de Til\liam2)
    param_file = path_pension + '\\France\\param.xml' #TODO: Amelioration
    try: 
        assert all(sali.index == workstate.index) and all(sali.index == info_ind.index)
    except:
        assert all(sali.index == workstate.index)
        assert len(sali) == len(info_ind)
        sal = sali.index
        idx = info_ind.index
        assert all(sal[sal.isin(idx)] == idx[idx.isin(sal)])
        print(sal[~sal.isin(idx)])
        print(idx[~idx.isin(sal)])
        
        # un décalage ?
        decal = idx[~idx.isin(sal)][0] - sal[~sal.isin(idx)][0]
        import pdb
        pdb.set_trace()
        
    dict_to_check = dict()
    ##TODO: should be done before
    assert sali.columns.tolist() == workstate.columns.tolist()
    assert sali.columns.tolist() == (sorted(sali.columns))
    dates = sali.columns.tolist()
    sali = np.array(sali)
    workstate = np.array(workstate)

    if max(info_ind.loc[:,'sexe']) == 2:
        info_ind.loc[:,'sexe'] = info_ind.loc[:,'sexe'].replace(1,0)
        info_ind.loc[:,'sexe'] = info_ind.loc[:,'sexe'].replace(2,1)
    info_ind.loc[:,'naiss'] = build_naiss(info_ind.loc[:,'agem'], dt.date(yearsim,1,1))
    # On fait l'hypothèse qu'on ne tient pas compte de la dernière année :
    date_param = str(yearsim)+ '-05-01'
    date_param = dt.datetime.strptime(date_param ,"%Y-%m-%d").date()
    P, P_longit = load_param(param_file, info_ind, date_param)
    config = {'datesim' : yearsim, 'P': P, 'P_longit': P_longit, 'dates': dates, 'index': info_ind.index,
              'time_step': time_step, 'data_type': 'numpy', 'first_year': first_year_sal}   
    
    sali = TimeArray(sali, dates, name='sali')
    sali.selected_dates(first=first_year_sal, last=yearsim + 1, inplace=True)
    workstate = TimeArray(workstate, dates, name='workstate')
    workstate.selected_dates(first=first_year_sal, last=yearsim + 1, inplace=True) 
   
    base_regimes = ['RegimeGeneral', 'FonctionPublique', 'RegimeSocialIndependants']
    complementaire_regimes = ['ARRCO', 'AGIRC']
    base_to_complementaire = {'RG': ['arrco', 'agirc'], 'FP': []}
    
    ### get trimesters and wages: 
    trimesters = dict()
    wages = dict()
    for reg_name in base_regimes:
        reg = eval(reg_name + '()')
        reg.set_config(**config)
        reg_trim, reg_sal = reg.get_trimesters_wages(workstate, sali, info_ind, dict_to_check)
        assert len([x for x in reg_trim.keys() if x in trimesters]) == 0
        trimesters.update(reg_trim)
        assert len([x for x in reg_sal.keys() if x in wages]) == 0
        wages.update(reg_sal)
    
    for reg_name in base_regimes:
        reg = eval(reg_name + '()')
        reg.set_config(**config)
        trim_regime = select_trim_regime(trimesters, reg.regime)
        sal_regime = select_wage_regime(wages, reg.regime)
        pension_reg = reg.calculate_pension(workstate, sali, info_ind, trim_regime, sal_regime, dict_to_check)
        if to_check == True:
            dict_to_check['pension_' + reg.regime] = pension_reg

    for reg_name in complementaire_regimes:
        reg = eval(reg_name + '()')
        reg.set_config(**config)
        trim_base = select_trim_base(trimesters, reg.regime, base_to_complementaire)
        #sal_base = select_trim_base(salaires, reg.regime, base_to_complementaire)
        pension_reg = reg.calculate_pension(workstate, sali, info_ind, trim_base, dict_to_check)
        if to_check == True:
            dict_to_check['pension_' + reg.regime] = pension_reg

    if to_check == True:
        #pd.DataFrame(to_check).to_csv('resultat2004.csv')
        return pd.DataFrame(dict_to_check)
    else:
        return pension_reg # TODO: define the output