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
import datetime as dt
import time

from CONFIG import path_pension
sys.path.append(path_pension)

from Regimes.Fonction_publique import FonctionPublique
from Regimes.Regimes_complementaires_prive import AGIRC, ARRCO
from Regimes.Regime_general import RegimeGeneral 
from SimulPension import PensionSimulation, first_year_sal
from utils import calculate_age, table_selected_dates, build_naiss

import cProfile
import re

def til_pension(sali, workstate, info_ind, info_child_father, info_child_mother, time_step='year', yearsim=2009, example=False):
    command = """run_pension(sali, workstate, info_ind, info_child_father, info_child_mother, time_step, yearsim, example)"""
    cProfile.runctx( command, globals(), locals(), filename="profile_pension" + str(yearsim))

def run_pension(sali, workstate, info_ind, info_child_father, info_child_mother, time_step='year', yearsim=2009, example=False):
    Pension = PensionSimulation()
    # I - Chargement des paramètres de la législation (-> stockage au format .json type OF) + des tables d'intéret
    # Pour l'instant on lance le calcul des retraites pour les individus ayant plus de 62 ans (sélection faite dans exprmisc de Til\liam2)
    param_file = path_pension + '\\France\\param.xml' #TODO: Amelioration
    if example:
        param_file =  path_pension +'param_example.xml'
    
    etape0 = time.time()
    info_ind['naiss'] = build_naiss(info_ind.loc[:,'agem'], dt.date(yearsim,1,1))
    etape1 = time.time()
    workstate = table_selected_dates(workstate, first_year=first_year_sal, last_year=yearsim)
    sali = table_selected_dates(sali, first_year=first_year_sal, last_year=yearsim)
    etape2 = time.time()
    config = {'year' : yearsim, 'workstate': workstate, 'sali': sali, 'info_ind': info_ind,
                'info_child_father': info_child_father, 'info_child_mother': info_child_mother, 'param_file' : param_file, 'time_step': time_step}
    Pension.set_config(**config)
    Pension.set_param()
    # II - Calculs des durées d'assurance et des SAM par régime de base
    etape3 = time.time()
    # II - 1 : Fonction Publique (l'ordre importe car bascule vers RG si la condition de durée minimale de cotisation n'est pas respectée)
    _P = Pension.P.fp
    FP = FonctionPublique(param_regime=_P, param_common=Pension.P.common, param_longitudinal=Pension.P_long)
    FP.set_config(**config)
    
    #trim_cot_FP = FP.trim_service()
    etape4 = time.time()
    # II - 2 : Régime Général
    _P =  Pension.P.prive.RG
    RG = RegimeGeneral(param_regime=_P, param_common=Pension.P.common, param_longitudinal=Pension.P_long)
    RG.set_config(**config)
    RG.build_salref()
    etape5 = time.time()
    
    trim_cot_RG = RG.nb_trim_cot() 
    trim_ass_RG = RG.nb_trim_ass()
    trim_maj_RG = RG.nb_trim_maj()
    trim_RG = trim_cot_RG + trim_ass_RG + trim_maj_RG
    SAM_RG = RG.SAM()
    etape6 = time.time()
    # III - Calculs des pensions tous régimes confondus 
    trim_cot = trim_cot_RG #+
    trim = trim_RG #+
    agem = info_ind['agem']
    trim_by_years = RG.trim_by_years
    trim_RG = RG.assurance_maj(trim_RG, trim, agem)
    CP_RG = RG.calculate_CP(trim_RG)
    etape7 = time.time()
    # III - 1 : Fonction Publique
    
    # III - 2 : Régime général
    decote_RG = RG.decote(trim, agem)
    surcote_RG = RG.surcote(trim_by_years, trim_maj_RG, agem)
    taux_RG = RG.calculate_taux(decote_RG, surcote_RG)
    assert max(taux_RG) < 1
    assert max(CP_RG) <= 1
    pension_RG = SAM_RG * CP_RG * taux_RG
    pension = pension_RG #+
    etape8 = time.time()
    # IV - Pensions de base finales (appication des minima et maxima)
    pension_RG = pension_RG + RG.minimum_contributif(pension_RG, pension, trim_RG, trim_cot, trim)
    pension_surcote_RG = SAM_RG * CP_RG * surcote_RG * RG._P.plein.taux
    pension_RG = RG.plafond_pension(pension_RG, pension_surcote_RG)

    # V - Régime complémentaire
    
    # V - 1: Régimes complémentaires du privé
        # ARRCO
    _P = Pension.P.prive
    arrco = ARRCO(param_regime=_P, param_common=Pension.P.common, param_longitudinal=Pension.P_long)
    arrco.set_config(**config)
    arrco.build_sal_regime() # Distinction cadre/non-cadre avec plafonnement des salaires des cadres
    
    points_arrco= arrco.nombre_points()
    maj_arrco = arrco.majoration_enf(points_arrco, agem) # majoration arrco AVANT éventuelle application de maj/mino pour âge
    print 'maj_arrco', maj_arrco
    coeff_arrco =  arrco.coeff_age(agem, trim)
    val_arrco = _P.complementaire.arrco.val_point 
    pension_arrco = val_arrco * points_arrco * coeff_arrco + maj_arrco
    etape9 = time.time()
        # AGIRC
    agirc = AGIRC(param_regime = _P, param_common = Pension.P.common, param_longitudinal = Pension.P_long)
    agirc.set_config(**config)
    agirc.build_sal_regime()
    points_agirc = agirc.nombre_points()
    coeff_agirc =  agirc.coeff_age(agem, trim)
    maj_agirc = agirc.majoration_enf(points_agirc, coeff_agirc, agem)  # majoration agirc APRES éventuelle application de maj/mino pour âge
    print 'maj_agirc', maj_agirc
    pension_agirc = _P.complementaire.agirc.val_point * points_agirc * coeff_agirc + maj_agirc

    to_check = {}
    to_check['dec'] = (decote_RG*RG._P.plein.taux).values
    to_check['sur'] = (surcote_RG*RG._P.plein.taux).values
    to_check['taux'] = taux_RG.values
    to_check['sam'] = SAM_RG.values
    to_check["pliq_rg"] = pension_RG.values
    to_check['prorat'] = CP_RG.values
    to_check['pts_ar'] = points_arrco
    to_check['pts_ag'] = points_agirc
    to_check['pliq_ar'] = pension_arrco
    to_check['pliq_ag'] = pension_agirc
    #print '\n', pd.DataFrame(to_check).to_string()
    etape10 = time.time()
    
    for etape in range(10):
        duree = eval('etape'+str(etape + 1)) - eval('etape'+str(etape))
        print(" la durée de l'étape " + str(etape) + " a été de ", duree)
    return pension_RG, pd.DataFrame(to_check)


def compare_til_pensipp(pensipp_input, pensipp_output, var_to_check, threshold):
    def _clean_info_child(info_child, year, id_selected):
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
    info_child_father, info_child_mother, id_enf = build_info_child(id_enf,info) 
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
        info_child_mother = _clean_info_child(info_child_mother, year, id_selected)
        info_child_father = _clean_info_child(info_child_father, year, id_selected)
        info_ind = info.loc[select_id,:]
        pension_RG, result_til_year = run_pension(sali, workstate, info_ind, info_child_father=info_child_father, info_child_mother=info_child_mother, yearsim=year, time_step='year')
        result_til.loc[result_til_year.index, :] = result_til_year
        result_til.loc[result_til_year.index,'yearliq'] = year
    #result_pensipp.to_csv('rpensipp.csv')
    #result_til.to_csv('rtil.csv')
    for var in var_to_check:
        til_var = result_til[var]
        pensipp_var = result_pensipp[var]
        conflict = ((til_var - pensipp_var).abs() > threshold)
        if conflict.any():
            print u"Le calcul de {} pose problème pour {} personne(s) sur {}: ".format(var, sum(conflict), sum(result_til['yearliq'] == 2004))
            print pd.DataFrame({
                "TIL": til_var[conflict],
                "PENSIPP": pensipp_var[conflict],
                "diff.": til_var[conflict].abs() - pensipp_var[conflict].abs(),
                "year_liq": result_til.loc[conflict, 'yearliq']
                }).to_string()
            #relevant_variables = relevant_variables_by_var[var]
            

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
    return info_enf[info_enf['sexe'] == 1], info_enf[info_enf['sexe'] == 2], info_enf['id_enf']

if __name__ == '__main__':    
    # Comparaison des résultats avec PENSIPP
    import numpy as np
    import pandas.rpy.common as com
    import datetime
    from rpy2 import robjects as r
    input_pensipp ='Z:/PENSIPP vs. TIL/dataALL.RData'
    output_pensipp = 'Z:/PENSIPP vs. TIL/output1.RData'
    var_to_check = ['sam', 'pliq_rg', 'pliq_ar', 'pliq_ag', 'pts_ag', 'pts_ar']
    threshold = 50
    
    compare_til_pensipp(input_pensipp, output_pensipp, var_to_check, threshold)

#    or to have a profiler : 
#    import cProfile
#    import re
#    command = """compare_til_pensipp(input_pensipp, output_pensipp, var_to_check, threshold)"""
#    cProfile.runctx( command, globals(), locals(), filename="OpenGLContext.profile1")