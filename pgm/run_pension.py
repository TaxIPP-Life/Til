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
from time_array import TimeArray
from utils_pension import build_naiss, calculate_age, table_selected_dates, load_param
from pension_functions import count_enf_born, count_enf_pac
first_year_sal = 1949 

import cProfile

def til_pension(sali, workstate, info_ind, time_step='year', yearsim=2009, example=False):
    command = """run_pension(sali, workstate, info_ind, time_step, yearsim, example)"""
    cProfile.runctx( command, globals(), locals(), filename="profile_pension" + str(yearsim))

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
    ##TODO: should be done before
    assert sali.columns.tolist() == workstate.columns.tolist()
    assert sali.columns.tolist() == (sorted(sali.columns))
    dates = sali.columns.tolist()
    sali = np.array(sali)
    workstate = np.array(workstate)

    #TODO: virer ça, non ?
    if max(info_ind['sexe']) == 2:
        info_ind['sexe'] = info_ind['sexe'].replace(1,0)
        info_ind['sexe'] = info_ind['sexe'].replace(2,1)
    info_ind['naiss'] = build_naiss(info_ind.loc[:,'agem'], dt.date(yearsim,1,1))
    # On fait l'hypothèse qu'on ne tient pas compte de la dernière année :


    date_param = str(yearsim)+ '-05-01'
    date_param = dt.datetime.strptime(date_param ,"%Y-%m-%d").date()
    P, P_longit = load_param(param_file, date_param)
    config = {'yearsim' : yearsim, 'P': P, 'P_longit': P_longit,
              'dates': dates, 'info_ind': info_ind,
               'time_step': time_step, 'data_type': 'numpy', 'first_year': first_year_sal}   
    
    sali = TimeArray(sali, dates)
    sali.selected_dates(first=first_year_sal, last=yearsim + 1, inplace=True)
    workstate = TimeArray(workstate, dates)
    workstate.selected_dates(first=first_year_sal, last=yearsim + 1, inplace=True) 
    # II - Calculs des durées d'assurance et des SAM par régime de base
   
    # II - 1 : Fonction Publique (l'ordre importe car bascule vers RG si la condition de durée minimale de cotisation n'est pas respectée)
    FP = FonctionPublique()
    FP.set_config(**config)
    trim_valides = FP.nb_trim_valide(workstate)
    trim_actif = FP.nb_trim_valide(workstate, FP.code_actif)
    FP.build_age_ref(trim_actif, workstate)
    trim_FP = trim_valides #+...
    
    # II - 2 : Régime Général
    _P =  P.prive.RG
    RG = RegimeGeneral()
    RG.set_config(**config)
    RG.build_salref()
    
    trim_cot_RG = RG.nb_trim_cot(workstate, sali) 
    trim_ass_RG = RG.nb_trim_ass(workstate)
    trim_maj_RG = RG.nb_trim_maj(workstate, sali)
    trim_RG = trim_cot_RG + trim_ass_RG + trim_maj_RG
    SAM_RG = RG.SAM()
    
    # III - Calculs des pensions tous régimes confondus 
    trim_cot = trim_cot_RG #+
    trim = trim_RG #+
    agem = info_ind['agem']
    trim_RG = RG.assurance_maj(trim_RG, trim, agem)
    CP_RG = RG.calculate_CP(trim_RG)
    # III - 1 : Fonction Publique
    
    # III - 2 : Régime général
    decote_RG = RG.decote(trim, agem)
    trim_by_year = RG.trim_by_year # + _.trim_by_year...
    surcote_RG = RG.surcote(trim_by_year, trim_maj_RG, agem)
    taux_RG = RG.calculate_taux(decote_RG, surcote_RG)
    pension_RG = SAM_RG*CP_RG*taux_RG
    pension = pension_RG #+
    
    # IV - Pensions de base finales (appication des minima et maxima)
    pension_RG = pension_RG + RG.minimum_contributif(pension_RG, pension, trim_RG, trim_cot, trim)
    
    pension_surcote_RG = SAM_RG*CP_RG*surcote_RG* P.prive.RG.plein.taux
    pension_RG = RG.plafond_pension(pension_RG, pension_surcote_RG)
    etape8 = time.time()
    # V - Régime complémentaire
    
    # V - 1: Régimes complémentaires du privé
        # ARRCO
    _P = P.prive
    arrco = ARRCO()
    arrco.set_config(**config)
    plaf_sali = arrco.plaf_sali(workstate, sali) # Distinction cadre/non-cadre avec plafonnement des salaires des cadres

    points_arrco = arrco.nombre_points(plaf_sali)
    maj_arrco = arrco.majoration_enf(plaf_sali, points_arrco, agem) # majoration arrco AVANT éventuelle application de maj/mino pour âge
    coeff_arrco =  arrco.coeff_age(agem, trim)
    val_arrco = _P.complementaire.arrco.val_point 
    pension_arrco = val_arrco * points_arrco * coeff_arrco + maj_arrco
        # AGIRC
    agirc = AGIRC()
    agirc.set_config(**config)
    plaf_sali = agirc.plaf_sali(workstate, sali)
    points_agirc = agirc.nombre_points(plaf_sali)
    coeff_agirc =  agirc.coeff_age(agem, trim)
    maj_agirc = agirc.majoration_enf(plaf_sali, points_agirc, coeff_agirc, agem)  # majoration agirc APRES éventuelle application de maj/mino pour âge
    pension_agirc = _P.complementaire.agirc.val_point*points_agirc # * coeff_agirc + maj_agirc
    
    if to_check == True:
        to_check = {}
        to_check['dec'] = (decote_RG*RG._P.plein.taux).values
        to_check['sur'] = (surcote_RG*RG._P.plein.taux)
        to_check['taux'] = taux_RG.values
        to_check['sam'] = SAM_RG.values
        to_check["pliq_rg"] = pension_RG.values
        to_check['prorat'] = CP_RG.values
        to_check['pts_ar'] = points_arrco
        to_check['pts_ag'] = points_agirc
        to_check['pliq_ar'] = pension_arrco
        to_check['pliq_ag'] = pension_agirc
        to_check['trim_fp'] = trim_FP
        to_check['DA_RG'] = trim_RG//4
        to_check['DA_RG_maj'] = (trim_RG + trim_maj_RG)//4
        #pd.DataFrame(to_check).to_csv('resultat2004.csv')
        return pension_RG, pd.DataFrame(to_check)
    else:
        return pension_RG
