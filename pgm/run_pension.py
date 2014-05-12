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
from SimulPension import first_year_sal, PensionSimulation
from utils_pension import build_naiss, calculate_age, table_selected_dates
from pension_functions import count_enf_born, count_enf_pac

import cProfile

def til_pension(sali, workstate, info_ind, time_step='year', yearsim=2009, example=False):
    command = """run_pension(sali, workstate, info_ind, time_step, yearsim, example)"""
    cProfile.runctx( command, globals(), locals(), filename="profile_pension" + str(yearsim))

def run_pension(sali, workstate, info_ind, time_step='year', yearsim=2009, to_check=False):
    if yearsim > 2009: 
        yearsim = 2009
    Pension = PensionSimulation()
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
    past_dates = sali.columns.tolist()
    sali = np.array(sali)
    workstate = np.array(workstate)
    
    etape0 = time.time()
    if max(info_ind['sexe']) == 2:
        info_ind['sexe'] = info_ind['sexe'].replace(1,0)
        info_ind['sexe'] = info_ind['sexe'].replace(2,1)
    info_ind['naiss'] = build_naiss(info_ind.loc[:,'agem'], dt.date(yearsim,1,1))
    etape1 = time.time()
    workstate = table_selected_dates(workstate, first_year=first_year_sal, last_year=yearsim)
    sali = table_selected_dates(sali, first_year=first_year_sal, last_year=yearsim)
    sali = np.array(sali)
    workstate = np.array(workstate)
    etape2 = time.time()
    config = {'year' : yearsim, 'workstate': workstate, 'sali': sali, 'info_ind': info_ind,
                'param_file' : param_file, 'time_step': time_step}
    Pension.set_config(**config)
    Pension.set_param()
    etape3 = time.time()
     
    # II - Calculs des durées d'assurance et des SAM par régime de base
   
    # II - 1 : Fonction Publique (l'ordre importe car bascule vers RG si la condition de durée minimale de cotisation n'est pas respectée)
    _P = Pension.P.fp
    FP = FonctionPublique(param_regime=_P, param_common=Pension.P.common, param_longitudinal=Pension.P_long)
    FP.set_config(**config)
    trim_cot_FP, trim_actif = FP.trim_service()
    FP.build_age_ref(trim_actif)
    trim_FP = trim_cot_FP #+...
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
    trim_RG = RG.assurance_maj(trim_RG, trim, agem)
    CP_RG = RG.calculate_CP(trim_RG)
    etape7 = time.time()
    # III - 1 : Fonction Publique
    
    # III - 2 : Régime général
    decote_RG = RG.decote(trim, agem)
    trim_by_years = RG.trim_by_years
    surcote_RG = RG.surcote(trim_by_years, trim_maj_RG, agem)
    taux_RG = RG.calculate_taux(decote_RG, surcote_RG)
    pension_RG = SAM_RG*CP_RG*taux_RG
    pension = pension_RG #+
    
    # IV - Pensions de base finales (appication des minima et maxima)
    pension_RG = pension_RG + RG.minimum_contributif(pension_RG, pension, trim_RG, trim_cot, trim)
    pension_surcote_RG = SAM_RG*CP_RG*surcote_RG* RG._P.plein.taux
    pension_RG = RG.plafond_pension(pension_RG, pension_surcote_RG)
    etape8 = time.time()
    # V - Régime complémentaire
    
    # V - 1: Régimes complémentaires du privé
        # ARRCO
    _P = Pension.P.prive
    arrco = ARRCO(param_regime=_P, param_common=Pension.P.common, param_longitudinal=Pension.P_long)
    arrco.set_config(**config)
    arrco.build_sal_regime() # Distinction cadre/non-cadre avec plafonnement des salaires des cadres
    
    points_arrco= arrco.nombre_points()
    maj_arrco = arrco.majoration_enf(points_arrco, agem) # majoration arrco AVANT éventuelle application de maj/mino pour âge
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
    pension_agirc = _P.complementaire.agirc.val_point*points_agirc # * coeff_agirc + maj_agirc
    etape10 = time.time()
    
    for etape in range(10):
        duree = eval('etape'+str(etape+1)) - eval('etape'+str(etape))
        print (u"  La durée de l'étape {} a été de : {} sec").format(etape+1, duree)
    if to_check == True:
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
        to_check['trim_fp'] = trim_FP
        to_check['DA_RG'] = trim_RG//4
        to_check['DA_RG_maj'] = (trim_RG + trim_maj_RG)//4
        #pd.DataFrame(to_check).to_csv('resultat2004.csv')
        return pension_RG, pd.DataFrame(to_check)
    else:
        return pension_RG
