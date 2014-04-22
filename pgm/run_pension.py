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

import os
import pandas as pd
import pickle
import sys

from CONFIG import path_pension

sys.path.append(path_pension)
from Regimes.Regime_general import Regime_general 
from SimulPension import PensionSimulation

    
def run_pension(sali, workstate, info_ind, info_child_father, info_child_mother, yearsim = 2009, example=False):
    Pension = PensionSimulation()
    # I - Chargement des paramètres de la législation (-> stockage au format .json type OF) + des tables d'intéret
    # Pour l'instant on lance le calcul des retraites pour les individus ayant plus de 62 ans (sélection faite dans exprmisc de Til\liam2)
    param_file = path_pension + '\\France\\param.xml' #TODO: Amelioration
    if example:
        param_file =  path_pension +'param_example.xml'

    config = {'year' : yearsim, 'workstate': workstate, 'sali': sali, 'info_ind': info_ind,
                'info_child_father': info_child_father, 'info_child_mother': info_child_mother, 'param_file' : param_file, 'time_step': 'year'}
    info_child_father.to_csv('info.csv')
    Pension.set_config(**config)
    Pension.set_param()
    # II - Calculs des durées d'assurance et des SAM par régime 
    
    # II - a : Régime Général
    _P =  Pension.P.RG.ret_base
    RG = Regime_general(param_regime = _P, param_common = Pension.P.common, param_longitudinal = Pension.P_long)
    RG.set_config(**config)
    RG.load()

    trim_cot_RG = RG.nb_trim_cot()
    trim_ass_RG = RG.nb_trim_ass()
    trim_maj_RG = RG.nb_trim_maj()
    trim_RG = trim_cot_RG + trim_ass_RG + trim_maj_RG
    
    SAM_RG = RG.SAM()
    
    # II - b : Fonction Publique
    
    
    # III - Calculs des pensions tous régimes confondus 
    trim_cot = trim_cot_RG #+
    trim = trim_RG #+
    agem = info_ind['agem']
    trim_by_years = RG.trim_by_years
    
    trim_RG = RG.assurance_maj(trim_RG, trim, agem)
    CP_RG = RG.calculate_CP(trim_RG)
    
    # III - 1 : Régime général
    decote_RG = RG.decote(trim, agem)
    surcote_RG = RG.surcote(trim_by_years, trim_maj_RG, agem)
    taux_RG = RG.calculate_taux(decote_RG, surcote_RG)
    assert max(taux_RG) < 1
    assert max(CP_RG) <= 1
    pension_RG = SAM_RG * CP_RG * taux_RG

    pension = pension_RG #+
    
    # IV - Pensions minimales et maximales
    pension_RG = pension_RG + RG.minimum_contributif(pension_RG, pension, trim_RG, trim_cot, trim)
    pension_surcote_RG = SAM_RG * CP_RG * surcote_RG * RG._P.plein.taux
    pension_RG = RG.plafond_pension(pension_RG, pension_surcote_RG)
    import pdb
    pdb.set_trace()
    return Pension.P

if __name__ == '__main__':    
    # Comparaison des résultats avec PENSIPP
    import pandas.rpy.common as com
    from rpy2 import robjects as r
    r.r("load('database.RData')")
    workstate = com.load_data('database')
    sali = com.load_data('database')
    info_ind = com.load_data('database')