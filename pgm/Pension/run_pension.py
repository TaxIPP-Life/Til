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


from pgm.CONFIG import path_til
from pgm.Pension.Param import legislations_add_pension as legislations
from pgm.Pension.Param import legislationsxml_add_pension as  legislationsxml
from openfisca_core import conv
from pgm.Pension.Model.Regime_general import Regime_general 
#from .columns import EnumCol, EnumPresta
#from .taxbenefitsystems import TaxBenefitSystem
from pgm.Pension.SimulPension import PensionSimulation
import openfisca_france
openfisca_france.init_country()

    
def run_pension(sali, workstate, info_ind, info_child_father, info_child_mother, yearsim = 2009, example=False):
    Pension = PensionSimulation()
    # I - Chargement des paramètres de la législation (-> stockage au format .json type OF) + des tables d'intéret
    # Pour l'instant on lance le calcul des retraites pour les individus ayant plus de 62 ans (sélection faite dans exprmisc de Til\liam2)
    param_file = path_til + 'pgm\\Pension\\Param\\' + 'param.xml' #TODO: Amelioration
    if example:
        param_file =  'param_example.xml'

    config = {'year' : yearsim, 'workstate': workstate, 'sali': sali, 'info_ind': info_ind,
                'info_child_father': info_child_father, 'info_child_mother': info_child_mother, 'param_file' : param_file, 'time_step': 'year'}
    Pension.set_config(**config)
    Pension.set_param()
    # II - Lancement des calculs
    # II/a - Régime général
    _P =  Pension.P.RG.ret_base
    RG = Regime_general(param_regime = _P, param_common = Pension.P.common, param_longitudinal = Pension.P_long)
    RG.set_config(**config)
    RG.load()
    
    # a.1 - Coefficient de proratisation
        # Nombre de trimestres côtisés
    trim_cot_RG = RG.nb_trim_cot()
    trim_ass_RG = RG.nb_trim_ass()
    trim_maj_RG = RG.nb_trim_maj()
    trim_RG = trim_cot_RG + trim_ass_RG + trim_maj_RG
    #CP = RG.calculate_CP(trim_RG)
    
    # a.2 - Calcul du SAM
    SAM_RG = RG.SAM()
    import pdb
    pdb.set_trace()
    return Pension.P

if __name__ == '__main__':    
    # 0 - Préparation de la table d'input
    filename = os.path.join(path_til + 'model', 'Destinie.h5')
    table = pd.read_hdf(filename, 'entities//person')
    past = pd.read_hdf(filename, 'entities//past')
    futur = pd.read_hdf(filename, 'entities//futur')
    
    dic_rename = {'id': 'ind', 'foy': 'idfoy', 'men': 'idmen'}
    table = table.rename(columns = dic_rename)
    table[['quifam', 'idfam']] = table[['quimen', 'idmen']]
    
    P = run_pension(past, futur) #, example=True)
    _P = P.RG.ret_base
    print "\n Exemple de variable type 'Paramètre' : "
    print _P.age_max
    print "\n Exemple de variable type 'Génération' :"
    print _P.age_min, _P.age_min.control
    print "\n Exemple de variable type 'Barème' :"
    print _P.deduc
    
