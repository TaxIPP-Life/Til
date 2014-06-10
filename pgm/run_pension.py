# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import sys
import datetime as dt
from dateutil.relativedelta import relativedelta
import time


from CONFIG import path_pension
sys.path.append(path_pension)

from pension_data import PensionData
from simulation import PensionSimulation

def run_pension(context, yearleg, time_step='year', to_check=False, cProfile=False):
    ''' run PensionSimulation after having converted the liam context in a PenionData 
        - note there is a selection '''
    
    sali = context['longitudinal']['sali']
    workstate = context['longitudinal']['workstate']
    # Pour l'instant selection a ce niveau des individus ayant plus de 60 ans pour lancer le calcul des retraites
    datesim = context['period']
    datesim = dt.date(datesim//100, datesim % 100, 1)
    naiss = [datesim - relativedelta(months=x) for x in context['agem']]
    info_ind = pd.DataFrame({'id':context['id'], 'agem': context['agem'],'naiss': naiss, 'sexe' : context['sexe'], 
                              'nb_born': context['nb_born'], 'nb_pac': context['nb_pac']})
    #print (context.keys())
    info_ind  = info_ind.loc[(info_ind['agem'] > 708), :] # 708 = 59 *12
    info_ind.set_index('id', inplace=True)
    workstate = workstate.loc[workstate['id'].isin(info_ind.index), :]
    workstate.set_index('id', inplace=True)
    sali = sali.loc[sali['id'].isin(info_ind.index), :]
    sali.set_index('id', inplace=True)
    data = PensionData.from_arrays(workstate, sali, info_ind)
    
    simul_til = PensionSimulation(data)
    if cProfile:
        result_til_year = simul_til.profile_main(yearleg, to_check=to_check)
    else:
        result_til_year = simul_til.main(yearleg, to_check=to_check)
    return result_til_year