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
from pension_legislation import PensionParam, PensionLegislation
from simulation import PensionSimulation

def run_pension(context, yearleg, time_step='year', to_check=False, output='pension', cProfile=False):
    ''' run PensionSimulation after having converted the liam context in a PenionData 
        - note there is a selection '''
    sali = context['longitudinal']['sali']
    workstate = context['longitudinal']['workstate']
    # Pour l'instant selection a ce niveau des individus ayant plus de 60 ans pour lancer le calcul des retraites
    datesim = context['period']
    datesim = dt.date(datesim//100, datesim % 100, 1)
    naiss = [datesim - relativedelta(months=x) for x in context['agem']]
    info_ind = pd.DataFrame({'id':context['id'], 'agem': context['agem'],'naiss': naiss, 'sexe' : context['sexe'], 
                              'nb_enf': context['nb_enf'], 'nb_pac': context['nb_pac'], 'nb_enf_RG': context['nb_enf_RG'],
                              'nb_enf_RSI': context['nb_enf_RSI'], 'nb_enf_FP': context['nb_enf_FP'], 'tauxprime': context['tauxprime']})
    info_ind.set_index('id', inplace=True)
    
    if output == 'dates_taux_plein':
        # But: déterminer les personnes partant à la retraite avec préselection des plus de 55 ans
        #TODO: faire la préselection dans Liam
        info_ind  = info_ind.loc[(info_ind['agem'] > 55*12), :] 

    if output == 'pension':
        info_ind = info_ind.loc[context['to_be_retired'], :] #TODO: filter should be done in yaml
    
    workstate = workstate.loc[workstate['id'].isin(info_ind.index), :]
    workstate.set_index('id', inplace=True)
    workstate.sort_index(inplace=True)
    sali = sali.loc[sali['id'].isin(info_ind.index), :]
    sali.set_index('id', inplace=True)
    sali.sort_index(inplace=True)
    sali.fillna(0, inplace=True)
    yearleg = context['period']//100
    if yearleg > 2009: #TODO: remove
        yearleg = 2009
    data = PensionData.from_arrays(workstate, sali, info_ind)
    param = PensionParam(yearleg, data)
    legislation = PensionLegislation(param)
    simul_til = PensionSimulation(data, legislation)
    if cProfile:
        result_til_year = simul_til.profile_evaluate(yearleg, to_check=to_check, output=output)
    else:
        result_til_year = simul_til.main(yearleg, to_check=to_check, output=output)
    return result_til_year