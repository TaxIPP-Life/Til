# -*- coding: utf-8 -*-
import pandas as pd
import sys
import datetime as dt
from dateutil.relativedelta import relativedelta

from numpy import array, around
import time


from til_pension.pension_data import PensionData
from til_pension.pension_legislation import PensionParam, PensionLegislation
from til_pension.simulation import PensionSimulation
from utils import output_til_to_liam

def get_pension(context, yearleg):
    ''' return a PensionSimulation '''
    sali = context['longitudinal']['sali']
    workstate = context['longitudinal']['workstate']
    # calcul de la date de naissance au bon format
    datesim = context['period']
    datesim_in_month = 12*(datesim // 100) + datesim % 100
    datenaiss_in_month = datesim_in_month - context['agem']
    naiss = 100*(datenaiss_in_month // 12) + datenaiss_in_month % 12 + 1
    naiss = pd.Series(naiss)
    naiss = pd.Series(naiss).map(lambda t: dt.date(t // 100, t % 100, 1))

    info_ind = pd.DataFrame({'index':context['id'], 'agem': context['agem'],'naiss': naiss, 'sexe' : context['sexe'],
                              'nb_enf_all': context['nb_enf'], 'nb_pac': context['nb_pac'], 'nb_enf_RG': context['nb_enf_RG'],
                              'nb_enf_RSI': context['nb_enf_RSI'], 'nb_enf_FP': context['nb_enf_FP'], 'tauxprime': context['tauxprime']})
    info_ind = info_ind.to_records(index=False)

    workstate = workstate.loc[workstate['id'].isin(info_ind.index), :].copy()
    workstate.set_index('id', inplace=True)
    workstate.sort_index(inplace=True)
    sali = sali.loc[sali['id'].isin(info_ind.index), :].copy()
    sali.set_index('id', inplace=True)
    sali.sort_index(inplace=True)
    sali.fillna(0, inplace=True)
    
    data = PensionData.from_arrays(workstate, sali, info_ind)
    param = PensionParam(yearleg, data)
    legislation = PensionLegislation(param)
    simulation = PensionSimulation(data, legislation)
    simulation.set_config()  
    return simulation