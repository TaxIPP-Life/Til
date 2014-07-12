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


def run_pension(context, yearleg, time_step='year', to_check=False, output='pension', cProfile=False):
    ''' run PensionSimulation after having converted the liam context in a PenionData
        - note there is a selection '''
    sali = context['longitudinal']['sali']
    workstate = context['longitudinal']['workstate']
    # calcul de la date de naissance au bon format
    datesim = context['period']
    age_year = context['agem'] // 12
    age_month = context['agem'] % 12 + 1
    naiss_year = datesim // 100 - age_year
    naiss_month = datesim % 100 - age_month + 1
    naiss = pd.Series(naiss_year * 100 + naiss_month)
    naiss = naiss.map(lambda t: dt.date(t // 100, t % 100, 1))

    info_ind = pd.DataFrame({'index':context['id'], 'agem': context['agem'],'naiss': naiss, 'sexe' : context['sexe'],
                              'nb_enf_all': context['nb_enf'], 'nb_pac': context['nb_pac'], 'nb_enf_RG': context['nb_enf_RG'],
                              'nb_enf_RSI': context['nb_enf_RSI'], 'nb_enf_FP': context['nb_enf_FP'], 'tauxprime': context['tauxprime']})
    info_ind = info_ind.to_records(index=False)
    # TODO: filter should be done in liam
    if output == 'dates_taux_plein':
        # But: déterminer les personnes partant à la retraite avec préselection des plus de 55 ans
        #TODO: faire la préselection dans Liam
        info_ind = info_ind[(info_ind['agem'] > 55 * 12)]

    if output == 'pension':
        info_ind = info_ind[context['to_be_retired']] #TODO: filter should be done in yaml

    workstate = workstate.loc[workstate['id'].isin(info_ind.index), :].copy()
    workstate.set_index('id', inplace=True)
    workstate.sort_index(inplace=True)
    sali = sali.loc[sali['id'].isin(info_ind.index), :].copy()
    sali.set_index('id', inplace=True)
    sali.sort_index(inplace=True)
    sali.fillna(0, inplace=True)
    yearleg = context['period'] // 100
    if yearleg > 2009: #TODO: remove
        yearleg = 2009
    data = PensionData.from_arrays(workstate, sali, info_ind)
    param = PensionParam(yearleg, data)
    legislation = PensionLegislation(param)
    simul_til = PensionSimulation(data, legislation)
    if cProfile:
        result_til_year = simul_til.profile_evaluate(yearleg, to_check=to_check, output=output)
    else:
        result_til_year = simul_til.evaluate(yearleg, to_check=to_check, output=output)
    if output == 'dates_taux_plein':
        # Renvoie un dictionnaire donnant la date de taux plein par régime (format numpy) et l'index associé
        return result_til_year
    elif output == 'pension':
        result_to_liam = output_til_to_liam(output_til=result_til_year,
                                            index_til=info_ind.index,
                                            context_id=context['id'])
        return result_to_liam.astype(float)