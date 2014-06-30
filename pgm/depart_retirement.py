# -*- coding: utf-8 -*-
import sys
from numpy import maximum, array, ones
from pandas import Series
from CONFIG import path_pension
sys.path.append(path_pension)

from run_pension import run_pension

def depart_retirement(context, yearleg, time_step='year', to_check=False, behavior='taux_plein', cProfile=False):
    ''' cette fonction renvoie un vecteur de booleens indiquant les personnes partant en retraite 
    TODO : quand les comportements de départ seront plus complexes créer les .py associés'''
    if behavior == 'taux_plein':
        dates_tauxplein = run_pension(context, yearleg,
                                             time_step=time_step, to_check=to_check,
                                             output='dates_taux_plein', cProfile=cProfile)
        index_pension = dates_tauxplein['index']
        date_tauxplein = maximum(dates_tauxplein['RSI'], dates_tauxplein['RG'], dates_tauxplein['FP'])
        dates_output = Series(- ones(len(context['id'])), index=context['id'])
        dates_output[index_pension] = date_tauxplein
        return array(dates_output)
        