# -*- coding: utf-8 -*-
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
        date_tauxplein = maximum(dates_taux_plein.values())
        return (yearleg - dates_tauxplein//100 > 0)
        