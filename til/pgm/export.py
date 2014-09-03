# -*- coding:utf-8 -*-
'''
Created on 10 août 2014

@author: alexis
'''

from pandas import HDFStore, DataFrame, merge # DataFrame
import numpy as np
import pdb
import time
from utils import til_name_to_of
temps = time.clock()
simul = "C:\\Til\\til\\output\\simul.h5"
# output = HDFStore(calc)
simul = HDFStore(simul)
nom = 'register'
base = 'entities/'+nom
register = simul[str(base)]
indiv = register['id'].unique()
reg_ind = register.groupby('id')
naiss = reg_ind.max()['naiss']
deces = reg_ind.max()['deces']
duree_vie = (deces>0) * (deces - naiss)
duree_vie_freq = duree_vie.value_counts()
print duree_vie_freq
pdb.set_trace()
table = {}
nom = 'person'
base = 'entities/'+nom
ent = til_name_to_of[nom]
table[ent] = simul[str(base)]
table[ent] = table[ent].rename(columns={'men': 'idmen', 'foy': 'idfoy', 'id': 'noi'})
# liste des donnees temporaire que l on peut supprimer
# anc, expr, education_level, nb_children_ind, dur_separated, dur_in_couple, agegroup_civilstate, agegroup_work
# quifoy, idfoy, quimen, idmen, wpr;_init
# get years
years = np.unique(table[ent]['period'].values)
# get individuals
ids = np.unique(table[ent]['noi'].values)
#typemap = {bool: int, int: int, float: float}
#res_type = typemap[dtype(expr, context)]
res_size = len(ids)
#
#sum_values = np.zeros((res_size,4), dtype=float)
#for ind in ids:
# x = table[ent][table[ent]['noi']==ind][['sali','rsti','choi']].sum().values
# sum_values[ind,1:] = x
# sum_values[ind,0] = ind
list2drop = ['wprm_init','age','idmen','idfoy','quifoy', 'pere','mere','conj','dur_in_couple','dur_out_couple',
'education_level','productivity']
list2keep = ['sexe','noi','findet','civilstate','workstate','sali','rsti','choi','xpr','anc'] # 'unempdur5','inacdur5','invaldur5'
#tab = table[ent].drop(list2drop, axis=1)
tab = table['ind'][list2keep]
indiv = tab.groupby(['noi'],sort=False)
cumul = indiv.sum()
nb_obs = indiv.size()
moyenne = cumul.div(nb_obs,axis=0)
# nombre d'annee dans chaque etat.
workstate = tab.groupby(['noi','workstate'],sort=False).size()
civilstate = tab.groupby(['noi','civilstate'],sort=False).size()
passage = table['ind'][['noi','period','idmen','idfoy','quifoy','quimen']]
pdb.set_trace()
## donnee menage
tabm = output['men']
tabm = merge(passage, tabm , how='right', on=['period','idmen'], sort=False)
tabm[['ndvdisp','ndvini','ndvnet']] = tabm[['revdisp','revini','revnet']].div(tabm['uc'],axis=0)
menag = tabm.groupby(['noi'],sort=False)
cumul = menag.sum()
nb_obs = menag.size()
moyenne = cumul.div(nb_obs,axis=0)
decile = tabm.groupby(['noi','decile'],sort=False).size()
df = DataFrame(columns=list(list2keep))
pdb.set_trace()
df.to_Stata("C:\Simulation\simul.dta")
simul.close()
output.close()