from __future__ import print_function
# -*- coding:utf-8 -*-


import pandas as pd


from til.CONFIG import path_liam, path_model
import sys
import os
from til import __path__ as path_til

sys.path.append(path_liam)
from src.simulation import Simulation


fichier = path_liam + '/tests/functional/matching_optimization.yml'
fichier = path_model + '/console.yml'
print (fichier)

output_dir = os.path.join(path_til[0], 'output')
  
simulation = Simulation.from_yaml( fichier,
                    input_dir = None,
                    input_file = None,
                    output_dir = output_dir,
                    output_file = None)
simulation.run(False)
  
# import cProfile
# command = """simulation.run(False)"""
# cProfile.runctx( command, globals(), locals(), filename="OpenGLContext.profile1")

# import pdb
#  
# output_dir = os.path.join(path_til[0], 'output', 'prob2.csv')
# prob = pd.read_csv(output_dir)
# prob['conj'].isin(prob['id'])
# prob['id'].isin(prob['conj'])
# test = prob.merge(prob, left_on='id', right_on='conj', suffixes=('','_c')) 
# test[['id','conj','conj_c','id_c','men','men_c']].head()
# pdb.set_trace()
# all(test['id'] == test['conj_c'])
# all(test['conj'] == test['id_c'])
# all(test['men'] == test['men_c'])
# all(test['to_divorce'] == test['to_divorce_c'])
# to_div = test[test['to_divorce']]
# all(to_div['conj'].isin(to_div['id']))
# all(to_div['id'].isin(to_div['conj']))
# all(to_div['id'] == to_div['conj_c'])
# all(to_div['conj'] == to_div['id_c'])
# all(to_div['men'] == to_div['men_c'])
# all(to_div['to_divorce'] == to_div['to_divorce_c'])
# cond = to_div['men'] != to_div['men_c']
# sum(to_div['to_divorce'] & (to_div['men'] == to_div['men_c']))
# sum(test['to_divorce'] & (test['men'] == test['men_c']))
# 
# sum(test['to_divorce'] & (test['civilstate'] != test['civilstate']))