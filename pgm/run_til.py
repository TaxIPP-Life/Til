# -*- coding:utf-8 -*-

from CONFIG import path_liam, path_til, path_model 
import sys 

sys.path.append(path_liam)
from src.simulation import Simulation

fichier = path_til + 'Model\\console_futur.yml'
fichier = path_model + 'console.yml'


simulation = Simulation.from_yaml( fichier,
                    input_dir = None,
                    input_file = None,
                    output_dir = path_til + 'output',                    
                    output_file = None)
simulation.run(False)

# import cProfile
# command = """simulation.run(False)"""
# cProfile.runctx( command, globals(), locals(), filename="OpenGLContext.profile1")