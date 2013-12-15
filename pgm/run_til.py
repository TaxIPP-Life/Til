# -*- coding:utf-8 -*-

from CONFIG import path_til_liam, path_til
import sys 

sys.path.append(path_til_liam)
from src.simulation import Simulation


fichier= path_til + 'Model\\console.yml'
#fichier=  path_til + '\\Model\\console_retro.yml'
# fichier= 'path_til + '\\tests\\functional\\simulation.yml'

simulation = Simulation.from_yaml( fichier,
                     input_dir=None,
                    input_file=None,
                    output_dir=path_til + '//output',                    
                    output_file=None)

simulation.run(False)

# import cProfile
# command = """simulation.run(False)"""
# cProfile.runctx( command, globals(), locals(), filename="OpenGLContext.profile1" )
