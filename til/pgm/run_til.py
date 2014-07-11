# -*- coding:utf-8 -*-

from til.CONFIG import path_liam, path_model
import sys
import os
from til import __path__ as path_til

sys.path.append(path_liam)
from src.simulation import Simulation

fichier = path_model + '/console.yml'
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