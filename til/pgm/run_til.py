# -*- coding:utf-8 -*-

from til.CONFIG import path_liam
import sys
import os
import pkg_resources

path_til = os.path.join(
    pkg_resources.get_distribution('Til').location,
    "til"
    )
path_pension = os.path.join(
    pkg_resources.get_distribution("Til-Pension").location,
    "til_pension",
    )
path_model = os.path.join(
    pkg_resources.get_distribution("Til-BaseModel").location,
    "til_base_model",
    )

sys.path.append(path_liam)

from liam2.simulation import Simulation

console_file = os.path.join(path_model, 'console.yml')
output_dir = os.path.join(path_til[0], 'output')

simulation = Simulation.from_yaml(console_file,
                    input_dir = None,
                    input_file = None,
                    output_dir = output_dir,
                    output_file = None)
simulation.run(False)

# import cProfile
# command = """simulation.run(False)"""
# cProfile.runctx( command, globals(), locals(), filename="OpenGLContext.profile1")
