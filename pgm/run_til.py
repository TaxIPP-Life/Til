# -*- coding:utf-8 -*-

# on dit à la fonction main (qui est ce qui est appelé avec f5 ou f6 d'être utilisée avec les paramètres que l'on donne à sys.argv

from src_liam.importer import  file2h5 #je laisse le main parce que je ne veux pas travailler sur le import, je laisse celui de LIAM2
from src_liam.simulation import Simulation

chemin = 'C:\\til\\'
#import

fichier = chemin + 'model\\import_retro.yml'
fichier = chemin + 'model\\import_PatrimoineR.yml'
file2h5(fichier)


fichier= chemin+'Model\\console.yml'
#fichier= chemin+'Model\\console_retro.yml'
# fichier= 'C:\\Myliam2\\tests\\functional\\simulation.yml'

simulation= Simulation.from_yaml(
                                 fichier,
                     input_dir=None,
                    input_file=None,
                    output_dir=None,                    
                    output_file=None)

simulation.run(False)

# import cProfile
# command = """simulation.run(False)"""
# cProfile.runctx( command, globals(), locals(), filename="OpenGLContext.profile1" )
