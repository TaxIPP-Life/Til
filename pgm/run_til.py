# -*- coding:utf-8 -*-

# on dit à la fonction main (qui est ce qui est appelé avec f5 ou f6 d'être utilisée avec les paramètres que l'on donne à sys.argv

from CONFIG import path_of, path_til_liam, path_til
import sys 

sys.path.append(path_til_liam)
sys.path.append(path_of)


from src_liam.importer import file2h5 #je laisse le main parce que je ne veux pas travailler sur le import, je laisse celui de LIAM2
from src_liam.simulation import Simulation

#import

# fichier = path_til + '\\model\\import_retro.yml'
# fichier = path_til + '\\model\\import_PatrimoineR.yml'
# fichier= path_til_liam + '\\tests\\functional\\import.yml'
# file2h5(fichier, 
#         input_dir=path_til + '\\data\\Patrimoine\\')


fichier= path_til + 'Model\\console.yml'
#fichier=  path_til + '\\Model\\console_retro.yml'

# fichier= 'path_til + '\\tests\\functional\\simulation.yml'

simulation= Simulation.from_yaml( fichier,
                     input_dir=None,
                    input_file=None,
                    output_dir=path_til + '//output',                    
                    output_file=None)

simulation.run(False)

# import cProfile
# command = """simulation.run(False)"""
# cProfile.runctx( command, globals(), locals(), filename="OpenGLContext.profile1" )
