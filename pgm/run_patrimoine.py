# -*- coding:utf-8 -*-

'''
Created on 2 Apr 2013

@author: alexis_e
'''
import sys
import main #je laisse le main parce que je ne veux pas travailler sur le import, je laisse celui de LIAM2
from simulation import Simulation
import pdb

chemin = 'C:\\Users\\a.eidelman\\Desktop\\GenIPP_Pyth\\liam\\'
chemin = 'M:\\Myliam2\\'
chemin = 'C:\\Myliam2\\'




########### RETRO ############
fichier = chemin+'Patrimoine\\Past\\import.yml'

sys.argv.append('import')
sys.argv.append(fichier)
main.main()
sys.argv.remove(fichier)

fichier = chemin+'Patrimoine\\Past\\retro.yml'

simulation= Simulation.from_yaml(
                                 fichier,
                     input_dir=None,
                    input_file=None,
                    output_dir=None,                    
                    output_file=None)
simulation.run(False)



fichier = chemin+'Patrimoine\\lien_parent_enfant\\match_par_enf.yml'
fichier = chemin+'Patrimoine\\lien_parent_enfant\\expand.yml'
fichier = chemin+'Patrimoine\\lien_parent_enfant\\match_par_enf.yml'
fichier = chemin+'Patrimoine\\lien_parent_enfant\\match_score.yml'

#simulation= Simulation.from_yaml(
#                                 fichier,
#                     input_dir=None,
#                    input_file=None,
#                    output_dir=None,                    
#                    output_file=None)
#simulation.run(False)
#
#sys.argv.append('import')
#fichier = chemin+'Patrimoine\\lien_parent_enfant\\import.yml'
#sys.argv.append(fichier)
#main.main()
#print sys.argv
#
#fichier = chemin+'Patrimoine\\lien_parent_enfant\\match_par_enf.yml'
#fichier = chemin+'Patrimoine\\lien_parent_enfant\\expand.yml'
#fichier = chemin+'Patrimoine\\lien_parent_enfant\\match_par_enf.yml'
#fichier = chemin+'Patrimoine\\lien_parent_enfant\\match_score.yml'
#
#simulation= Simulation.from_yaml(
#                                 fichier,
#                     input_dir=None,
#                    input_file=None,
#                    output_dir=None,                    
#                    output_file=None)
#simulation.run(False)
