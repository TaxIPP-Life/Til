# -*- coding:utf-8 -*-

'''
Created on 2 Apr 2013

@author: alexis_e
'''
import sys

import src_liam
import src_liam.main as main 
#from src_liam.simulation import Simulation

########### LIEN ############
chemin = 'C:\\til\\'
fichier = chemin+'data\\Patrimoine\\lien_parent_enfant\\import.yml'

sys.argv.append('import')
sys.argv.append(fichier)
main.main()
sys.argv.remove(fichier)

fichier = chemin+'data\\Patrimoine\\lien_parent_enfant\\match_score.yml'

simulation= src_liam.simulation.Simulation.from_yaml(
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

