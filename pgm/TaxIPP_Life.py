# -*- coding:utf-8 -*-
'''

Created on 25 Apr 2013

@author: alexis_e
'''
import gc
import time
import pdb
from rpy2.robjects import r

# work on wealth survey (in R)
#TODO: with option to expand and to run from the beginning or not
#phrase = str("source(\"C://Myliam2//Patrimoine//run_all.R\")")
#r(phrase) 
#pdb.set_trace()

# import data to run liam part
import sys
chemin = 'C:\\Myliam2\\'
sys.path.append(chemin+'src')
import main

sys.argv.append('import')
fichier= chemin + 'Model\\import_patrimoine.yml'
sys.argv.append(fichier)
tps_debut = time.clock()   
main.main()
tps_import = time.clock() - tps_debut  
gc.collect()

# run liam 
from simulation import Simulation
fichier= chemin+'Model\\console.yml'
simulation= Simulation.from_yaml( fichier, input_dir=None, input_file=None,
                                output_dir=None, output_file=None)
simulation.run(False)
tps_simul = time.clock() - tps_import
gc.collect()
# convert liam output to OF input
import liam2OF
liam2OF.main()
tps_convert = time.clock() - tps_simul
gc.collect()
# run OF
import of_on_liam
of_on_liam.main()
tps_of= time.clock() - tps_convert 
gc.collect()


total_time = time.clock() - tps_debut



print """
==========================================
 simulation done
==========================================
 * total process lasted %d seconds 
 * %d second in import
 * %d second in simulation
 * %d second in conversion
 * %d second in legislation
==========================================
""" % (total_time, 
       tps_import,
       tps_simul,
       tps_convert,
       tps_of)

#       100*tps_import/total_time,
#       100*tps_simul/total_time,
#       100*tps_convert/total_time,
#       100*tps_of/total_time)