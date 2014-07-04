
from pandas import HDFStore, merge # DataFrame
import numpy as np
import pdb
import time


path_data = 'C:\\Til-BaseModel\\Destinie.h5'


# output = HDFStore(calc)
simul = HDFStore(path_data)

nom = 'register'
base = 'entities/'+nom
register = simul[str(base)]

futur = simul['entities/futur']
init = simul['entities/person']

pdb.set_trace()
list_id_temp = init['id']

year = 2010
cond_period = futur['period'] == 2010
futur_period = futur[cond_period]
list_id = futur_period['id']

set_1 = list_id.isin(list_id_temp)
not_in_previous = futur_period[~set_1]
assert sum(not_in_previous['naiss'] != year) == 0

set_2 = list_id_temp.isin(list_id)
not_in_futur = init[~set_2]