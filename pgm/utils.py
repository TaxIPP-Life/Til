# -*- coding:utf-8 -*-

'''
Created on 5 août 2013
@author: a.eidelman
'''
import numpy as np
from numpy.lib.stride_tricks import as_strided

of_name_to_til = {'ind':'person','foy':'declar','men':'menage', 'fam':'famille'}
til_name_to_of  = dict ( (v,k) for k, v in of_name_to_til.items() )


def concatenated_ranges(ranges_list) :
    ranges_list = np.array(ranges_list, copy=False)
    base_range = np.arange(ranges_list.max())
    base_range =  as_strided(base_range,
                             shape=ranges_list.shape + base_range.shape,
                             strides=(0,) + base_range.strides)
    return base_range[base_range < ranges_list[:, None]]