# -*- coding:utf-8 -*-

'''
Created on 29 juil. 2013
@author: Alexis
'''


from CONFIG import path_til_liam
import sys
import pdb
sys.path.append(path_til_liam)

from src_liam.exprbases import FunctionExpression #Filtered? 


class Pension(FunctionExpression): #TODO: switch to filter ? 
    '''
    Class pour définir toutes les pensions,
    très en cours de réflexion
    '''
    #copié de alignment pour l'instant et donc à changer
    # si on veut juste écrire retraite(), ou retraite(regime=) etc, adapter
    def __init__(self, score=1, need = None,
                 filter=None, take=None, leave=None):
        print score
        self.regime = 'general'
        
        
    def collect_variable(self):
        raise NotImplementedError()
        #pour chaque système collecter les variables qui correspondent
        #on peut peut-être déjà prévoir de charger les données du registre
        
    def calculate(self):
        pdb.set_trace()
        raise NotImplementedError()
        # applique la formule
        
    def collect_parameter(self, param):

        # import read_param
        
        def _linear(self, liste):
            '''
            pour chaque éléments successifs age1,age2, 
            renvoie une fonction linéraire qui donne la valeur
            val1 si age<= age1, 
            val2 si age>= age2
            val1 + (age-age1)*(val2-val1)/(age2-age1) sinon
            ie la droite linéaire si age est entre age1 et age2.
            '''
            val = 0
            return val #un vecteur de taille ind
        
        raise NotImplementedError()

class Reversion(Pension):
    def __init__(self, score, need = None,
                 filter=None, take=None, leave=None,
                 expressions=None, possible_values=None,
                 errors='default', frac_need='uniform',
                 method='default', periodicity_given='year',
                 link=None, secondary_axis=None):
        super(Pension, self).__init__(score, filter) 
            
            