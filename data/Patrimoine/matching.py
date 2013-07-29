# -*- coding:utf-8 -*-
'''
Created on 23 juil. 2013
Alexis Eidelman
'''
import pdb
import string
import pandas as pd
import time
import numpy as np
import sys

def rewrite_score(score, first, other ):
    list_words = score.replace("(", "( ").replace(")", " )").split()
    final = ''
    exclude = set(string.punctuation)
    for word in list_words:
        if len(word) > 1:
            try: 
                exclude = set(string.punctuation)
                word2 = ''.join(ch for ch in word if ch not in exclude)
                float(word2)
            except:     
                if 'other.' in word:
                    word = word.replace('other.', other + "['")  + "']"
                else: 
                    word = first + "['" +  word + "']"
        final = final + word
    return final

class Matching(object):
    '''
    Comment réaliser un matching de la table 1 et de la table 2
    method : fafs : 'first arrived, first served
    '''
    def __init__(self, table1, table2, score, orderby= None, method='fafs'):
        self.table1 = table1
        self.table2 = table2
        if not isinstance(score, basestring):
            raise Exception("Le score doit être un caractere, désolé")
        self.score_str = score
        if orderby is not None and method != 'fafs':
            print 'normalement ça ne sert à rien là'
        self.orderby = orderby
        self.method = method

    def evaluate(self):
        if self.orderby is not None:
            self.table1.sort(self.orderby)
        if self.method == 'fafs':
            score_str = rewrite_score(self.score_str, 'temp', 'table2')         
            #boucle:
            debut_match = time.clock()


            percent = 0
            table2 = self.table2.fillna(0)
            table1 = self.table1.fillna(0)
            match = pd.Series(0, index=self.table1.index)
            index2 = pd.Series(True, index=self.table2.index)  
            
            
            k_max = min(len(table2), len(table1))
            def matching():
                for k in xrange(k_max):   
                    temp = table1.iloc[k] 
                    score = eval(score_str)[index2]
                    idx2 = score.idxmax()
                    match.iloc[k] = idx2 # print( k, 0, index2)
                    index2[idx2] = False
                
            pdb.set_trace()
#                 # update progress bar
#                 percent_done = (k * 100) / k_max
#                 to_display = percent_done - percent
#                 if to_display:
#                     chars_to_write = list("." * to_display)
#                     offset = 9 - (percent % 10)
#                     while offset < to_display:
#                         chars_to_write[offset] = '|'
#                         offset += 10
#                     sys.stdout.write(''.join(chars_to_write))
#                 percent = percent_done
#                 
            import cProfile
            command = """matching()"""
            cProfile.runctx( command, globals(), locals(), filename="matching.profile1" )
            
            pdb.set_trace()

            print 'temps dédié au matching :', time.clock() - debut_match  
            return match
          
        else: 
            NotImplementedError()

                   

if __name__ == '__main__':
    score = "- 1 * (other.anais - anais) - 1.0 * (other.situa - situa) - 0.5 * (other.sexe - sexe) - 1.0 * (other.dip6 - dip6) - 1.0 * (other.nb_enf - nb_enf)"
    match = Matching(None, None, score)
