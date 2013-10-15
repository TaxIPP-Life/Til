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
import random


def _varname_or_index(word, list_col):
    if list_col is None: 
        return  "'" + word + "'" 
    else:
        return str(list_col.index(word))
   
                                
def _rewrite_score(score, first, other, list_col1=None, list_col2=None ):
    '''
    Return a new string and a list of variables of other
    If list_columns is not None, change name by position, useful for the use of numpy
    '''    
       
    other_var = []
        
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
                    word = word[6:]
                    other_var +=  [_varname_or_index(word, None)[1:-1]]
                    word = _varname_or_index(word, list_col2)
                    word = other + "[:," +  word + "]"
                else: 
                    word = _varname_or_index(word, list_col1)
                    word = first + "[" +  word + "]"
        final += word
    return final, list(set(other_var)) #astuce pour avoir des valeurs uniques

class Matching(object):
    #TODO: Faire des sous classes Matching_cells et matching_simple, ce serait plus propre 
    
    '''
    Comment réaliser un matching de la table 1 et de la table 2
    method : fafs : 'first arrived, first served
    '''
    def __init__(self, table1, table2, score):
        self.table1 = table1
        self.table2 = table2
                    
        if table2.columns.tolist() != table1.columns.tolist():
            raise Exception("Les variables doivent être les mêmes dans les deux tables")
        if not isinstance(score, basestring):
            raise Exception("Le score doit être un caractere, désolé")
        self.score_str = score



    def evaluate(self, orderby, method):
        table2 = self.table2
        index_init = self.table1.index 
        if orderby is not None:
            table1 = self.table1.sort(orderby)
        else:
            table1 = self.table1.loc[np.random.permutation(index_init)]
        index_init = table1.index
        
        table2 = self.table2.fillna(0)
        table1 = self.table1.fillna(0)
             
        if len(table1)>len(table2):
            print ("WARNING : La table de gauche doit être la plus petite, "\
                "traduire le score dans l'autre sens et changer l'ordre." \
                "pour l'instant table1 est reduite à la taille de table2. ")
            table1 = table1[:len(table2)]
        index_modif = table1.index    
        

        score_str, vars = _rewrite_score(self.score_str, 'temp', 'table2', table1.columns.tolist(), table2.columns.tolist())    
        n = len(table1)       
        
        if method=='cells':
            groups2 = table2.groupby(vars)
            cells_ini = pd.DataFrame(groups2.groups.keys(),columns =vars)
            score_str, vars = _rewrite_score(self.score_str, 'temp', 'cells', table1.columns.tolist(), vars)
            size = pd.DataFrame(groups2.size(), columns = ['size'])
            if len(size) != len(cells_ini): 
                raise Exception('Oups, il y a un problème de valeurs nulles a priori')
            cells_ini = cells_ini.merge(size, left_on=vars, right_index=True, how='left')
            cells_ini['id'] = cells_ini.index
            # conversion en numpy
            table1 = np.array(table1, dtype=np.int32)
            cells = np.array(cells_ini, dtype=np.int32) 
            #definition de la boucle
            nvar = len(vars)-1
            
        else:
            # conversion en numpy
            table1 = np.array(table1, dtype=np.int32)
            table2 = np.array(table2, dtype=np.int32)  
            
                                  
#         #definition de la boucle
#         def real_match_cell(k, cells):
#             temp = table1[k]
#             score = eval(score_str)
#             idx = score.argmax()
#             idx2 = cells[idx,nvar+2]
#             match[k] = idx2 
#             cells[idx,nvar+1] -= 1
#             if cells[idx,nvar+1]==0:
#                 print idx
#                 cells = np.delete(cells, idx, 0)      
#             if cells[idx,nvar+1]<=0:
#                 pdb.set_trace()      
#         def real_match_simple(k, table2):
#             temp = table1[k]
#             score = eval(score_str)
#             idx = score.argmax()
#             idx2 = cells[idx,nvar+1]
#             match[k] = idx2 
#             table2 = np.delete(table2, idx, 0)

        match = np.empty(n, dtype=int)
        percent = 0
        start = time.clock()
        #check
        assert  cells[:,nvar+1].min() > 0
        if method=='cells':
            for k in xrange(n):   
#                 real_match_cells(k,cells)
                temp = table1[k]
                try: 
                    score = eval(score_str)
                except:
                    pdb.set_trace()
                try:
                    idx = score.argmax()
                except:
                    pdb.set_trace()
                idx2 = cells[idx,nvar+2]
                match[k] = idx2 
                cells[idx,nvar+1] -= 1
                if cells[idx,nvar+1]==0:
                    cells = np.delete(cells, idx, 0)     
                # update progress bar
                percent_done = (k * 100) / n
                to_display = percent_done - percent
                if to_display:
                    chars_to_write = list("." * to_display)
                    offset = 9 - (percent % 10)
                    while offset < to_display:
                        chars_to_write[offset] = '|'
                        offset += 10
                    sys.stdout.write(''.join(chars_to_write))
                percent = percent_done 
                
                   
        else:
            for k in xrange(n):   
#                 real_match_simple(k,table2)
                temp = table1[k]
                score = eval(score_str)
                idx = score.argmax()
                idx2 = cells[idx,nvar+1]
                match[k] = idx2 
                table2 = np.delete(table2, idx, 0)
                # update progress bar
                percent_done = (k * 100) / n
                to_display = percent_done - percent
                if to_display:
                    chars_to_write = list("." * to_display)
                    offset = 9 - (percent % 10)
                    while offset < to_display:
                        chars_to_write[offset] = '|'
                        offset += 10
                    sys.stdout.write(''.join(chars_to_write))
                percent = percent_done                              
        
        match = pd.Series(match, index = index_modif).sort_index()
        if method == 'cells':
            match_ini = match.copy()
            match_count = match.value_counts()
            for group in match_count.index: 
                nb_selected = match_count[group]
                keys_group = cells_ini.loc[group,vars].tolist()
                try: 
                    match[match_ini==group] = groups2.groups[tuple(keys_group)][:nb_selected]
                except:
                    pdb.set_trace()
        print 'temps dédié au real_matching :', time.clock() - start  
        
        assert match.nunique() == len(match)
        return match
