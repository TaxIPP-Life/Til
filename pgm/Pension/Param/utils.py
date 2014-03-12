# -*- coding:utf-8 -*-
import numpy as np
import pandas as pd
from xml.dom import minidom
from datetime import datetime, date, timedelta

def multiple_lists(element, date):
    seuils = []
    values = []
    for val in element.getElementsByTagName("VALUE"):
        try:
            deb = datetime.strptime(val.getAttribute('deb'),"%Y-%m-%d").date()
            fin   = datetime.strptime(val.getAttribute('fin'),"%Y-%m-%d").date()
            if deb <= date <= fin:
                valeur = float(val.getAttribute('valeur'))
                seuil =  datetime.strptime(val.getAttribute('valeurcontrol'),"%Y-%m-%d").date()
                if not valeur is None and not seuil is None:
                    values = values + [valeur]
                    seuils =  seuils + [seuil] 
        except Exception, e:
            code = element.getAttribute('code')
            raise Exception("Problem error when dealing with %s : \n %s" %(code,e))
        
    return values, seuils

def TranchesAttr(node, V, S):
        nb_tranches = len(S)
        S = S + ["unbound"]
        for i in range(nb_tranches):     
            seuilinf  = 'tranche'+ str(i) + '_seuilinf'
            seuilsup  = 'tranche'+ str(i) + '_seuilsup'
            setattr(node, seuilinf, S[i])
            setattr(node, seuilsup, S[i+1])
            setattr(node, 'tranche%d' %i, V[i])
            setattr(node, '_nb', nb_tranches)
        return node

class Tree2Object(object):
    def __init__(self, node, default = False):
        for child in node._children:
            setattr(self, child.code, child)
        for a, b in self.__dict__.iteritems():

            if b.typeInfo == 'CODE' or b.typeInfo == 'BAREME':
                if default:
                    setattr(self,a, b.default)
                else:
                    setattr(self,a, b.value)
                    
            elif  b.typeInfo == 'VALBYTRANCHES' :
                setattr(self, a, b)

            else:
                setattr(self,a, Tree2Object(b, default))

def from_excel_to_xml(data, code, data_date, description, xml = 'test_xml', format = "integer", ascendant_date = False, format_date = None):
    ''' Fonction qui transforme une colonne de data en lignes .xml '''
    def _format_date(date):
        day = date[0:2]
        month = date[3:5]
        year = date[6:]
        date = year + "-" + month + "-" + day
        return date
    
    if ascendant_date:
        data = data[::-1]
        data_date = data_date[::-1]
        
    with open(xml, "w") as f:
        to_write = '<CODE description= "' + description + '" code="' + code + '" format="' + format + '">\n'
        f.write(to_write)
        # Insertions des paramètres
        for i in range(len(data)):
            if i == 0: 
                fin = "2100-12-01"
            else:
                fin = str(datetime.strptime(debut,"%Y-%m-%d").date() - timedelta(days=1))
            if format_date == 'year':
                debut = str(data_date[i]) + "-01-01"
            else:
                debut = str(data_date[i])[0:10]
            if format_date == "/":
                debut = _format_date(debut)
                fin = _format_date(fin)
            to_write ='  <VALUE valeur= "' + str(data[i]) + '" deb="' + debut + '" fin="' + fin + '"/>\n'
            f.write(to_write)
        to_write = '</CODE>\n'
        f.write(to_write)
        # Corps du dofile
        f.close()
if __name__ == '__main__': 
    # Examples  
    example = False
    if example:      
        data = [0,2,3,99]
        date = ["01/04/2013", "01/04/2012", "01/04/2011", "01/04/2010"]
        code = "test"
        description = "salut"
        from_excel_to_xml(data, code, date, description, xml = 'test_xml', format_date = '/')
        
        date = [2009,2010,2011,2012]
        from_excel_to_xml(data, code, date, description, xml = 'test_xml', ascendant_date = True, format_date = 'year')
        

    
    #########
    # Séries chronologiques alimentant le noeud 'common'
    ########
    
    def _francs_to_euro(data,ix):
        data = [w.replace(',', '.') for w in data.astype(str)] 
        data = [w.replace('FRF', '')  for w in data]
        data = [w.replace(' ', '') for w in data]
        data = np.array(data, dtype = np.float)
        data[ix:] = data[ix:] / 6.5596
        return data
        # 1 -- Importation des Barmes IPP
    xlsxfile = pd.ExcelFile('Bareme_retraite.xlsx')
    # AVTS
    data = xlsxfile.parse('AVTS_montants (1962-2012)', index_col = None)
    #print data.ix[1:14, 'date'].to_string()
    dates = np.array(data.ix[1:, 'date'])
    avts = data.ix[1:, 'avts']
    avts = _francs_to_euro(avts, 13)
    plaf_avts_seul = np.array(data.ix[1:, 'plaf_mv_seul'])
    plaf_avts_seul = _francs_to_euro(plaf_avts_seul, 13)
    plaf_avts_couple = np.array(data.ix[1:, 'plaf_mv_men'])
    plaf_avts_couple = _francs_to_euro(plaf_avts_couple, 13)
    
    #from_excel_to_xml(data = avts, description = "Montant de l'allocations aux vieux travailleurs salariés", code = "montant", format = "float", data_date = dates)
    #from_excel_to_xml(data = plaf_avts_seul, description = "Plafond de ressources - personne seul", code = "plaf_seul", format = "float", data_date = dates)
    from_excel_to_xml(data = plaf_avts_couple, description = "Plafond de ressources - couple", code = "plaf_couple", format = "float", data_date = dates)
    
    
        # 2 -- Importation du Excel ParamSociaux
    xlsxfile = pd.ExcelFile('ParamSociaux.xls')
    # Paramètres généraux
    data = xlsxfile.parse('ParamGene', index_col = None, header = True)
    dates = np.array(data['Indice Prix'].index)
    indice =  np.array(data['Indice Prix'])
    #from_excel_to_xml(data = indice, description = "Indice des prix", code = "ip_reval", format = "float", data_date = dates, ascendant_date = True, format_date = 'year')
    
    plaf_ss =  np.array(data['Plafond SS'])
    #from_excel_to_xml(data = plaf_ss, description = "Plafond de la sécurité sociale", code = "plaf_ss", format = "float", data_date = dates, ascendant_date = True, format_date = 'year')