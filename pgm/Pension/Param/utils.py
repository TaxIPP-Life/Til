# -*- coding:utf-8 -*-

from xml.dom import minidom
from datetime import datetime

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
