# -*- coding:utf-8 -*-
         
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree, SubElement, Element, Comment

import time
from datetime import datetime
from xml.dom import minidom


class Tree2Object(object):
    def __init__(self, node, defaut = False):
        for child in node._children:
            setattr(self, child.code, child)
        for a, b in self.__dict__.iteritems():
            if b.typeInfo == 'CODE' or b.typeInfo == 'BAREME' or b.typeInfo == 'GENERATION':
                if defaut:
                    setattr(self,a, b.default)
                else:
                    setattr(self,a, b.value)
            else:
                setattr(self,a, Tree2Object(b, defaut))

class Node(object):
    def __init__(self, code, description = '', parent=None):        
        super(Node, self).__init__()
        self._parent = parent
        self._children = []
        self.code = code
        self.description = description
        self.valueFormat = 'none'
        self.valueType = 'none'
        self.typeInfo = 'NODE'
        self.varcontrol = 'none'
        
        if parent is not None:
            parent.addChild(self)

    def rmv_empty_code(self):
        to_remove = []
        for child in self._children:
            if not child.hasValue():
                to_remove.append(child.row())
            else:
                child.rmv_empty_code()
        for indice in reversed(to_remove):
            self.removeChild(indice)

    def insertChild(self, position, child):
        
        if position < 0 or position > len(self._children):
            return False
        
        self._children.insert(position, child)
        child._parent = self
        return True

    def removeChild(self, position):
        
        if position < 0 or position > len(self._children):
            return False
        
        child = self._children.pop(position)
        child._parent = None

        return True

    def asXml(self, fileName):
        doc = ElementTree()
        datesim = "2010 "
        root = Element(tag = self.typeInfo, 
                       attrib={'datesim': datesim})

        for i in self._children:
            print 'OK'
            i._recurseXml(root)

        doc._setroot(root)
        return doc.write(fileName, encoding = "utf-8", method = "xml")
        #return dom.ext.PrettyPrint(doc,open(name, "w"))
    def _recurseXml(self, parent):
        child = SubElement(parent, 
                               tag = self.typeInfo,
                               attrib = {'code': self.code,
                                         'description': self.description})
            
        for i in self._children:
            i._recurseXml(child)

    def load(self, other):
        for child in other._children:
            for mychild in self._children:
                if mychild.code == child.code:
                    mychild.load(child)

    def getCode(self):
        return self._code

    def setCode(self, value):
        self._code = value

    code = property(getCode, setCode)
    
    def getType(self):
        return self._typeInfo

    def setType(self, value):
        self._typeInfo = value

    typeInfo = property(getType, setType)
    
    def getDescription(self):
        return self._description

    def setDescription(self, value):
        self._description = value
        
    description = property(getDescription, setDescription)

    def getValueFormat(self):
        return self._format

    def setValueFormat(self, value):
        if not value in ('none', 'integer', 'percent'):
            return Exception("Unknowned %s valueFormat: valueFormat can be 'none', 'integer', 'percent'" % value)
        self._format = value
    
    valueFormat = property(getValueFormat, setValueFormat)

    def getValueType(self):
        return self._type

    def setValueType(self, value):
        type_list = ('none', 'monetary', 'age', 'hours', 'days', 'years')
        if not value in type_list:
            return Exception("Unknowned %s valueType: valueType can be 'none', 'monetary', 'age', 'hours', 'days', 'years'" % value)
        self._type = value
    valueType = property(getValueType, setValueType)
    
    def getValue(self):
        return self._value

    def setValue(self, value):
        self._value = value

    value = property(getValue, setValue)

    def getDefault(self):
        return self._default

    def setDefault(self, value):
        self._default = value
        
    default = property(getDefault, setDefault)

    def hasValue(self):
        out = False
        for child in self._children:
            out = out or child.hasValue()
        return out
    
    def isDirty(self):
        '''
        Check if a value has been changed in a child object
        '''
        dirty = False
        for child in self._children:
            dirty = dirty or child.isDirty()
        return dirty
            

    def addChild(self, child):
        self._children.append(child)
        child._parent = self

    def child(self, row):
        return self._children[row]
    
    def childCount(self):
        return len(self._children)

    def parent(self):
        return self._parent
    
    def row(self):
        if self._parent is not None:
            return self._parent._children.index(self)

    def data(self, column):        
        if column is 0: return self.description
    
    def setData(self, column, value):
        if column is 0: pass
    
class CodeNode(Node):
    def __init__(self, code, description, value, parent, valueFormat = 'none', valueType = 'none', varcontrol = 'none'):
        super(CodeNode, self).__init__(code, description, parent)
        self.value = value
        self.default = value
        self.typeInfo = 'CODE'
        self.valueFormat = valueFormat
        self.valueType   = valueType
        self.varcontrol = varcontrol
        
    def _recurseXml(self, parent):
        child = ET.SubElement(parent, 
                           tag = self.typeInfo,
                           attrib = {'code': self.code,
                                     'description': self.description,
                                     'varcontrol' : self.varcontrol})

        date = "2010"
        ET.SubElement(child, 
                   tag = 'VALUE', 
                   attrib = {'valeur': '%f' % self.value,
                             'deb': date,
                             'fin': date})

    def load(self, other):
        self.value = other.value

    def hasValue(self):
        if self.value is None:
            return False
        return True
    
    def isDirty(self):
        if self.value == self.default:
            return False
        return True
        
    def data(self, column):
        r = super(CodeNode, self).data(column)        
        if   column is 1: r = self.default
        if   column is 2: r = self.value
        return r

    def setData(self, column, value):
        super(CodeNode, self).setData(column, value)        
        if   column is 1: pass

class CodeNode2(Node):
    def __init__(self, code, description, value, parent, valueFormat = 'none', valueType = 'none', varcontrol ='none'):
        super(CodeNode2, self).__init__(code, description, parent)
        self.value = value
        self.default = value
        self.typeInfo = 'CODE'
        self.valueFormat = valueFormat
        self.valueType   = valueType
        self.varcontrol = varcontrol
        
    def _recurseXml(self, parent):
        if self.isDirty():
            child = SubElement(parent, 
                               tag = self.typeInfo,
                               attrib = {'code': self.code,
                                         'description': self.description,
                                         'varcontrol' : self.varcontrol})

            date = str(CONF.get('simulation', 'datesim'))
            SubElement(child, 
                       tag = 'VALUE', 
                       attrib = {'valeur': '%f' % self.value,
                                 'deb': date,
                                 'fin': date})

    def load(self, other):
        self.value = other.value

    def hasValue(self):
        if self.value is None:
            return False
        return True
    
    def isDirty(self):
        if self.value == self.default:
            return False
        return True
        
    def data(self, column):
        r = super(CodeNode, self).data(column)        
        if   column is 1: r = self.default
        if   column is 2: r = self.value
        return r

    def setData(self, column, value):
        super(CodeNode, self).setData(column, value)        
        if   column is 1: pass

class XmlReader(object):
    def __init__(self, paramFile):
        self._date = datetime.strptime("1994-01-02","%Y-%m-%d").date()
        self._doc = minidom.parse(paramFile)        
        self.tree = Node('root')
        self.handleNodeList(self._doc.childNodes, self.tree)
        self.tree = self.tree.child(0)
        self.param = Tree2Object(self.tree)
    
    def handleNodeList(self, nodeList, parent):
        for element in nodeList:
            if element.nodeType is not element.TEXT_NODE and element.nodeType is not element.COMMENT_NODE:
                if element.tagName == "BAREME":
                    code = element.getAttribute('code')
                    desc = element.getAttribute('description')
                    option = element.getAttribute('option')
                    valueType   = element.getAttribute('type')
                    tranches = Bareme(code)
                    tranches.setOption(option)
                    for tranche in element.getElementsByTagName("TRANCHE"):
                        seuil = self.handleValues(tranche.getElementsByTagName("SEUIL")[0], self._date)
                        assi = tranche.getElementsByTagName("ASSIETTE")
                        if assi:  assiette = self.handleValues(assi[0], self._date)
                        else: assiette = 1
                        taux  = self.handleValues(tranche.getElementsByTagName("TAUX")[0], self._date)
                        if not seuil is None and not taux is None:
                            tranches.addTranche(seuil, taux*assiette)
                    tranches.marToMoy()
                    node = BaremeNode(code, desc, tranches, parent, valueType)
                    
                elif element.tagName == "GENERATION":
                    valueType   = element.getAttribute('type')
                    #desc = element.getAttribute('description')
                    varcontrol = element.getAttribute('varcontrol')
                    valueFormat   = element.getAttribute('format')
                    for gen in element.getElementsByTagName("GENE"):
                        code = gen.getAttribute('code')
                        desc = gen.getAttribute('description')
                        val = self.handleValues(gen, self._date)
                        if not val is None:
                            node = CodeNode(code, desc, float(val), parent, valueFormat, valueType, varcontrol) 
                
                elif element.tagName == "CODE":
                    code = element.getAttribute('code')
                    desc = element.getAttribute('description')
                    valueFormat = element.getAttribute('format')
                    valueType   = element.getAttribute('type')
                    val = self.handleValues(element, self._date)
                    if not val is None:
                        node = CodeNode(code, desc, float(val), parent, valueFormat, valueType)
                else:
                    code = element.getAttribute('code')
                    desc = element.getAttribute('description')
                    node = Node(code, desc, parent)
                    self.handleNodeList(element.childNodes, node)

    def handleValues(self, element, date):
        # TODO gérer les assiettes en mettan l'assiette à 1 si elle n'existe pas
        for val in element.getElementsByTagName("VALUE"):
            try:
                deb = datetime.strptime(val.getAttribute('deb'),"%Y-%m-%d").date()
                fin   = datetime.strptime(val.getAttribute('fin'),"%Y-%m-%d").date()
                if deb <= date <= fin:
                    return float(val.getAttribute('valeur'))
            except Exception, e:
                code = element.getAttribute('code')
                raise Exception("Problem error when dealing with %s : \n %s" %(code,e))
        return None
    
    def handleValues1(self):
        leg = self.leg
        root = self.root
        tree = self.tree
    
        for val in root.iter("VALUE"):
            try:
                deb = datetime.strptime(val.get('deb'),"%Y-%m-%d").date()
                fin   = datetime.strptime(val.get('fin'),"%Y-%m-%d").date()
                if deb <= leg <= fin:
                    print "Un paramètre de plus", deb,leg, fin
                    return float(val.get('valeur'))
            except Exception, e:
                code = val.get('code')
                raise Exception("Problem error when dealing with %s : \n %s" %(code,e))
       
    def handleValues2(self): 
        leg = self.leg
        root = self.root
        tree = self.tree
        list_var = []
        list_val = []

        for code in root.iter('CODE'):
            code = code.get('code')
            val = self.handleValues()
            if not val is None:
                list_var = list_var + [code]
                list_val = list_val + [val]
        print len(list_val), list_var
        #return list_var, list_val
        '''
        for code in root.iter('GENE'):
            #var_control = code.get('varcontrol')
            #format = code.get('format')
            code = code.get('code')
            print code
            val = self.handleValues()
            if not val is None:
                list_var = list_var + [code]
                list_val = list_val + [val]
        '''
        print list_val, list_var
        return list_var, list_val

paramFile = 'param_2.xml'
read = XmlReader(paramFile)
param = read.param
tree = read.tree
#print param.ret_base.RG.tx_plein
# print read._doc.toxml() -> retourne le code présent dans paramFile
print tree.child(0).child(0).child(1).getValue()
read.tree.asXml('testtree.xml')

