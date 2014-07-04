import numpy as np
import tables
import pdb


__version__ = "0.3"

table_convertion = {'famille':'menage'}

def get_h5_fields(input_file):
    return dict((table._v_name, get_fields(table))
                for table in input_file.iterNodes(input_file.root.entities))


def main(simulation, input1_path, input2_path, period):
    pdb.set_trace()
    input1_file = tables.openFile(input1_path, mode="a")
    input2_file = tables.openFile(input2_path, mode="r")

    print "merging legislation from ", input2_path, "to simulation file"

    input1_entities = input1_file.root.entities
    input2_entities = input2_file.root.entities

    fields1 = get_h5_fields(input1_file)
    fields2 = get_h5_fields(input2_file)
    
    set_ent_names1 = set(fields1.keys())
    set_ent_names2 = set(fields2.keys())
    
    table_reg = getattr(input1_entities, 'register')
    input_reg_rows = index_table_light(table_reg)
    
    for ent_name in sorted(set_ent_names2): 
        
        ent_name1 = ent_name
        ent_name2 = ent_name
        if ent_name in table_convertion.keys():
            ent_name1 = table_convertion[ent_name]
            
        ent_fields1 = fields1.get(ent_name1, [])
        ent_fields2 = fields2.get(ent_name2, [])
        output_fields = merge_items(ent_fields1, ent_fields2)
        if ent_name1 in set_ent_names1:
            table1 = getattr(input1_entities, ent_name1)
#            print " * indexing table from %s ..." % input1_path,
            input1_rows = index_table_light(table1)
#            print "done."
        else:
            table1 = None
            input1_rows = {}

        if ent_name in set_ent_names2: #ce if est inutile maintenant
            table2 = getattr(input2_entities, ent_name2)
#            print " * indexing table from %s ..." % input2_path,
            input2_rows = index_table_light(table2)
#            print "done."
        else:
            table2 = None
            input2_rows = {}
             
#        print " * merging: ",
        input1_periods = input1_rows.keys()
        input2_periods = input2_rows.keys()
        output_periods = sorted(set(input1_periods) | set(input2_periods))
        
        if ent_name1 in set_ent_names1:
            #if period = None, take whole base
            start1, stop1 = input1_rows.get(period, (0, table1.nrows))
            input1_array = table1.read(start1, stop1)
        else:
            input1_array = None

        if ent_name2 in set_ent_names2:
            #if period = None, take whole base
            start2, stop2 = input2_rows.get(period, (0, table2.nrows))
            input2_array = table2.read(start2, stop2)
        else:
            input2_array = None

        if ent_name1 in set_ent_names1 and ent_name2 in set_ent_names2:
            output_array, _ = mergeArrays(input1_array, input2_array)
        elif ent_name1 in set_ent_names1:
            output_array = input1_array
        elif ent_name2 in set_ent_names2:
            output_array = input2_array
        else:
            raise Exception("this shouldn't have happened")
        
        table1.append(output_array)             
        table1.removeRows(start1, stop1)
        table1.flush()
        
               
            
    print " done."



    input1_file.close()
    input2_file.close()



if __name__ == '__main__':
    import sys
    import platform

    print "LIAM HDF5 merge %s using Python %s (%s)\n" % \
          (__version__, platform.python_version(), platform.architecture()[0])


    tables.copyFile("C:/Myliam2/Model/SimulTest.h5", "C:/Myliam2/Model/SimulTestTemp.h5", overwrite=True)
    merge_h5("C:/Myliam2/Model/SimulTest.h5", output+"LiamLeg.h5",None)