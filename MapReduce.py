import numpy as np
import multiprocessing as mp

#Mapper for Solution1: returns age and year
def mapper1(x):
    k=  x[0]
    v = x[1]  
    return k,v

#Mapper for Solution2:returns age and weight
def mapper2(x):
    v = x[1]
    r = x[2]
    return v,r

#Mapper for Solution3: return age and 1 for each record
def mapper3(x):
    v = x[1]
    r = 1
    return v,r

#common partitioner for all the mappers
#input the data returned from mapper function and returns key value dictionary
def partitioner(m_data):
    p_data = {}   
    for k, v in m_data:
        if k in p_data:
            p_data[k].append(v)
        else:
            p_data.update({k:[v]})
    return p_data.items()
        
#reducer for solution 1: returns the average value for each key i.e. year    
def reducer1(p_data):
    k, v = p_data
    return k, round(np.mean(v),2)

#reducer for solution 2: returns the maximum value for each key i.e. age    
def reducer2(p_data):
    k, v = p_data
    return k, np.max(v)

#reducer for solution 1: returns the count of each key i.e.age
def reducer3(p_data):
    k, v = p_data
    return k, sum(v)

#function to read the file and return the whole dataset
def reading_data(filename):   
    data = []
    try:
        f = open(filename, 'r')       
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
    except ValueError:
        print "Could not convert data to an integer."
    except:
         print "Unexpected error:", sys.exc_info()[0]    
    for line in f:
        year, age, weight = line.split()        
        data.append((int(year), int(age), int(weight)))
    return data 

if __name__ == "__main__":
    filename = 'demo.dat'
    data = reading_data(filename)    
    pool = mp.Pool(3)
    m_data = pool.map(mapper1,data)
    p_data = partitioner(m_data)
    r_data = pool.map(reducer1,p_data)
    
    print("Solution1: Average Age Per Year")
    print ("--------------------------------")
    for record in r_data:
        print "Year:%d Average Age:%2.2f"% (record[0], record[1])
    
    m_data = pool.map(mapper2,data)
    p_data = partitioner(m_data)
    r_data = pool.map(reducer2,p_data)
    
    print("Solution2: Maximum weight per age")
    print ("--------------------------------")
    for record in r_data:
        print "Age:%d Maximum Weight:%d"% (record[0], record[1])
    
    m_data = pool.map(mapper3,data)
    p_data = partitioner(m_data)
    r_data = pool.map(reducer3,p_data)
    
    print("Solution3: Number of People in each age group")
    print ("--------------------------------")
    for record in r_data:
        print "Age:%d Count:%d"% (record[0], record[1])
    
    



