from pyspark import SparkContext
import sys

def extractDesc(partId, records):
    if partId==0:
        next(records)
    import csv
    reader = csv.reader(records)
    for row in reader:
        (camis,desc) = (int(row[0]), row[7])
        yield (camis, desc)
        
if __name__=='__main__':
    sc = SparkContext()
 
    inspections = sc.textFile(sys.argv[1], use_unicode=False).cache()
    
    desc = inspections.mapPartitionsWithIndex(extractDesc)
    
    camis = desc.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
    
    desc_grouped = camis.join(desc)\
                       .reduceByKey(lambda x,y:y)\
                       .values()\
                       .map(lambda x: (x[1], 1))\
                       .reduceByKey(lambda x,y: x+y)\
                       .saveAsTextFile(sys.argv[-1])