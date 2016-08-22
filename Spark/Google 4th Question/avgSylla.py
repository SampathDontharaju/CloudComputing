# Spark example to print the average average sylaables per word using Spark
# To run, do: spark-submit --master yarn-client avgSylla.py hdfs://hadoop2-0-0/data/1gram/*

from __future__ import print_function
import sys, json, re
from pyspark import SparkContext

def syllable(line):
  year=line.strip().lower().split()[1]
  word=line.strip().lower().split()[0]
  count = 0
  vowels = 'aeiouy'
  word = word.lower().strip(".:;?!")
  if word[0] in vowels:
      count +=1
  for index in range(1,len(word)):
      if word[index] in vowels and word[index-1] not in vowels:
          count +=1
  if word.endswith('e'):
      count -= 1
  if count == 0:
      count +=1
  return (year,(count,1))
  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="avgSylla")
  
  file1 = sc.textFile(sys.argv[1],)
  
  mapper_1 = file1.map(syllable)
  reducer_1= mapper_1.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))
  sort=reducer_1.sortByKey()
  mapper_2=reducer_1.map(lambda line:line[0]+' '+str(float(line[1][0])/line[1][1]))
  print(mapper_2.take(10))
 
  # Save to your local HDFS folder 
  mapper_2.saveAsTextFile("avgSylla")
  sc.stop()
