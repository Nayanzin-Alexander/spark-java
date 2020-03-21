# noinspection
import csv
import random

g=open("scores_300mb.csv","w")
w=csv.writer(g)
w.writerow(('panda_id','score'))
for i in range(10000000):
    w.writerow((i+1,random.uniform(1,10)))
