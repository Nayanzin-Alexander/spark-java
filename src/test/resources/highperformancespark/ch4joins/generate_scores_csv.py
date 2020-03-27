# noinspection
import csv
import random

g=open("scores_300mb.csv","w")
w=csv.writer(g)
w.writerow(('id','score'))
for i in range(10000000):
    w.writerow((random.randint(1,2000000),random.uniform(1,10)))
