# noinspection
import csv
import random

g = open("actors.csv", "w")
w = csv.writer(g)
w.writerow(('id', 'name', 'year'))
for i in range(10000000):
    w.writerow((random.randint(1, 2000000), '{}{}'.format('address_', i), random.randint(2010, 2020)))
