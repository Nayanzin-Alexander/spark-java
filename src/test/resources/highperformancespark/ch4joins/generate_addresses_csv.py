# noinspection
import csv

g=open("addresses_70mb.csv","w")
w=csv.writer(g)
w.writerow(('id','address','name'))
for i in range(2000000):
    w.writerow((i+1,'{}{}'.format('address_',i),'{}{}'.format('name_',i)))