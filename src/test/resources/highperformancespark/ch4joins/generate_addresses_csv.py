# noinspection
import csv

g=open("addresses_70mb.csv","w")
w=csv.writer(g)
w.writerow(('panda_id','panda_address','panda_name'))
for i in range(2000000):
    w.writerow((i+1,'{}{}'.format('address_',i),'{}{}'.format('name_',i)))