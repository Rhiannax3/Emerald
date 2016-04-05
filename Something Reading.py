import csv as csv
import numpy as np

csv_file_ob = csv.reader(open('C:\Users\Administrator\Documents\Python_and_R\train.csv', 'rb'))
header = csv_file_ob.next()

data =[]
for row in csv_file_ob:
	data.append(row)
data = np.array(data)
