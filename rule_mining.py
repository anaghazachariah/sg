###################################################  APRIORI ALGORITHM    ################################################################
from efficient_apriori import apriori
import pandas as pd
dataa=pd.read_csv('binary.csv',skipinitialspace=True) 
x=list(dataa.columns)
data=dataa.drop([x[0]], axis = 1)
data.fillna('missing', inplace=True)
data.columns = data.columns.str.replace(' ', '')
#getting the transactions from the data
transactions=[]
hds=list(data.columns) 

for i in range(0,len(data)):
    col_list=[]
    for j in data.columns:
        val=data.at[i,j]
        if val==1:
            col_list.append(j)
    transactions.append(col_list)
print('h')
itemsets, rules =apriori(transactions, min_support=0.01, min_confidence=1)
file1 = open("myfile_28.txt","w") 
for i in rules:
    newi=str(i).replace(" ", "")
    if (newi.count('SECONDBIT=1_APP_ORGIN_RCG')>0 and newi.count('FIRSTBIT=1_APP_ORGIN_RCG')>0 and newi.count('INVENTORY_UPDATION_QTZ/PEC_MI')>0 and newi.count('INVENTORY_ADJ_ADJUSTMENT_TYPE_MI')>0) :
        file1.write(str(i))
        file1.write("\n")
