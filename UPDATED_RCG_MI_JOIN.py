import pandas as pd 
import numpy as np

#reading RCG
rcg_data = pd.read_csv("RCG_G2279_20210121.csv",skipinitialspace=True)
rcg_data.fillna('missing', inplace=True)
del_cols=['CHAR_FIELD1','CHAR_FIELD2','CHAR_FIELD3','CHAR_FIELD4','CHAR_FIELD5','AMOUNT_FIELD1','AMOUNT_FIELD2','AMOUNT_FIELD3','AMOUNT_FIELD4','AMOUNT_FIELD5','DATE_FIELD1','DATE_FIELD2','DATE_FIELD3','E2KACCT_DESC','LCL_CCY_DIFF_AMT']
for i in del_cols:
  del rcg_data[i]
rcg_data = rcg_data.add_suffix('_RCG')
rcg_data.insert(0, 'ID', range(0, 0 + len(rcg_data)))
rcg_cols=list(rcg_data.columns) 

#reading MI
mi_data = pd.read_csv("MI_G2279.csv",skipinitialspace=True)
mi_data.fillna('missing', inplace=True)
del_cols=['DLR_PUBLISH_DATE','DEAL_ID','TRANSACTION_ID','INVENTORY_DATE','TRANSACTION_TYPE','PORTFOLIO_ID','PARTY_ID','PRODUCT_GUID','ADJUSTMENT_DATE','VALUE_DATE','AFF_CODFIL','CONSO1_SUFF','OBJECT','MATURITY_DATE','CLOSING_DATE']
for i in del_cols:
  del mi_data[i]
mi_data = mi_data.add_suffix('_MI')

#joining RCG and MI
rcg_mi_join = pd.merge(rcg_data, mi_data,  how='inner', left_on=['E2KACCT_NBR_RCG','BUS_UNIT_RCG','DEPT_ID_RCG','CCY_CD_RCG','ALTACCT_RCG','AFFIL_CD_RCG','GL_PROD_CD_RCG','OPERATING_UNIT_RCG','E2K_PRODUCT_CD_RCG'], right_on = ['ACCOUNT_MI','BUSINESS_UNIT_MI','DEPTID_MI','CURRENCY_MI','ALTACCT_MI','AFFILIATE_MI','OBJECT_CODE_MI','OPERATING_UNIT_MI','PRODUCT_MI'])
del_cols=['ACCOUNT_MI','BUSINESS_UNIT_MI','DEPTID_MI','CURRENCY_MI','ALTACCT_MI','AFFILIATE_MI','OBJECT_CODE_MI','OPERATING_UNIT_MI','PRODUCT_MI']
for i in del_cols:
  del rcg_mi_join[i]
  
#grouping rows of MI corresponds to single RCG row
mi=rcg_mi_join.groupby('ID').agg({'ID': lambda x: x.unique(),'TRAN_AMOUNT_MI': lambda x: x.unique().tolist(),'EVENT_NATURE_MI': lambda x: x.unique().tolist(),'ADJUSTMENT_TYPE_MI': lambda x: x.unique().tolist(),'SOURCE_APPLICATION_ID_MI': lambda x: x.unique().tolist(),'INVENTORY_TYPE_MI': lambda x: x.unique().tolist(),'OPERATION_CODE_MI': lambda x: x.unique().tolist(),'OPERATION_DIRECTION_MI': lambda x: x.unique().tolist(),'CONSO1_MI': lambda x: x.unique().tolist(),'PRC_IAS_MI': lambda x: x.unique().tolist()})
mi_new=rcg_mi_join.groupby('ID').agg({'TRAN_AMOUNT_MI': lambda x: x.unique().tolist(),'EVENT_NATURE_MI': lambda x: x.unique().tolist(),'ADJUSTMENT_TYPE_MI': lambda x: x.unique().tolist(),'SOURCE_APPLICATION_ID_MI': lambda x: x.unique().tolist(),'INVENTORY_TYPE_MI': lambda x: x.unique().tolist(),'OPERATION_CODE_MI': lambda x: x.unique().tolist(),'OPERATION_DIRECTION_MI': lambda x: x.unique().tolist(),'CONSO1_MI': lambda x: x.unique().tolist(),'PRC_IAS_MI': lambda x: x.unique().tolist()})


#TO REMOVE WRONG GLAAM FROM RCG
rows_present_in_rcg_mi=mi['ID'].tolist() #RECORD ID'S OF ROWS WHICH HAVE A MATCH IN MI


inventory_0_rows=[]#RECORDS WITH INVENTORY IS 0
for i in range (0,len(rcg_data)):
  if rcg_data.loc[ i,'ORIG_CCY_MEAS_AMT_RCG']==0  :
    inventory_0_rows.append(rcg_data.loc[ i,'ID'])



rows_not_present_in_rcg_mi=[]#RECORD ID'S OF ROWS WHICH DONT HAVE A MATCH IN MI
for i in range(0,2105):
  if i not in rows_present_in_rcg_mi:
    rows_not_present_in_rcg_mi.append(rcg_data.loc[ i,'ID'])


wrong_glaam=[]#finding wrong glaam records
for i in rows_not_present_in_rcg_mi:
  if i not in inventory_0_rows:
    wrong_glaam.append(i)

for i in wrong_glaam:#deleting records from rcg
  delete_row = rcg_data[rcg_data["ID"]==i].index
  rcg_data = rcg_data.drop(delete_row)
 
rcg_mi_merged=pd.merge(rcg_data, mi_new,  how='left', left_on=['ID'], right_on = ['ID'])#rcg mi merged file
