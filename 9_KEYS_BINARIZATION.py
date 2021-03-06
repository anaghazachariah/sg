import pandas as pd
import numpy as np
data=pd.read_csv('grit_rcg_mi_e2k_prc_final1.csv',skipinitialspace=True) 


#creating columns
import ast
cols=['ADJUSTMENT_TYPE_GRIT_PRC','INTERNALPNLFAMILY_E2K','PRC_GRIT_PRC','CHAPTER_GRIT_PRC','GAAP_TYPE_GRIT_PRC','CCY_CD_RCG','FIN_CLASS_RCG','AFFIL_CD_RCG','GLACCT_NBR_RCG','ACCOUNTTYPE_E2K','CHARTOFACCOUNTTYPE_E2K','ACCOUNTINGNORM_E2K','ACCOUNTMONETARYTYPE_E2K','ACCOUNTBALANCE_E2K','INTERNALEXTERNAL_E2K','IASECONOMICPURPOSE_E2K','VALUATIONMETHOD_E2K','TRANSACTIONALINDICATOR_E2K','INTERCOFOLLOWUPTYPE_E2K','BASELPERIMETER_E2K','BASELAMOUNTTYPE_E2K','GLOBALBASELRECONCILIATION_E2K','PRCACCOUNTENGLISHNAME_IAS_PRC','ACCOUNTCLASS_IAS_PRC','ACCOUNTMONETARYTYPE_IAS_PRC','ACCOUNTBALANCE_IAS_PRC','PRCECONOMICPURPOSE_IAS_PRC','INTERCOFOLLOWUPTYPE_IAS_PRC','BASELPERIMETER_IAS_PRC','PRCACCOUNTENGLISHNAME_GAAP_PRC','ACCOUNTCLASS_GAAP_PRC','ACCOUNTMONETARYTYPE_GAAP_PRC','ACCOUNTBALANCE_GAAP_PRC','PRCECONOMICPURPOSE_GAAP_PRC','INTERCOFOLLOWUPTYPE_GAAP_PRC','ENTITY_GRIT_E2k','COUNTERPART_GRIT_E2k','E2K_PCI_AGG_AMOUNT_GRIT_E2k','CHPFR_GRIT_E2k','CHPIAS_GRIT_E2k','CLASS_FR_GRIT_E2k','CLASS_IAS_GRIT_E2k']
data_cols=[]
for i in cols:
  values=data[i].tolist()
  unique_values=list(set(values))
  values_with_suffix = [sub + '_'+str(i) for sub in unique_values]
  data_cols.extend(values_with_suffix)
extra_cols=['WITHIN_ENTITY_INTERCO_TYPE_GRIT_E2k','SAME_REGION_INTERCO_TYPE_GRIT_E2k','INTRA_REGION_INTERCO_TYPE_GRIT_E2k','NON_SGCIB_ENTITY_INTERCO_TYPE_GRIT_E2k','ODS/CNR_ADJUSTMENT_TYPE_MI','QTZ_SOURCE_APPLICATION_ID_MI','PEC/QTZ_SOURCE_APPLICATION_ID_MI','PEC_SOURCE_APPLICATION_ID_MI','INVENTORYIS-_ORIG_CCY_MEAS_AMT_RCG','INVENTORY=0_ORIG_CCY_MEAS_AMT_RCG','INVENTORYIS+_ORIG_CCY_MEAS_AMT_RCG','GLIS-_GLACCT_BAL_AMT_RCG','GL=0_GLACCT_BAL_AMT_RCG','GLIS+_GLACCT_BAL_AMT_RCG','BREAKIS-_ORIG_CCY_DIFF_AMT_RCG','BREAK=0_ORIG_CCY_DIFF_AMT_RCG','BREAKIS+_ORIG_CCY_DIFF_AMT_RCG','FIRSTBIT=1_APP_ORGIN_RCG','SECONDBIT=1_APP_ORGIN_RCG','THIRDBIT=1_APP_ORGIN_RCG']
data_cols.extend(extra_cols)
mi_cols=['PRC_IAS_MI','CONSO1_MI','EVENT_NATURE_MI','OPERATION_CODE_MI','OPERATION_DIRECTION_MI','TRANSACTION_TYPE_MI']
mi_vals=[]
for i in mi_cols:
  values=[]
  x=data[i].tolist()
  for j in x:
    if j=='missing' or j=="['missing']":
      continue
    else:
      m = ast.literal_eval(j)
      values.extend(m)
  
  values_with_suffix = [sub + '_'+str(i) for sub in values]
  unique_values=list(set(values_with_suffix))
  mi_vals.extend(unique_values)
data_cols.extend(mi_vals)
data_cols =[x for x in data_cols if not x.startswith('missing')]
bin_data = pd.DataFrame(0, index=np.arange(len(data)), columns=data_cols) 

#entering values


for j in cols:
  for i in range(0,len(data)):
    item=data.loc[ i,j]
    if item.startswith('missing'):
      continue
    else:
      item=data.loc[ i,j]+'_'+str(j)
      bin_data.loc[ i,item]=1


rcg_col=['ORIG_CCY_MEAS_AMT_RCG','GLACCT_BAL_AMT_RCG','ORIG_CCY_DIFF_AMT_RCG']
for ii in range(0,len(rcg_col)):
  col=data[rcg_col[ii]].tolist()
  for i in range(0,len(col)):         
    if col[i]=='missing':
      continue
    else:
      if ii==0:
        if float(col[i])<0:
          bin_data.loc[ i,'INVENTORYIS-_ORIG_CCY_MEAS_AMT_RCG']=1    
                                     
        elif float(col[i])==0:
          bin_data.loc[ i,'INVENTORY=0_ORIG_CCY_MEAS_AMT_RCG']=1
        else:                       
          bin_data.loc[ i,'INVENTORYIS+_ORIG_CCY_MEAS_AMT_RCG']=1
                      
      elif ii==2:
        if float(col[i])<0:
          bin_data.loc[ i,'BREAKIS-_ORIG_CCY_DIFF_AMT_RCG']=1
                    
        elif float(col[i])==0:
          bin_data.loc[ i,'BREAK=0_ORIG_CCY_DIFF_AMT_RCG']=1
        else:
                        
          bin_data.loc[ i,'BREAKIS+_ORIG_CCY_DIFF_AMT_RCG']=1
      elif ii==1:
        if float(col[i])<0:
          bin_data.loc[ i,'GLIS-_GLACCT_BAL_AMT_RCG']=1
                       
                    
        elif float(col[i])==0:
          bin_data.loc[ i,'GL=0_GLACCT_BAL_AMT_RCG']=1
        else:                        
          bin_data.loc[ i,'GLIS+_GLACCT_BAL_AMT_RCG']=1
app_orgin_list=data['APP_ORIGIN_RCG'].tolist()
for i in range(0,len(app_orgin_list)):
  if app_orgin_list[i]=='missing':
    continue
  else:   
    num=str(app_orgin_list[i]).zfill(3)
    if num[0]=="1":
      bin_data.loc[ i,'FIRSTBIT=1_APP_ORGIN_RCG']=1
    if num[1]=="1":
      bin_data.loc[ i,'SECONDBIT=1_APP_ORGIN_RCG']=1       
    if num[2]=="1":
      bin_data.loc[ i,'THIRDBIT=1_APP_ORGIN_RCG']=1

interco=data['INTERCO_TYPE_GRIT_E2k'].tolist()
for i in range(0,len(interco)):
  if interco[i]=='missing':
    continue
  else:   
    num=str(interco[i])
    if num=="0.0":
      bin_data.loc[ i,'WITHIN_ENTITY_INTERCO_TYPE_GRIT_E2k']=1
    elif num=="1.0":
      bin_data.loc[ i,'SAME_REGION_INTERCO_TYPE_GRIT_E2k']=1       
    elif num=="2.0":
      bin_data.loc[ i,'INTRA_REGION_INTERCO_TYPE_GRIT_E2k']=1
    elif num=="3.0":
      bin_data.loc[ i,'NON_SGCIB_ENTITY_INTERCO_TYPE_GRIT_E2k']=1
adjustment_list=data['ADJUSTMENT_TYPE_MI'].tolist()
for i in range(0,len(adjustment_list)):
  if adjustment_list[i]=='missing' or adjustment_list[i]=="['missing']":
    continue
  else:   
    org_list= ast.literal_eval(adjustment_list[i])
    if ('ODS' in org_list) or ('CNR' in org_list):
      bin_data.loc[i,'ODS/CNR_ADJUSTMENT_TYPE_MI']=1

adjustment_list=data['SOURCE_APPLICATION_ID_MI'].tolist()
for i in range(0,len(adjustment_list)):
  if adjustment_list[i]=='missing' or adjustment_list[i]=="['missing']":
    continue
  else:   
    org_list= ast.literal_eval(adjustment_list[i])
    if ('QTZ' in org_list )and ('PEC' in org_list):
      bin_data.loc[i,'PEC/QTZ_SOURCE_APPLICATION_ID_MI']=1
    elif 'QTZ' in org_list:
      bin_data.loc[i,'QTZ_SOURCE_APPLICATION_ID_MI']=1
    elif 'PEC' in org_list:
      bin_data.loc[i,'PEC_SOURCE_APPLICATION_ID_MI']=1

adjustment_list=data['PRC_IAS_MI'].tolist()
for i in range(0,len(adjustment_list)):
  if adjustment_list[i]=='missing' or adjustment_list[i]=="['missing']":
    continue
  else:   
    org_list= ast.literal_eval(adjustment_list[i])
    item_sets=str(org_list[0])+'_'+'PRC_IAS_MI'
    bin_data.loc[i,item_sets]=1

adjustment_list=data['CONSO1_MI'].tolist()
for i in range(0,len(adjustment_list)):
  if adjustment_list[i]=='missing' or adjustment_list[i]=="['missing']":
    continue
  else:   
    org_list= ast.literal_eval(adjustment_list[i])
    for j in org_list:
      if str(j)=='missing':
        continue
      else:
        item_s=str(j)+'_'+'CONSO1_MI'
        bin_data.loc[i,item_s]=1
adjustment_list=data['EVENT_NATURE_MI'].tolist()
for i in range(0,len(adjustment_list)):
  if adjustment_list[i]=='missing' or adjustment_list[i]=="['missing']":
    continue
  else:   
    org_list= ast.literal_eval(adjustment_list[i])
    for j in org_list:
      if str(j)=='missing':
        continue
      else:
        item_s=str(j)+'_'+'EVENT_NATURE_MI'
        bin_data.loc[i,item_s]=1
adjustment_list=data['OPERATION_CODE_MI'].tolist()

for i in range(0,len(adjustment_list)):
  if adjustment_list[i]=='missing' or adjustment_list[i]=="['missing']":
    continue
  else:   
    org_list= ast.literal_eval(adjustment_list[i])
    for j in org_list:
      if str(j)=='missing':
        continue
      else:
        item_s=str(j)+'_'+'OPERATION_CODE_MI'
        bin_data.loc[i,item_s]=1
adjustment_list=data['TRANSACTION_TYPE_MI'].tolist()
for i in range(0,len(adjustment_list)):
  if adjustment_list[i]=='missing' or adjustment_list[i]=="['missing']":
    continue
  else:   
    org_list= ast.literal_eval(adjustment_list[i])
    item_sets=str(org_list[0])+'_'+'TRANSACTION_TYPE_MI'
    bin_data.loc[i,item_sets]=1
adjustment_list=data['OPERATION_DIRECTION_MI'].tolist()
adjustment_list=data['OPERATION_DIRECTION_MI'].tolist()
for i in range(0,len(adjustment_list)):
  if adjustment_list[i]=='missing' or adjustment_list[i]=="['missing']":
    continue
  else:   
    org_list= ast.literal_eval(adjustment_list[i])
    for j in org_list:
      if str(j)=='missing':
        continue
      else:
        item_s=str(j)+'_'+'OPERATION_DIRECTION_MI'
        bin_data.loc[i,item_s]=1

bin_data.to_csv('binarized_file_new_9ucc_GRIT_final_new_final1.csv')
