#READING_RCG_DATA
import pandas as pd 
import numpy as np
rcg_data = pd.read_csv("RCG_G2279_20210121.csv",skipinitialspace=True)

rcg_data.fillna('missing', inplace=True)
del_cols=['CHAR_FIELD1','CHAR_FIELD2','CHAR_FIELD3','CHAR_FIELD4','CHAR_FIELD5','AMOUNT_FIELD1','AMOUNT_FIELD2','AMOUNT_FIELD3','AMOUNT_FIELD4','AMOUNT_FIELD5','DATE_FIELD1','DATE_FIELD2','DATE_FIELD3','E2KACCT_DESC','LCL_CCY_DIFF_AMT']
for i in del_cols:
  del rcg_data[i]
rcg_data = rcg_data.add_suffix('_RCG')
rcg_data.insert(0, 'ID', range(0, 0 + len(rcg_data)))

rcg_cols=list(rcg_data.columns) 

#READING_MI
mi_data = pd.read_csv("MI_G2279.csv",skipinitialspace=True)
mi_data.fillna('missing', inplace=True)
del_cols=['DLR_PUBLISH_DATE','DEAL_ID','TRANSACTION_ID','INVENTORY_DATE','PORTFOLIO_ID','PARTY_ID','PRODUCT_GUID','ADJUSTMENT_DATE','VALUE_DATE','AFF_CODFIL','CONSO1_SUFF','OBJECT','MATURITY_DATE','CLOSING_DATE']
for i in del_cols:
  del mi_data[i]
mi_data = mi_data.add_suffix('_MI')

#MI AND RCG JOIN
rcg_mi_join = pd.merge(rcg_data, mi_data,  how='inner', left_on=['E2KACCT_NBR_RCG','BUS_UNIT_RCG','DEPT_ID_RCG','CCY_CD_RCG','ALTACCT_RCG','AFFIL_CD_RCG','GL_PROD_CD_RCG','OPERATING_UNIT_RCG','E2K_PRODUCT_CD_RCG'], right_on = ['ACCOUNT_MI','BUSINESS_UNIT_MI','DEPTID_MI','CURRENCY_MI','ALTACCT_MI','AFFILIATE_MI','OBJECT_CODE_MI','OPERATING_UNIT_MI','PRODUCT_MI'])
del_cols=['ACCOUNT_MI','BUSINESS_UNIT_MI','DEPTID_MI','CURRENCY_MI','ALTACCT_MI','AFFILIATE_MI','OBJECT_CODE_MI','OPERATING_UNIT_MI','PRODUCT_MI']
for i in del_cols:
  del rcg_mi_join[i]

#GROUPING MI
mi=rcg_mi_join.groupby('ID').agg({'ID': lambda x: x.unique(),'TRANSACTION_TYPE_MI': lambda x: x.unique().tolist(),'TRAN_AMOUNT_MI': lambda x: x.unique().tolist(),'EVENT_NATURE_MI': lambda x: x.unique().tolist(),'ADJUSTMENT_TYPE_MI': lambda x: x.unique().tolist(),'SOURCE_APPLICATION_ID_MI': lambda x: x.unique().tolist(),'INVENTORY_TYPE_MI': lambda x: x.unique().tolist(),'OPERATION_CODE_MI': lambda x: x.unique().tolist(),'OPERATION_DIRECTION_MI': lambda x: x.unique().tolist(),'CONSO1_MI': lambda x: x.unique().tolist(),'PRC_IAS_MI': lambda x: x.unique().tolist()})
mi_new=rcg_mi_join.groupby('ID').agg({'TRAN_AMOUNT_MI': lambda x: x.unique().tolist(),'TRANSACTION_TYPE_MI': lambda x: x.unique().tolist(),'EVENT_NATURE_MI': lambda x: x.unique().tolist(),'ADJUSTMENT_TYPE_MI': lambda x: x.unique().tolist(),'SOURCE_APPLICATION_ID_MI': lambda x: x.unique().tolist(),'INVENTORY_TYPE_MI': lambda x: x.unique().tolist(),'OPERATION_CODE_MI': lambda x: x.unique().tolist(),'OPERATION_DIRECTION_MI': lambda x: x.unique().tolist(),'CONSO1_MI': lambda x: x.unique().tolist(),'PRC_IAS_MI': lambda x: x.unique().tolist()})

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

#RCG AND MI JOIN
rcg_mi_merged=pd.merge(rcg_data, mi_new,  how='left', left_on=['ID'], right_on = ['ID'])
#READING E2K
e2k_data = pd.read_csv("E2K_REF.csv",skipinitialspace=True)
e2k_data.fillna('missing', inplace=True)
e2k_data['E2KACCOUNTID'] = e2k_data.E2KACCOUNTID.astype(str)
del_cols=['VERSIONEFFECTIVEDATE','E2KACCOUNTNAME','E2KACCOUNTLABEL','ACCOUNTAGGREGATEID','ACCOUNTAGGREGATEMNEMONIC','AGGREGATEAXISTYPE','REVERSALINDICATOR','FRENCHGAAPECONOMICPURPOSE','ACCOUNTINGFLOWNATURE','MULTIPRODUCTINDICATOR','PCINFOFIINDICATOR','INFOFI60INDICATOR','INTERESTRATENATURE','BASELDEALUPDATEPROCESSSCHEME','ACCOUNTFOLLOWUPMETHOD','MATCHINGINDICATOR','RETENTIONDAYCOUNT','INTACCOUNTAGGREGATEID','INTACCOUNTAGGREGATEMNEMONIC','EXTACCOUNTAGGREGATEID','EXTACCOUNTAGGREGATEMNEMONIC']
for i in del_cols:
  del e2k_data[i]
e2k_data = e2k_data.add_suffix('_E2K')
acnt=e2k_data['E2KACCOUNTID_E2K'].tolist()
acnt=list(map(str, acnt))
for i in range(0,len(e2k_data )):
  if ( e2k_data.loc[ i,'GAAPASSETPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ i,'GAAPLIABILITYPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ i,'IASLIABILITYPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ i,'IASASSETPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ i,'PARENTE2KACCOUNTID_E2K']!='missing'):
    parent=e2k_data.loc[ i,'PARENTE2KACCOUNTID_E2K']
    if str(parent) in acnt:
      we=e2k_data[e2k_data['E2KACCOUNTID_E2K']==parent].index.values
      new_acnt_indx=we[0]

      if e2k_data.loc[ new_acnt_indx,'GAAPASSETPRCACCOUNTID_E2K']!='missing' and e2k_data.loc[ new_acnt_indx,'GAAPLIABILITYPRCACCOUNTID_E2K']!='missing' and  e2k_data.loc[ new_acnt_indx,'IASLIABILITYPRCACCOUNTID_E2K']!='missing' and e2k_data.loc[ new_acnt_indx,'IASASSETPRCACCOUNTID_E2K']!='missing' :
        e2k_data.loc[ i,'IAS_E2K']='hybrid' 
        e2k_data.loc[ i,'GAAP_E2K']='hybrid'
      else:
        if e2k_data.loc[ new_acnt_indx,'IASASSETPRCACCOUNTID_E2K']!='missing' and e2k_data.loc[ new_acnt_indx,'IASLIABILITYPRCACCOUNTID_E2K']=='missing' :
          e2k_data.loc[ i,'IAS_E2K']=int(e2k_data.loc[ new_acnt_indx,'IASASSETPRCACCOUNTID_E2K'])
        elif e2k_data.loc[ new_acnt_indx,'IASLIABILITYPRCACCOUNTID_E2K']!='missing' and e2k_data.loc[ new_acnt_indx,'IASASSETPRCACCOUNTID_E2K']=='missing':
          e2k_data.loc[ i,'IAS_E2K']=int(e2k_data.loc[ new_acnt_indx,'IASLIABILITYPRCACCOUNTID_E2K'])
        elif e2k_data.loc[ new_acnt_indx,'IASLIABILITYPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ new_acnt_indx,'IASASSETPRCACCOUNTID_E2K']=='missing':
          e2k_data.loc[ i,'IAS_E2K']='missing'
        else:
          e2k_data.loc[ i,'IAS_E2K']='missing'

          
      
        if e2k_data.loc[ new_acnt_indx,'GAAPLIABILITYPRCACCOUNTID_E2K']!='missing' and e2k_data.loc[ new_acnt_indx,'GAAPASSETPRCACCOUNTID_E2K']=='missing':
          e2k_data.loc[ i,'GAAP_E2K']=int(e2k_data.loc[ new_acnt_indx,'GAAPLIABILITYPRCACCOUNTID_E2K'])
        elif e2k_data.loc[ new_acnt_indx,'GAAPASSETPRCACCOUNTID_E2K']!='missing' and e2k_data.loc[ new_acnt_indx,'GAAPLIABILITYPRCACCOUNTID_E2K']=='missing':
          e2k_data.loc[ i,'GAAP_E2K']=int(e2k_data.loc[ new_acnt_indx,'GAAPASSETPRCACCOUNTID_E2K'])
        elif e2k_data.loc[ new_acnt_indx,'GAAPASSETPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ new_acnt_indx,'GAAPLIABILITYPRCACCOUNTID_E2K']=='missing' :
          e2k_data.loc[ i,'GAAP_E2K']='missing'
        else:
          e2k_data.loc[ i,'GAAP_E2K']='missing'

  else:
    if e2k_data.loc[ i,'GAAPLIABILITYPRCACCOUNTID_E2K']!='missing' and e2k_data.loc[ i,'GAAPASSETPRCACCOUNTID_E2K']!='missing' and  e2k_data.loc[ i,'IASLIABILITYPRCACCOUNTID_E2K']!='missing' and e2k_data.loc[ i,'IASASSETPRCACCOUNTID_E2K']!='missing':
      e2k_data.loc[ i,'GAAP_E2K']='hybrid'
      e2k_data.loc[ i,'IAS_E2K']='hybrid'
    else:
      if e2k_data.loc[ i,'IASLIABILITYPRCACCOUNTID_E2K']!='missing' and e2k_data.loc[ i,'IASASSETPRCACCOUNTID_E2K']=='missing':
        e2k_data.loc[ i,'IAS_E2K']=int(e2k_data.loc[ i,'IASLIABILITYPRCACCOUNTID_E2K'])
      elif e2k_data.loc[ i,'IASLIABILITYPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ i,'IASASSETPRCACCOUNTID_E2K']!='missing':
        e2k_data.loc[ i,'IAS_E2K']=int(e2k_data.loc[ i,'IASASSETPRCACCOUNTID_E2K'])
        
      elif e2k_data.loc[ i,'IASLIABILITYPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ i,'IASASSETPRCACCOUNTID_E2K']=='missing':
        e2k_data.loc[ i,'IAS_E2K']='missing'
      else:
        e2k_data.loc[ i,'IAS_E2K']='missing' 


      if e2k_data.loc[ i,'GAAPLIABILITYPRCACCOUNTID_E2K']!='missing' and  e2k_data.loc[ i,'GAAPASSETPRCACCOUNTID_E2K']=='missing':
        e2k_data.loc[ i,'GAAP_E2K']=int(e2k_data.loc[ i,'GAAPLIABILITYPRCACCOUNTID_E2K'])
      elif e2k_data.loc[ i,'GAAPLIABILITYPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ i,'GAAPASSETPRCACCOUNTID_E2K']!='missing':
        e2k_data.loc[ i,'GAAP_E2K']=int(e2k_data.loc[ i,'GAAPASSETPRCACCOUNTID_E2K'])
      elif  e2k_data.loc[ i,'GAAPLIABILITYPRCACCOUNTID_E2K']=='missing' and e2k_data.loc[ i,'GAAPASSETPRCACCOUNTID_E2K']=='missing':
        e2k_data.loc[ i,'GAAP_E2K']='missing'
      else:
        e2k_data.loc[ i,'GAAP_E2K']='missing'

#MERGING E2K MI RCG
rcg_mi_merged['E2KACCT_NBR_RCG'] = rcg_mi_merged['E2KACCT_NBR_RCG'].astype(str)
e2k_data['E2KACCOUNTMNEMONIC_E2K'] = e2k_data['E2KACCOUNTMNEMONIC_E2K'].astype(str)
rcg_mi_e2k_merged = pd.merge(  rcg_mi_merged,e2k_data, how='inner', left_on=['E2KACCT_NBR_RCG'], right_on = ['E2KACCOUNTMNEMONIC_E2K'])
del rcg_mi_e2k_merged['E2KACCOUNTMNEMONIC_E2K']

#deleting hybrid accounts
delete_row = rcg_mi_e2k_merged[rcg_mi_e2k_merged["IAS_E2K"]=='hybrid'].index 
rcg_mi_e2k_merged = rcg_mi_e2k_merged.drop(delete_row)

#READING PRC
prc_data = pd.read_csv("PRC_REF.csv",skipinitialspace=True)
del_cols=['PRCACCOUNTFRENCHLABEL','PRCACCOUNTENGLISHLABEL','ACCOUNTINGNORM','VALIDITYSTARTDATE','VALIDITYENDDATE','PCINFOFIINDICATOR','INFOFI60INDICATOR','DRACALLOCATION','PRCBRANCHESNUMBER','START_DATE','END_DATE','STATUS']
for i in del_cols:
  del prc_data[i]
prc_data.fillna('missing', inplace=True)
ias_prc_data = prc_data.add_suffix('_IAS_PRC')
gaap_prc_data = prc_data.add_suffix('_GAAP_PRC')

#MERGING E2K PRC MI E2K
rcg_mi_e2k_prcias_merged= pd.merge(rcg_mi_e2k_merged,ias_prc_data,  how='left', left_on=['IAS_E2K'], right_on = ['PRC_ID_IAS_PRC'])
del rcg_mi_e2k_prcias_merged['PRC_ID_IAS_PRC']
rcg_mi_e2k_prc_merged = pd.merge(rcg_mi_e2k_prcias_merged, gaap_prc_data,  how='left', left_on=['GAAP_E2K'], right_on = ['PRC_ID_GAAP_PRC'])
del rcg_mi_e2k_prc_merged ['PRC_ID_GAAP_PRC']
rcg_mi_e2k_prc_merged.fillna('missing', inplace=True)

#READING GRIT_E2K
grit_e2k_data = pd.read_csv("GRIT_E2k_PCI_G2279.csv",skipinitialspace=True)
grit_e2k_data.fillna('missing', inplace=True)
grit_e2k_data = grit_e2k_data.add_suffix('_GRIT_E2k')
del_cols=['PUBLISH_DATE_GRIT_E2k','INVENTORY_DATE_GRIT_E2k','PRC_IAS_LIABILITY_GRIT_E2k','PRC_IAS_ASSET_GRIT_E2k','PRC_FR_LIABILITY_GRIT_E2k','PRC_FR_ASSET_GRIT_E2k','E2KPCI_LOCREP_CCY_AMT_GRIT_E2k','E2KPCI_LOCREP_CCY_AMT_CPT_GRIT_E2k','E2KPCI_GRP_AMT_GRIT_E2k']
for i in del_cols:
  del grit_e2k_data[i]

#READING GRIT_PRC
grit_prc_data = pd.read_csv("GRIT_PRC_ADJ_2020.csv",skipinitialspace=True)
grit_prc_data.fillna('missing', inplace=True)
grit_prc_data = grit_prc_data.add_suffix('_GRIT_PRC')
#MERGING GRIT_E2K AND GRIT_PRC
grit_e2k_enitiy_list=grit_e2k_data['ENTITY_GRIT_E2k'].tolist()
grit_e2k_counterparty_list=grit_e2k_data['COUNTERPART_GRIT_E2k'].tolist()
grit_e2k_crncy_list=grit_e2k_data['CURRENCY_GRIT_E2k'].tolist()
grit_e2k_prc_ias_list=grit_e2k_data['PRC_IAS_GRIT_E2k'].tolist()
grit_e2k_prc_fr_list=grit_e2k_data['PRC_FR_GRIT_E2k'].tolist()
grit_e2k_chap_fr_list=grit_e2k_data['CHPFR_GRIT_E2k'].tolist()
grit_e2k_chap_ias_list=grit_e2k_data['CHPIAS_GRIT_E2k'].tolist()
grit_prc_entity_list=grit_prc_data['ENTITY_GRIT_PRC'].tolist()
grit_prc_counterparty_list=grit_prc_data['COUNTERPART_GRIT_PRC'].tolist()
grit_prc_crncy_list=grit_prc_data['TRAN_CURRENCY_GRIT_PRC'].tolist()
grit_prc_prc_list=grit_prc_data['PRC_GRIT_PRC'].tolist() 
grit_prc_chap_list=grit_prc_data['CHAPTER_GRIT_PRC'].tolist() 


for j in range(0,len(grit_e2k_enitiy_list)):
  for i in range(0,len(grit_prc_chap_list)):
    if grit_prc_entity_list[i]==grit_e2k_enitiy_list[j] and grit_prc_counterparty_list[i]==grit_e2k_counterparty_list[j] and grit_prc_crncy_list[i]==grit_e2k_crncy_list[j]:
      q1=grit_prc_prc_list[i]
      q2=grit_e2k_prc_ias_list[j]
      q3=grit_e2k_prc_fr_list[j]
      if str(q1)==str(q2) or str(q1)==str(q3):
        w1=grit_prc_chap_list[i] 
        w2=grit_e2k_chap_fr_list[j]
        w3=grit_e2k_chap_ias_list[j]
        if (w1==w2) or (w1==w3):
          grit_e2k_data.loc[j,'PRC_GRIT_PRC']=str(grit_prc_data.loc[i,'PRC_GRIT_PRC'])
          grit_e2k_data.loc[j,'CHAPTER_GRIT_PRC']=str(grit_prc_data.loc[i,'CHAPTER_GRIT_PRC'])
          grit_e2k_data.loc[j,'GAAP_TYPE_GRIT_PRC']=str(grit_prc_data.loc[i,'GAAP_TYPE_GRIT_PRC'])
          grit_e2k_data.loc[j,'TRAN_AMOUNT_ADJ_GRIT_PRC']=str(grit_prc_data.loc[i,'TRAN_AMOUNT_ADJ_GRIT_PRC'])
          grit_e2k_data.loc[j,'TOTAL_AFTER_ADJ_GRIT_PRC']=str(grit_prc_data.loc[i,'TOTAL_AFTER_ADJ_GRIT_PRC'])
          grit_e2k_data.loc[j,'ADJUSTMENT_TYPE_GRIT_PRC']=str(grit_prc_data.loc[i,'ADJUSTMENT_TYPE_GRIT_PRC'])

          
#deleting non mi records
grit_e2k_datas=grit_e2k_data.copy()
for i in range(0,len(grit_e2k_datas)):
  if grit_e2k_data.loc[i,'SOURCE_GRIT_E2k']!='MI':
     grit_e2k_data=grit_e2k_data.drop(i)
grit_e2k_data.fillna('missing', inplace=True)

#OPENING INFOFY
infofy_data = pd.read_csv("grit_reclass_data.csv",skipinitialspace=True)
infofy_data.fillna('missing', inplace=True)
infofy_data= infofy_data.add_suffix('_INFOFY')

#MERGING INFOFY,GRIT_E2K,GRIT_ADJ
grit_e2k_enitiy_list=grit_e2k_data['ENTITY_GRIT_E2k'].tolist()
grit_e2k_counterparty_list=grit_e2k_data['COUNTERPART_GRIT_E2k'].tolist()
grit_e2k_prc_ias_list=grit_e2k_data['PRC_IAS_GRIT_E2k'].tolist()
grit_e2k_prc_fr_list=grit_e2k_data['PRC_FR_GRIT_E2k'].tolist()
grit_e2k_chap_fr_list=grit_e2k_data['CHPFR_GRIT_E2k'].tolist()
grit_e2k_crncy_list=grit_e2k_data['CURRENCY_GRIT_E2k'].tolist()
grit_e2k_chap_ias_list=grit_e2k_data['CHPIAS_GRIT_E2k'].tolist()
infofy_entity_list=infofy_data['ENTITY_INFOFY'].tolist()
infofy_counterparty_list=infofy_data['COUNTERPART_INFOFY'].tolist()
infofy_crncy_list=infofy_data['TRAN_CURRENCY_INFOFY'].tolist()
infofy_prc_list=infofy_data['ORIG_PRC_INFOFY'].tolist() 
infofy_chap_list=infofy_data['ORIG_CHAPTER_INFOFY'].tolist() 
infofy_crncy_list=infofy_data['TRAN_CURRENCY_INFOFY'].tolist()
for j in range(0,len(grit_e2k_enitiy_list)):
  for i in range(0,len(infofy_chap_list)):
    if infofy_entity_list[i]==grit_e2k_enitiy_list[j] and infofy_counterparty_list[i]==grit_e2k_counterparty_list[j] and infofy_crncy_list[i]==grit_e2k_crncy_list[j]:
      q1=infofy_prc_list[i]
      q2=grit_e2k_prc_ias_list[j]
      q3=grit_e2k_prc_fr_list[j]
      if str(q1)==str(q2) or str(q1)==str(q3):
        w1=infofy_chap_list[i] 
        w2=grit_e2k_chap_fr_list[j]
        w3=grit_e2k_chap_ias_list[j]
        if (w1==w2) or (w1==w3):
          grit_e2k_data.loc[j,'ORIG_PRC_INFOFY']=str(infofy_data.loc[i,'ORIG_PRC_INFOFY'])
          grit_e2k_data.loc[j,'RECLASS_PRC_INFOFY']=str(infofy_data.loc[i,'RECLASS_PRC_INFOFY'])

          grit_e2k_data.loc[j,'ORIG_CHAPTER_INFOFY']=str(infofy_data.loc[i,'ORIG_CHAPTER_INFOFY'])
          grit_e2k_data.loc[j,'RECLASS_CHAPTER_INFOFY']=str(infofy_data.loc[i,'RECLASS_CHAPTER_INFOFY'])

          grit_e2k_data.loc[j,'ORIG_PRC_MNEMONIC_INFOFY']=str(infofy_data.loc[i,'ORIG_PRC_MNEMONIC_INFOFY'])
          grit_e2k_data.loc[j,'RECLASS_PRC_MNEMONIC_INFOFY']=str(infofy_data.loc[i,'RECLASS_PRC_MNEMONIC_INFOFY'])

          grit_e2k_data.loc[j,'RIG_ASSET_LIAB_FLAG_INFOFY']=str(infofy_data.loc[i,'ORIG_ASSET_LIAB_FLAG_INFOFY'])
          grit_e2k_data.loc[j,'RECLASS_ASSET_LIAB_FLAG_INFOFY']=str(infofy_data.loc[i,'RECLASS_ASSET_LIAB_FLAG_INFOFY'])


#MERGING MI,E2K,RCG,PRC,GRIT_ADJ,GRIT_E2K,INFOFY
rcg_grit_e2k_prc_infofy_join = pd.merge(rcg_mi_e2k_prc_merged, grit_e2k_data,  how='left', left_on=['E2KACCT_NBR_RCG','BUS_UNIT_RCG','DEPT_ID_RCG','CCY_CD_RCG','ALTACCT_RCG'], right_on = ['ACCOUNT_GRIT_E2k','BUSINESS_UNIT_GRIT_E2k','DEPTID_GRIT_E2k','CURRENCY_GRIT_E2k','ALTACCT_GRIT_E2k'])
del_cols=['ACCOUNT_GRIT_E2k','BUSINESS_UNIT_GRIT_E2k','DEPTID_GRIT_E2k','CURRENCY_GRIT_E2k','ALTACCT_GRIT_E2k','AFFILIATE_GRIT_E2k']
for i in del_cols:
  del rcg_grit_e2k_prc_infofy_join [i]
rcg_grit_e2k_prc_infofy_join .fillna('missing', inplace=True)

rcg_grit_e2k_prc_infofy_join .to_csv('grit_rcg_mi_e2k_prc_infofy.csv') 
