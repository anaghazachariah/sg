import pandas as pd 
import numpy as np
rcg_file="RCG_G2279_20210121.csv"
mi_file="MI_G2279.csv"
e2k_file="E2K_REF.csv"
prc_file="PRC_REF.csv"
grit_e2k_file="GRIT_E2k_PCI_G2279.csv"

#reading RCG
rcg_data = pd.read_csv(rcg_file,skipinitialspace=True)
rcg_data.fillna('missing', inplace=True)
del_cols=['CHAR_FIELD1','CHAR_FIELD2','CHAR_FIELD3','CHAR_FIELD4','CHAR_FIELD5','AMOUNT_FIELD1','AMOUNT_FIELD2','AMOUNT_FIELD3','AMOUNT_FIELD4','AMOUNT_FIELD5','DATE_FIELD1','DATE_FIELD2','DATE_FIELD3','E2KACCT_DESC','LCL_CCY_DIFF_AMT']
for i in del_cols:
  del rcg_data[i]
rcg_data = rcg_data.add_suffix('_RCG')
rcg_data.insert(0, 'ID', range(0, 0 + len(rcg_data)))
rcg_cols=list(rcg_data.columns) 

#READING MI
mi_data = pd.read_csv(mi_file,skipinitialspace=True)
mi_data.fillna('missing', inplace=True)
del_cols=['DLR_PUBLISH_DATE','DEAL_ID','TRANSACTION_ID','INVENTORY_DATE','TRANSACTION_TYPE','PORTFOLIO_ID','PARTY_ID','PRODUCT_GUID','ADJUSTMENT_DATE','VALUE_DATE','AFF_CODFIL','CONSO1_SUFF','OBJECT','MATURITY_DATE','CLOSING_DATE']
for i in del_cols:
  del mi_data[i]
mi_data = mi_data.add_suffix('_MI')

#JOIN MI AND RCG
rcg_mi_join = pd.merge(rcg_data, mi_data,  how='inner', left_on=['E2KACCT_NBR_RCG','BUS_UNIT_RCG','DEPT_ID_RCG','CCY_CD_RCG','ALTACCT_RCG','AFFIL_CD_RCG','GL_PROD_CD_RCG','OPERATING_UNIT_RCG','E2K_PRODUCT_CD_RCG'], right_on = ['ACCOUNT_MI','BUSINESS_UNIT_MI','DEPTID_MI','CURRENCY_MI','ALTACCT_MI','AFFILIATE_MI','OBJECT_CODE_MI','OPERATING_UNIT_MI','PRODUCT_MI'])
del_cols=['ACCOUNT_MI','BUSINESS_UNIT_MI','DEPTID_MI','CURRENCY_MI','ALTACCT_MI','AFFILIATE_MI','OBJECT_CODE_MI','OPERATING_UNIT_MI','PRODUCT_MI']
for i in del_cols:
  del rcg_mi_join[i]

#GROUPING MI
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

rcg_mi_merged=pd.merge(rcg_data, mi_new,  how='left', left_on=['ID'], right_on = ['ID'])

#READING E2K
e2k_data = pd.read_csv(e2k_file,skipinitialspace=True)
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

#MERGING E2K,PRC,MI
rcg_mi_merged['E2KACCT_NBR_RCG'] = rcg_mi_merged['E2KACCT_NBR_RCG'].astype(str)
e2k_data['E2KACCOUNTMNEMONIC_E2K'] = e2k_data['E2KACCOUNTMNEMONIC_E2K'].astype(str)
rcg_mi_e2k_merged = pd.merge(  rcg_mi_merged,e2k_data, how='inner', left_on=['E2KACCT_NBR_RCG'], right_on = ['E2KACCOUNTMNEMONIC_E2K'])
del rcg_mi_e2k_merged['E2KACCOUNTMNEMONIC_E2K']

#deleting hybrid accounts
delete_row = rcg_mi_e2k_merged[rcg_mi_e2k_merged["IAS_E2K"]=='hybrid'].index 
rcg_mi_e2k_merged = rcg_mi_e2k_merged.drop(delete_row)

#READING PRC
prc_data = pd.read_csv(prc_file,skipinitialspace=True)
del_cols=['PRCACCOUNTFRENCHLABEL','PRCACCOUNTENGLISHLABEL','ACCOUNTINGNORM','VALIDITYSTARTDATE','VALIDITYENDDATE','PCINFOFIINDICATOR','INFOFI60INDICATOR','DRACALLOCATION','PRCBRANCHESNUMBER','START_DATE','END_DATE','STATUS']
for i in del_cols:
  del prc_data[i]
prc_data.fillna('missing', inplace=True)
ias_prc_data = prc_data.add_suffix('_IAS_PRC')
gaap_prc_data = prc_data.add_suffix('_GAAP_PRC')

#merging e2k prc mi rcg
rcg_mi_e2k_prcias_merged= pd.merge(rcg_mi_e2k_merged,ias_prc_data,  how='left', left_on=['IAS_E2K'], right_on = ['PRC_ID_IAS_PRC'])
del rcg_mi_e2k_prcias_merged['PRC_ID_IAS_PRC']
rcg_mi_e2k_prc_merged = pd.merge(rcg_mi_e2k_prcias_merged, gaap_prc_data,  how='left', left_on=['GAAP_E2K'], right_on = ['PRC_ID_GAAP_PRC'])
del rcg_mi_e2k_prc_merged ['PRC_ID_GAAP_PRC']
rcg_mi_e2k_prc_merged.fillna('missing', inplace=True)

#GRIT OPENING
grit_e2k_data = pd.read_csv(grit_e2k_file,skipinitialspace=True)
grit_e2k_data.fillna('missing', inplace=True)
grit_e2k_data = grit_e2k_data.add_suffix('_GRIT_E2k')
del_cols=['PUBLISH_DATE_GRIT_E2k','INVENTORY_DATE_GRIT_E2k','PRC_IAS_LIABILITY_GRIT_E2k','PRC_IAS_ASSET_GRIT_E2k','PRC_FR_LIABILITY_GRIT_E2k','PRC_FR_ASSET_GRIT_E2k', 	'PRC_IAS_GRIT_E2k','PRC_IAS_FLG_GRIT_E2k','PRC_FR_GRIT_E2k','PRC_FR_FLG_GRIT_E2k','E2KPCI_LOCREP_CCY_AMT_GRIT_E2k','E2KPCI_LOCREP_CCY_AMT_CPT_GRIT_E2k','E2KPCI_GRP_AMT_GRIT_E2k']
for i in del_cols:
  del grit_e2k_data[i]
#DELITING RECORDS WITH SOURCE!=MI
grit_e2k_datas=grit_e2k_data.copy()
for i in range(0,len(grit_e2k_datas)):
  if grit_e2k_data.loc[i,'SOURCE_GRIT_E2k']!='MI':
     grit_e2k_data=grit_e2k_data.drop(i)
#MERGE GRIT_E2K,MI,RCG,E2K,PRC
rcg_grit_e2k_join = pd.merge(rcg_mi_e2k_prc_merged, grit_e2k_data,  how='left', left_on=['E2KACCT_NBR_RCG','BUS_UNIT_RCG','DEPT_ID_RCG','CCY_CD_RCG','ALTACCT_RCG'], right_on = ['ACCOUNT_GRIT_E2k','BUSINESS_UNIT_GRIT_E2k','DEPTID_GRIT_E2k','CURRENCY_GRIT_E2k','ALTACCT_GRIT_E2k'])
del_cols=['ACCOUNT_GRIT_E2k','BUSINESS_UNIT_GRIT_E2k','DEPTID_GRIT_E2k','CURRENCY_GRIT_E2k','ALTACCT_GRIT_E2k','AFFILIATE_GRIT_E2k']
for i in del_cols:
  del rcg_grit_e2k_join [i]
  
rcg_grit_e2k_join.to_csv('grit_rcg_mi_e2k_prc.csv') 
