import pandas as pd 
import numpy as np

#file names
rcg_name="RCG_G2279_20210121.csv"
mi_name="MI_G2279.csv"
e2k_name="E2K_REF.csv"
prc_name="PRC_REF.csv"


#reading RCG
rcg_data = pd.read_csv(rcg_name,skipinitialspace=True)
rcg_datas.fillna('missing', inplace=True)
del_cols=['CHAR_FIELD1','CHAR_FIELD2','CHAR_FIELD3','CHAR_FIELD4','CHAR_FIELD5','AMOUNT_FIELD1','AMOUNT_FIELD2','AMOUNT_FIELD3','AMOUNT_FIELD4','AMOUNT_FIELD5','DATE_FIELD1','DATE_FIELD2','DATE_FIELD3','E2KACCT_DESC','LCL_CCY_DIFF_AMT']
for i in del_cols:
  del rcg_datas[i]
rcg_datas = rcg_datas.add_suffix('_RCG')
rcg_cols=list(rcg_datas.columns) 

#GROUPING RCG BASED ON ACCOUNT NUMBER,CCY,DEPT
grp_rcg=rcg_datas.groupby(['E2KACCT_NBR_RCG','DEPT_ID_RCG','CCY_CD_RCG']).agg({'E2KACCT_NBR_RCG': lambda x: x.unique(),'DEPT_ID_RCG': lambda x: x.unique(),'CCY_CD_RCG': lambda x: x.unique(),'TRAN_MEAS_SYS_SRC_CD_RCG': lambda x: x.unique().tolist(),'FIN_CLASS_RCG': lambda x: x.unique().tolist(),'ALTACCT_RCG': lambda x: x.unique().tolist(),'GL_PROD_CD_RCG': lambda x: x.unique().tolist(),'ORIG_CCY_MEAS_AMT_RCG': lambda x: x.unique().tolist(),'GLACCT_BAL_AMT_RCG': lambda x: x.unique().tolist(),'ORIG_CCY_DIFF_AMT_RCG': lambda x: x.unique().tolist(),'GLACCT_NBR_RCG': lambda x: x.unique().tolist(),'LOAD_MD_RCG': lambda x: x.unique().tolist(),'APP_ORIGIN_RCG': lambda x: x.unique().tolist(),'E2K_PRODUCT_CD_RCG': lambda x: x.unique().tolist(),'OPERATING_UNIT_RCG': lambda x: x.unique().tolist(),'REGION_RCG': lambda x: x.unique().tolist()})
grp_rcg_cols=list(grp_rcg.columns)
random_col=grp_rcg['E2KACCT_NBR_RCG'].tolist()
rcg_data= pd.DataFrame(0, index=np.arange(len(random_col)), columns=grp_rcg_cols) 
for i in grp_rcg_cols:
  rcg_data[i] = grp_rcg[i].values
rcg_data.insert(0, 'ID', range(0, 0 + len(rcg_data)))

#reading MI
mi_data = pd.read_csv(mi_name,skipinitialspace=True)
mi_data.fillna('missing', inplace=True)
del_cols=['DLR_PUBLISH_DATE','DEAL_ID','TRANSACTION_ID','INVENTORY_DATE','TRANSACTION_TYPE','PORTFOLIO_ID','PARTY_ID','PRODUCT_GUID','ADJUSTMENT_DATE','VALUE_DATE','AFF_CODFIL','CONSO1_SUFF','OBJECT','MATURITY_DATE','CLOSING_DATE']
for i in del_cols:
  del mi_data[i]
mi_data = mi_data.add_suffix('_MI')

#joining RCG and MI
rcg_mi_join = pd.merge(rcg_data, mi_data,  how='inner', left_on=['E2KACCT_NBR_RCG','DEPT_ID_RCG','CCY_CD_RCG'], right_on = ['ACCOUNT_MI','DEPTID_MI','CURRENCY_MI'])

#grouping rows of MI corresponds to single RCG row
mi=rcg_mi_join.groupby('ID').agg({'ID': lambda x: x.unique(),'TRAN_AMOUNT_MI': lambda x: x.unique().tolist(),'EVENT_NATURE_MI': lambda x: x.unique().tolist(),'ADJUSTMENT_TYPE_MI': lambda x: x.unique().tolist(),'SOURCE_APPLICATION_ID_MI': lambda x: x.unique().tolist(),'INVENTORY_TYPE_MI': lambda x: x.unique().tolist(),'OPERATION_CODE_MI': lambda x: x.unique().tolist(),'OPERATION_DIRECTION_MI': lambda x: x.unique().tolist(),'CONSO1_MI': lambda x: x.unique().tolist(),'PRC_IAS_MI': lambda x: x.unique().tolist()})
mi_new=rcg_mi_join.groupby('ID').agg({'TRAN_AMOUNT_MI': lambda x: x.unique().tolist(),'EVENT_NATURE_MI': lambda x: x.unique().tolist(),'ADJUSTMENT_TYPE_MI': lambda x: x.unique().tolist(),'SOURCE_APPLICATION_ID_MI': lambda x: x.unique().tolist(),'INVENTORY_TYPE_MI': lambda x: x.unique().tolist(),'OPERATION_CODE_MI': lambda x: x.unique().tolist(),'OPERATION_DIRECTION_MI': lambda x: x.unique().tolist(),'CONSO1_MI': lambda x: x.unique().tolist(),'PRC_IAS_MI': lambda x: x.unique().tolist()})

#TO REMOVE WRONG GLAAM FROM RCG
rows_present_in_rcg_mi=mi['ID'].tolist() #RECORD ID'S OF ROWS WHICH HAVE A MATCH IN MI
wrong_glaam=[]#RECORD ID'S OF ROWS WHICH DONT HAVE A MATCH IN MI
for i in range(0,len(rcg_data)):
  if i not in rows_present_in_rcg_mi:
    gl=rcg_data.loc[ i,'GLACCT_BAL_AMT_RCG']
    total_gl=sum(rows_not_present_in_rcg_mi)
    if total_gl==0:
      inv=rcg_data.loc[ i,'ORIG_CCY_MEAS_AMT_RCG']
      total_inventory=sum(inv)
      if total_inventory==0:
        wrong_glaam.append(i)

for i in wrong_glaam:#deleting records from rcg
  delete_row = rcg_data[rcg_data["ID"]==i].index
  rcg_data = rcg_data.drop(delete_row)
  
rcg_mi_merged=pd.merge(rcg_data, mi_new,  how='left', left_on=['ID'], right_on = ['ID'])


#READING AND PROCESSING E2K
e2k_data = pd.read_csv(e2k_name,skipinitialspace=True)
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
#MERGING E2K,RCG,MI
rcg_mi_merged['E2KACCT_NBR_RCG'] = rcg_mi_merged['E2KACCT_NBR_RCG'].astype(str)
e2k_data['E2KACCOUNTMNEMONIC_E2K'] = e2k_data['E2KACCOUNTMNEMONIC_E2K'].astype(str)
rcg_mi_e2k_merged = pd.merge(  rcg_mi_merged,e2k_data, how='inner', left_on=['E2KACCT_NBR_RCG'], right_on = ['E2KACCOUNTMNEMONIC_E2K'])
del rcg_mi_e2k_merged['E2KACCOUNTMNEMONIC_E2K']

#deleting hybrid accounts
delete_row = rcg_mi_e2k_merged[rcg_mi_e2k_merged["IAS_E2K"]=='hybrid'].index 
rcg_mi_e2k_merged = rcg_mi_e2k_merged.drop(delete_row)

#READING PRC
prc_data = pd.read_csv(prc_name,skipinitialspace=True)
del_cols=['PRCACCOUNTFRENCHLABEL','PRCACCOUNTENGLISHLABEL','ACCOUNTINGNORM','VALIDITYSTARTDATE','VALIDITYENDDATE','PCINFOFIINDICATOR','INFOFI60INDICATOR','DRACALLOCATION','PRCBRANCHESNUMBER','START_DATE','END_DATE','STATUS']
for i in del_cols:
  del prc_data[i]
prc_data.fillna('missing', inplace=True)
ias_prc_data = prc_data.add_suffix('_IAS_PRC')
gaap_prc_data = prc_data.add_suffix('_GAAP_PRC')

#JOINING PRC,MI,RCG,E2K
rcg_mi_e2k_prcias_merged= pd.merge(rcg_mi_e2k_merged,ias_prc_data,  how='left', left_on=['IAS_E2K'], right_on = ['PRC_ID_IAS_PRC'])
del rcg_mi_e2k_prcias_merged['PRC_ID_IAS_PRC']
rcg_mi_e2k_prc_merged = pd.merge(rcg_mi_e2k_prcias_merged, gaap_prc_data,  how='left', left_on=['GAAP_E2K'], right_on = ['PRC_ID_GAAP_PRC'])
del rcg_mi_e2k_prc_merged ['PRC_ID_GAAP_PRC']
rcg_mi_e2k_prc_merged.fillna('missing', inplace=True)


rcg_mi_e2k_prc_merged.to_csv('binary.csv') #SAVING
