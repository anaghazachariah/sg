import pandas as pd
import numpy as np
def create_headers(col,file_name,suffix):
    feature_list=[]
    for i in col:
        column_values=file_name[i].tolist()
        dist_values_set=set(column_values)
        dist_values_list = list(dist_values_set)
        if suffix=='MI' and i=='ADJUSTMENT_TYPE' :
            if 'CNR' or 'ODS' in dist_values_list:
                cell_value=('INVENTORY_ADJ'+'_'+i+'_'+suffix).upper() 
            elif 'ODT' in dist_values_list:
                cell_value=('INVENTORY_ACCOUNTING_ADJ'+'_'+i+'_'+suffix).upper()
            elif 'STD' or 'ODC' in dist_values_list:                
                cell_value=('ACCOUNTING_ADJ'+'_'+i+'_'+suffix).upper()
            feature_list.append(cell_value) 
        else:   
            for j in dist_values_list:
                if j=='missing':
                    continue
                else:
                    cell_value=(str(j)+'_'+i+'_'+suffix).upper() 
                    feature_list.append(cell_value)  
    return feature_list
def binarize(file_name,e2k_name,prc_name,mi_name):
    rcg_data=pd.read_csv(file_name,skipinitialspace=True) 
    rcg_data.fillna('missing', inplace=True)
    rcg_data.columns = rcg_data.columns.str.replace(' ', '')
    e2k_ref=pd.read_csv(e2k_name,skipinitialspace=True) 
    e2k_ref.fillna('missing', inplace=True)
    e2k_ref.columns = e2k_ref.columns.str.replace(' ', '')
    prc_data=pd.read_csv(prc_name,skipinitialspace=True) 
    prc_data.fillna('missing', inplace=True)
    prc_data.columns = prc_data.columns.str.replace(' ', '')
    mi_data=pd.read_csv(mi_name,skipinitialspace=True) 
    mi_data.fillna('missing', inplace=True)
    mi_data.columns = mi_data.columns.str.replace(' ', '')
    mi_col=['ADJUSTMENT_TYPE','OPERATION_CODE','OPERATION_DIRECTION','TRANSACTION_TYPE','CONSO1','PRC_IAS']
    e2k_col=['ACCOUNTTYPE','CHARTOFACCOUNTTYPE','ACCOUNTINGNORM','ACCOUNTMONETARYTYPE','ACCOUNTBALANCE','ACCOUNTBALANCE','INTERNALEXTERNAL','IASECONOMICPURPOSE','VALUATIONMETHOD','TRANSACTIONALINDICATOR','INTERCOFOLLOWUPTYPE','BASELPERIMETER','BASELAMOUNTTYPE','GLOBALBASELRECONCILIATION','INTERNALPNLFAMILY','EXTPNLFAMILY']
    #prc_col=['PRCACCOUNTMNEMONIC','PRCACCOUNTENGLISHNAME','ACCOUNTCLASS','ACCOUNTMONETARYTYPE','ACCOUNTBALANCE','PRCECONOMICPURPOSE','INTERCOFOLLOWUPTYPE','BASELPERIMETER']
    rcg_cols=['TRAN_MEAS_SYS_SRC_CD','CCY_CD','FIN_CLASS','AFFIL_CD','GLACCT_NBR']
    extra_cols=['INVENTORY_CREATION_QTZ_MI','INVENTORY_UPDATION_QTZ/PEC_MI','INVENTORYIS-_RCG','INVENTORY=0_RCG','INVENTORYIS+_RCG','GLIS-_RCG','GL=0_RCG','GLIS+_RCG','BREAKIS-_RCG','BREAK=0_RCG','BREAKIS+_RCG','FIRSTBIT=1_APP_ORGIN_RCG','SECONDBIT=1_APP_ORGIN_RCG','THIRDBIT=1_APP_ORGIN_RCG']
    mi_feature_list=create_headers(mi_col,mi_data,'MI')
    rcg_feature_list=create_headers(rcg_cols,rcg_data,'RCG')
    e2k_feature_list=create_headers(e2k_col,e2k_ref,'E2K')
    #prc_feature_list=create_headers(prc_col,prc_data,'PRC_FRENCH')
    #prc_feature_list=create_headers(prc_col,prc_data,'PRC_IAS')
    feature_list=extra_cols+mi_feature_list+rcg_feature_list+e2k_feature_list                                  
    bin_data = pd.DataFrame(0, index=np.arange(len(rcg_data)), columns=feature_list) 
    for ii in range(0,len(rcg_cols)):
        col=rcg_data[rcg_cols[ii]].tolist()
        for i in range(0,len(col)):             
            if col[i]=='missing':
                continue
            else:
                attributes=(str(col[i])+'_'+rcg_cols[ii]+'_RCG').upper()  
                bin_data.loc[i,attributes]=1
    rcg_col=['ORIG_CCY_MEAS_AMT','GLACCT_BAL_AMT','ORIG_CCY_DIFF_AMT']
    for ii in range(0,len(rcg_col)):
        col=rcg_data[rcg_col[ii]].tolist()
        for i in range(0,len(col)):         
            if col[i]=='missing':
                continue
            else:
                if ii==0:
                    if float(col[i])<0:
                        bin_data.loc[ i,'INVENTORYIS-_RCG']=1    
                                     
                    elif float(col[i])==0:
                        bin_data.loc[ i,'INVENTORY=0_RCG']=1
                    else:                       
                        bin_data.loc[ i,'INVENTORYIS+_RCG']=1
                      
                elif ii==2:
                    if float(col[i])<0:
                        bin_data.loc[ i,'BREAKIS-_RCG']=1
                    
                    elif float(col[i])==0:
                        bin_data.loc[ i,'BREAK=0_RCG']=1
                    else:
                        
                        bin_data.loc[ i,'BREAKIS+_RCG']=1
                elif ii==1:
                    if float(col[i])<0:
                        bin_data.loc[ i,'GLIS-_RCG']=1
                       
                    
                    elif float(col[i])==0:
                        bin_data.loc[ i,'GL=0_RCG']=1
                    else:                        
                        bin_data.loc[ i,'GLIS+_RCG']=1
                     
    app_orgin_list=rcg_data['APP_ORIGIN'].tolist()
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
    e2k_list=rcg_data['E2KACCT_NBR'].tolist()
    e2k_ref_list=e2k_ref['E2KACCOUNTMNEMONIC'].tolist()
    #prc_ref_list=prc_data['PRC_ID'].tolist()
    for i in range(0,len(e2k_list)):      
        if e2k_list[i]=='missing':
            continue
        else:         
            try:
                val_in=e2k_ref_list.index(e2k_list[i])                  
                for j in e2k_col:
                    
                    val=e2k_ref.iloc[val_in][j]
                    if val=='missing':
                        continue
                    else: 
                        attri=(str(val)+'_'+j+'_E2K').upper()  
                        bin_data.loc[i,attri]=1   
            except:          
                continue
    rcg_E2KACCT_NBR_list=rcg_data['E2KACCT_NBR'].tolist()
    rcg_CCY_CD_list=rcg_data['CCY_CD'].tolist()
    rcg_DEPT_ID_list=rcg_data['DEPT_ID'].tolist()
    rcg_BUS_UNIT_list=rcg_data['BUS_UNIT'].tolist()
    mi_E2KACCT_NBR_list=mi_data['ACCOUNT'].tolist()
    mi_CCY_CD_list=mi_data['CURRENCY'].tolist()
    mi_DEPT_ID_list=mi_data['DEPTID'].tolist()
    mi_BUS_UNIT=mi_data['BUSINESS_UNIT'].tolist() 
    for i in range(0,len(rcg_E2KACCT_NBR_list)):       
        if rcg_E2KACCT_NBR_list[i]=='missing' or rcg_CCY_CD_list[i]=='missing'  or rcg_DEPT_ID_list[i]=='missing' or rcg_BUS_UNIT_list[i]=='missing' :
            continue
        else:         
            try:
                cnt=0
                src_applization_id=[]
                t=0
                for j in range(0,len(mi_E2KACCT_NBR_list)):
                    if rcg_E2KACCT_NBR_list[i]==mi_E2KACCT_NBR_list[j] and rcg_CCY_CD_list[i]==mi_CCY_CD_list[j] and rcg_DEPT_ID_list[i]==mi_DEPT_ID_list[j] and rcg_BUS_UNIT_list[i]==mi_BUS_UNIT[j]:
                        index=cnt
                        src_applization_id.append(mi_data.iloc[index]['SOURCE_APPLICATION_ID'])
                        if t==0:
                            t=1
                            for k in mi_col:
                                value=mi_data.iloc[index][k]
                                if value=='missing':
                                    continue
                                if k=='ADJUSTMENT_TYPE':
                                    if value in ['CNR','ODS']:
                                        bin_data.loc[i,'INVENTORY_ADJ_ADJUSTMENT_TYPE_MI']=1 
                                    elif value in ['ODT']:
                                        bin_data.loc[i,'INVENTORY_ACCOUNTING_ADJ_ADJUSTMENT_TYPE_MI']=1 
                                    elif value in ['STD','ODC']:
                                        bin_data.loc[i,'ACCOUNTING_ADJ_ADJUSTMENT_TYPE_MI']=1 
                                    
                                else:
                                    attributes=(str(value)+'_'+k+'_MI').upper()   
                                    bin_data.loc[i,attributes]=1 
                        
                    cnt=cnt+1 
                
                if 'PEC' in src_applization_id:
                    if  'QTZ' in src_applization_id:
                        
                        bin_data.loc[i,'INVENTORY_UPDATION_QTZ/PEC_MI']=1
                else:
                    if 'QTZ' in src_applization_id:
                        
                        bin_data.loc[i,'INVENTORY_CREATION_QTZ_MI']=1
            except:          
                continue  
    return bin_data        
data=binarize('RCG_G2279.csv','E2K_REF.csv','PRC_REF.csv','MI_G2279.csv')


data.to_csv('binary.csv')
