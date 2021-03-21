'''This is not a working code...YOU HAVE TO ADD GRAPH VISUALIZATION PART[REFER: https://github.com/lucksd356/DecisionTrees/blob/master/dtree.py WE ARE USING SAME LOGIC )
(FOR SAVING BRANCHES WE ARE USING LIST..SO CHANGE THAT CODE ACCORDINGLY)]..VENKATESH WILL PROVIDE OTHER FUNCTIONS..)

'''
import csv
from collections import defaultdict
import pydotplus


class DecisionTree:
    """Binary tree implementation with true and false branch. """
    def __init__(self, col=None, branches=[]):
        self.col = col
        self.branches=branches

def growDecisionTreeFrom(dataset,cnsqnt,ante):
    #dataset dataframe does not contain antecedent columns
    if len(datase) == 0: return DecisionTree()
    col_with_low_p_value=p_value()#function to find the column in raw file with lowest p value.Fuction should return a tuple with child_name and p_value
    if col_with_low_p_value[1] >0.005:
      return DecisionTree(col=col_with_low_p_value)
    else:
      child=binary_cols_of_given_raw_col(col_with_low_p_value[0])#binary column names corresponding to the column in raw file with lowest p value
      i=0
   
      for x in child:
        branches=[]
        data_copy_=dataset.copy()
        data_copy=data_copy_.loc[data_copy_[x] == 1]
        del data_copy[x]
        data_copy=data_copy[,-(which(colSums(data_copy)==0))]#deleting columns with all values in the column=0
        antecedent=ante.copy() 
        antecedent.extend(x)
        branch.append(growDecisionTreeFrom(data_copy,cnsqnt,antecedent))
        i=i+1
      return DecisionTree(col=col_with_low_p_value, branches=branch)     
cnsqnt='not_SECONDBIT=1_APP_ORGIN_RCG'
ante=['not_PEC/QTZ_SOURCE_APPLICATION_ID_MI']
if __name__ == '__main__':
  bHeader = True
  data1= pd.read_csv('in.csv')
  print(data1.columns)
  data=data1.loc[data1['not_PEC/QTZ_SOURCE_APPLICATION_ID_MI'] == 1]
  data=data[,-(which(colSums(data)==0))]#deleting columns with all values in the column=0
  del data['not_PEC/QTZ_SOURCE_APPLICATION_ID_MI']
  decisionTree = growDecisionTreeFrom(data,cnsqnt,ante)
  result = plot(decisionTree)
  '''dot_data = dotgraph(decisionTree)
  graph = pydotplus.graph_from_dot_data(dot_data)
  graph.write_png("iris.png")'''


