#!/usr/bin/env python
# coding: utf-8

# In[80]:



import pandas as pd
def partner_network_creater(datamyne_sliced):
    T0=datamyne_sliced['Consignee Declared'].unique().tolist()
    company_names=list()
    tier=[]
    parent=[]
    root_parent=[]
    address=[]
    for i in T0:
        company_names.append(i)
        tier.append('T0')

        # print('hi')
        t1_names=datamyne_sliced.loc[datamyne_sliced['Consignee Declared']==i]

        #addresss for company
        
        parent.append('')
        root_parent.append(i)
        company_names.append(t1_names['Shipper Declared'].unique().tolist())
        # print(len(t1_names['Shipper Declared'].unique().tolist()))
        length=len(t1_names['Shipper Declared'].unique().tolist())
        for j in range(length):
            # print(j)
            tier.append('T1')
            parent.append(i)
            root_parent.append(i)

    print(len(company_names),len(tier),len(parent),len(root_parent))
    df=pd.DataFrame(columns=['company_names'])
    df['company_names']=company_names
    df= df.explode('company_names')
    # df.drop(df.tail(1).index,inplace=True) # drop last n rows
    df['tier']=tier
    df['parent_company']=parent
    df['root_company']=root_parent
    # df
    #  address for T1
    addresses=datamyne_sliced[['Shipper Declared', 'Shipper Address','Consignee Declared']]
    addresses=addresses.drop_duplicates(subset=['Shipper Declared', 'Shipper Address','Consignee Declared'])
    addresses.shape
    dfnew=pd.merge(df,addresses,left_on=['company_names','root_company'],right_on=['Shipper Declared','Consignee Declared'],how='left')
    # dfnew.shape
    dfnew=dfnew[['company_names', 'tier', 'parent_company', 'root_company',
        'Shipper Address']]

    # address for T0
    T0_addresses=datamyne_sliced[['Consignee Declared','Consignee address']]
    T0_addresses=T0_addresses.drop_duplicates(subset=['Consignee Declared','Consignee address'])
    dfnew_all=pd.merge(dfnew,T0_addresses,left_on=['company_names'],right_on=['Consignee Declared'],how='left')
    # dfnew_all.shape
    dfnew_all=dfnew_all[['company_names', 'tier', 'parent_company', 'root_company',
       'Shipper Address','Consignee address']]
    
    dfnew_all.loc[(dfnew_all['tier']=='T0') & dfnew_all['Shipper Address'].notnull()]=' '

    dfnew_all['Shipper Address'] = dfnew_all['Shipper Address'].fillna('')
    dfnew_all['Consignee address'] = dfnew_all['Consignee address'].fillna('')


    
    # Using + operator to combine two columns
    dfnew_all["Address"] = dfnew_all['Shipper Address'].astype(str) +" "+ dfnew_all["Consignee address"].astype(str)
    

    return dfnew_all
def main():
    datamyne_=pd.read_csv("adidas.csv")
    print('done reading data')
    print(datamyne_.columns)
    datamyne_ = datamyne_[datamyne_['Shipper Declared'].notna()]
    # datamyne_['Shipper Declared']=datamyne_['Shipper Declared Address']
    # datamyne_ = datamyne_.rename(columns={'Shipper Declared': 'Shipper Declared Address'}, axis=1)
    datamyne_.rename(columns = {'Shipper Declared Address':'Shipper Address'}, inplace = True)
    print(datamyne_.columns)
    # datamyne_=datamyne_.drop(['row_id'],axis=1)
    # print(datamyne_.shape)
    datamyne_=datamyne_.drop_duplicates()
    # print(datamyne_.shape)
    datamyne_['Consignee address'] = datamyne_[['Consignee State','Consignee City']].apply(lambda x: ', '.join(x[x.notnull()]), axis = 1)
    dfnew_all=partner_network_creater(datamyne_)
    print(dfnew_all.shape)
    dfnew_all=dfnew_all[dfnew_all['company_names']!='NOT DECLARED']
    print(dfnew_all.columns)
    dfnew_all = dfnew_all[dfnew_all['Address'].notna()]
    dfnew_all=dfnew_all.drop_duplicates(subset=['company_names', 'tier', 'parent_company', 'root_company'],keep="first")
    dfnew_all.to_csv('adidas_PN_T0_T1.csv')

    print(dfnew_all)
    # return dfnew_all 

if __name__ == '__main__':
    main()
# datamyne_partner_network.py
# Displaying datamyne_partner_network.py.


# # In[40]:


# import pandas as pd
# data=pd.read_csv('corteva.csv')


# # In[73]:


# data.columns


# # In[56]:


# data['Consignee Declared'].unique()


# # In[43]:


# data['Consignee Declared']---t0


# # In[44]:


# data['Shipper Declared']---t1


# # In[ ]:


################################### pyspark ######################

