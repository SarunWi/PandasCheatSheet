#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import sys

INPUT_PATH 	= 'input\\'
RESULT_PATH = 'output\\'

left_file_name      = 'raw_prospect_with_id.csv'
right_file_name     = 'raw_customer_with_id.csv'

def main():
    _merge_in_large_file_()


# scenario when we have 2 csv file 
# merge them and keep only record that found both file 
def _merge_2_file_():
    external_key_column = 'Name'
    result_file_name    = 'merge_campaign_lead_with_lm_assign_by.csv'
    # Open left file
    df1 = pd.read_csv(INPUT_PATH+left_file_name,dtype={external_key_column:str})
    # Open right file
    df2 = pd.read_csv(INPUT_PATH+right_file_name,dtype={external_key_column:str,'Employee_Id__c':str})
	# merge them with external_key_column  
    result = pd.merge(df1,df2,on=external_key_column)
    # write to new result file 
    result.to_csv(RESULT_PATH+result_file_name)

def _filter_in_other_file_():
    external_key_column = 'ID_NUMBER_ENCRYPTED__C'
    result_file_name    = 'duplicate_prospect_with_customer.csv'
    # Open left file 
    df1 = pd.read_csv(INPUT_PATH + left_file_name,dtype={"ID_NUMBER_ENCRYPTED__C":str} )
    print('open left file')
    # Open right file
    df2 = pd.read_csv(INPUT_PATH + right_file_name,dtype={"ID_NUMBER_ENCRYPTED__C":str} )
    # filter them 
    print('open right file')
    result = df1[ df1[external_key_column].isin(df2[external_key_column])]
    print('after filter')
    # write to new result file
    result.to_csv(RESULT_PATH + result_file_name)
# scenario when we have 2 excel sheets
# want to get only record that have in first file
def _filter_not_in_():
    external_key_column = 'External_Key_Field'
    result_file_name    = 'result_file_name.csv'
    # Open left file 
    df1 = pd.read_csv(INPUT_PATH + left_file_name )
    # Open right file
    df2 = pd.read_csv(INPUT_PATH + right_file_name )
    # filter them 
    result = df1[~df1[external_key_column].isin(df2[external_key_column])]
    # write to new result file
    result.to_csv(RESULT_PATH + result_file_name)

# scenario when find record in specific list 
def _find_record_():
    external_key_column = 'External_Key_Field'
    result_file_name    = 'result_file_name.csv'
    find_value = ['1','2','3']
    df1 = pd.read_csv(INPUT_PATH + left_file_name )
    result = df1.loc[df1[external_key_column].isin(find_value)]
    # write to new result file
    result.to_csv(RESULT_PATH + result_file_name)    

def _custom_find_record_():
    result_file_name    = 'filter_prospect_need_to_convert.csv'
    df1 = pd.read_csv(INPUT_PATH + left_file_name ,dtype={'READY_FOR_CONVERSION_STAMP__C':str})
    result = df1.loc[df1['READY_FOR_CONVERSION__C']==True ]
    result = result.loc[result['READY_FOR_CONVERSION_STAMP__C']=='0.0']
    result.to_csv(RESULT_PATH + result_file_name)

def _find_duplicate_row_():
    result_file_name    = 'duplicate_id_customer.csv'
    df1 = pd.read_csv(INPUT_PATH + left_file_name ,dtype={'ID_NUMBER_ENCRYPTED__C':str},usecols=["ID","ID_NUMBER_ENCRYPTED__C"] )
    print('after read file',sys.getsizeof(df1))
    ids = df1["ID_NUMBER_ENCRYPTED__C"]
    df1 = df1[ids.isin(ids[ids.duplicated()])]
    df1.to_csv(RESULT_PATH + result_file_name)
    
def _filter_blank_row_():
    result_file_name    = 'filter_blank_row.csv'
    df1 = pd.read_csv(INPUT_PATH + left_file_name ,dtype={'ID_NUMBER_ENCRYPTED__C':str},usecols=["ID","ID_NUMBER_ENCRYPTED__C"] )
    df1 = df1.loc[df1['ID_NUMBER_ENCRYPTED__C']!= "" ]
    df1.to_csv(RESULT_PATH + result_file_name)

def _group_id_():
    result_file_name = 'group_id.csv'
    df1 = pd.read_csv(INPUT_PATH + left_file_name ,dtype={'ID_NUMBER_ENCRYPTED__C':str}).groupby(['ID_NUMBER_ENCRYPTED__C']).agg(['count']).sort_values(by='count', ascending=False)
    df1.to_csv(RESULT_PATH + result_file_name)

def _split_large_file_():
    max_file_row = 5000000
    file_row   = 0
    chunkrows   = 100000 #read 100k rows at a time
    result_file_name = 'customer_split_file'
    file_count = 1
    df = pd.read_csv(INPUT_PATH + 'raw_customer_with_id.csv',dtype={"ID_NUMBER_ENCRYPTED__C":str}, iterator=True, chunksize=chunkrows )
    for chunk in df: #for each 100k rows
        outname = result_file_name+ str(file_count)+'.csv'
        #append each output to same csv, using no header
        if( file_row == 0):
            chunk.to_csv(RESULT_PATH + outname, index=None)
        else :
            chunk.to_csv( RESULT_PATH + outname, mode='a',header=None, index=None)
        file_row = file_row + 100000
        if(file_row>=max_file_row):
            file_count = file_count+1
            file_row = 0

def _merge_in_large_file_():
    chunkrows   = 100000 #read 100k rows at a time
    result_file_name = 'merge_raw_customer_with_phone.csv'
    file_count = 1
    df1 = pd.read_csv(INPUT_PATH + 'raw_customer_with_phone.csv',dtype={"ID_NUMBER_ENCRYPTED__C":str} )
    df2 = pd.read_csv(INPUT_PATH + 'raw_customer_with_id.csv',dtype={"ID_NUMBER_ENCRYPTED__C":str}, iterator=True, chunksize=chunkrows) 
    isNewFile = True
    for chunk in df2: #for each 100k rows
        ids = df1["ID_NUMBER_ENCRYPTED__C"]
        result = chunk[chunk["ID_NUMBER_ENCRYPTED__C"].isin(ids)]

        if(len(result)>0 ):
            if( isNewFile ):
                result.to_csv(RESULT_PATH + result_file_name, index=None)
                isNewFile = False
            else :
                result.to_csv( RESULT_PATH + result_file_name, mode='a',header=None, index=None)

def _file_related_item_():
    related_item_file_name = "raw_opportunity.csv"
    duplicate_prospect_file_name = "merge_prospect_id.csv"

    df1 = pd.read_csv(INPUT_PATH + duplicate_prospect_file_name,dtype={"CUSTOMER_ID":str} )
    df2 = pd.read_csv(INPUT_PATH + related_item_file_name ,dtype={"CUSTOMER_ID":str})
    
    customer_ids = df1['CUSTOMER_ID']
    
    result = df2[df2["ACCOUNTID"].isin(customer_ids)]

    result.to_csv(RESULT_PATH+'existing_prospect_opportunity.csv', index=None)

def merge_related_item():
    related_item_file_name = "existing_prospect_opportunity.csv"
    merge_customer_file = "duplicate_customer_file.csv"
    
    df1 = pd.read_csv(INPUT_PATH+related_item_file_name,dtype="str")
    df2 = pd.read_csv(INPUT_PATH+merge_customer_file,dtype="str")

    result = pd.merge(df1,df2,on="PROSPECT_ID")
    
    result.to_csv(RESULT_PATH+'merge_opportunity_with_new_customer_id.csv',index=None)


if __name__ == '__main__':
    merge_related_item()

