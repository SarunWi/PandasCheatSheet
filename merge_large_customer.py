#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import sys

INPUT_PATH 	= 'input\\'
RESULT_PATH = 'output\\'

left_file_name      = 'raw_prospect_with_phone_info.csv'
right_file_name     = 'raw_customer_with_phone_info.csv'

def _filter_blank_Id_():
    df1 = pd.read_csv(INPUT_PATH+ right_file_name  ,dtype="str",iterator=True, chunksize=100000)
    isNewFile = True
    for chunk in df1:
        chunk = chunk.dropna(subset=["ID_NUMBER_ENCRYPTED__C"])
        if isNewFile :
            chunk.to_csv(RESULT_PATH + right_file_name, index=None)
            isNewFile = False
        else :
            chunk.to_csv(RESULT_PATH + right_file_name, mode='a',header=None, index=None)

def _find_merge_file_():
    chunckNo = 1
    chunkrows = 100000
    isNewFile = True
    result_only_prospect_file_name  = 'duplicate_prospect_file.csv'
    result_merge_customer_file_name = 'duplicate_customer_file.csv'
    df1 = pd.read_csv(INPUT_PATH+ left_file_name  ,dtype="str")
    print(len(df1))
    df2 = pd.read_csv(INPUT_PATH+ right_file_name ,dtype="str", iterator=True, chunksize=chunkrows )
    for chunk in df2:
        print("chunck No :"+str(chunckNo) )
        chunckNo = chunckNo +1
        customer_id = chunk['ID_NUMBER_ENCRYPTED__C']
        duplicateProspect = df1[df1["ID_NUMBER_ENCRYPTED__C"].isin(customer_id)]
        merge_customer = pd.merge(df1,chunk,on="ID_NUMBER_ENCRYPTED__C")
        if(len(merge_customer)>0):
            if( isNewFile ):
                duplicateProspect.to_csv(RESULT_PATH + result_only_prospect_file_name, index=None)
                merge_customer.to_csv(RESULT_PATH + result_merge_customer_file_name, index=None)
                isNewFile = False
            else :
                duplicateProspect.to_csv( RESULT_PATH + result_only_prospect_file_name, mode='a',header=None, index=None)
                merge_customer.to_csv( RESULT_PATH + result_merge_customer_file_name, mode='a',header=None, index=None)

_find_merge_file_()