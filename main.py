#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

INPUT_PATH 	= 'input\\'
RESULT_PATH = 'output\\'

left_file_name  = 'example1.csv'
right_file_name = 'example2.csv'

def main():
    _merge_2_file_()


# scenario when we have 2 csv file 
# merge them and keep only record that found both file 
def _merge_2_file_():
    external_key_column = 'External_Key'
    result_file_name    = 'merge_example_1_2.csv'
    # Open left file
    df1 = pd.read_csv(INPUT_PATH+left_file_name,dtype={external_key_column:str})
    # Open right file
    df2 = pd.read_csv(INPUT_PATH+right_file_name,dtype={external_key_column:str})
	# merge them with external_key_column  
    result = pd.merge(df1,df2,on=external_key_column)
    # write to new result file 
    result.to_csv(RESULT_PATH+result_file_name)

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

if __name__ == '__main__':
    main()

