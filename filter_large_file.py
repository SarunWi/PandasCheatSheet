#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import sys
import glob,os

INPUT_PATH 	= 'input\\'
RAW_CUSTOMER_PATH = 'raw_customer_file\\'
RESULT_PATH = 'output\\'

def _file_all_file_name_(path):
    return os.listdir(INPUT_PATH+path)

def _get_investment_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_investment_account.csv'
    sub_path = 'raw_investment_file\\'
    cif_file = 'match_investment_account_owner.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype="str",delimiter="|" )
    externalKeySet = df1["INVESTMENT_ACCOUNT_KEY_REF"]
    for fileName in _file_all_file_name_(sub_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+sub_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["INVESTMENT_ACCOUNT_KEY_REF"].isin(externalKeySet)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_investment_account_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_investment_account.csv'
    sub_path = 'raw_investment_account_file\\'
    cif_file = 'match_investment_account_owner.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype="str",delimiter="|" )
    externalKeySet = df1["INVESTMENT_ACCOUNT_KEY_REF"]
    for fileName in _file_all_file_name_(sub_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+sub_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["INVESTMENT_ACCOUNT_KEY_REF"].isin(externalKeySet)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_investment_account_owner_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_investment_account_owner.csv'
    investment_account_owner_path = 'raw_investment_account_owner_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(investment_account_owner_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+investment_account_owner_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CIF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_td_item_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_td_item.csv'
    sub_path = 'raw_td_item_file\\'
    cif_file = 'match_deposit_owner.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype="str",delimiter="|" )
    externalKeySet = df1["DEPOSIT_KEY_REF"]
    for fileName in _file_all_file_name_(sub_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+sub_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["DEPOSIT_KEY_REF"].isin(externalKeySet)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_deposit_atm_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_deposit_atm.csv'
    sub_path = 'raw_deposit_atm_file\\'
    cif_file = 'match_deposit_owner.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype="str",delimiter="|" )
    externalKeySet = df1["DEPOSIT_KEY_REF"]
    for fileName in _file_all_file_name_(sub_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+sub_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["DEPOSIT_KEY_REF"].isin(externalKeySet)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_atm_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_atm.csv'
    sub_path = 'raw_atm_file\\'
    cif_file = 'match_deposit_atm.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype="str",delimiter="|" )
    externalKeySet = df1["ATM_KEY_REF"]
    for fileName in _file_all_file_name_(sub_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+sub_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["ATM_KEY_REF"].isin(externalKeySet)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_deposit_detail_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_deposit_detail.csv'
    sub_path = 'raw_deposit_detail_file\\'
    cif_file = 'match_deposit_owner.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype="str",delimiter="|" )
    externalKeySet = df1["DEPOSIT_KEY_REF"]
    for fileName in _file_all_file_name_(sub_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+sub_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["DEPOSIT_KEY_REF"].isin(externalKeySet)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_deposit_owner_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_deposit_owner.csv'
    investment_account_owner_path = 'raw_deposit_owner_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(investment_account_owner_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+investment_account_owner_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CIF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_customer_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_cif.csv'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(RAW_CUSTOMER_PATH):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+RAW_CUSTOMER_PATH+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CIF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_bancassurance_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_bancassurance.csv'
    raw_path = 'raw_bancassurance_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CIF"].isin(maskedCIF)]
            
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_cash_management_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_cash_management.csv'
    raw_path = 'raw_cash_management_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CIF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_edc_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_edc.csv'
    raw_path = 'raw_edc_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CIF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_fcd_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_fcd.csv'
    raw_path = 'raw_fcd_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CIF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_shareholder_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_shareholder.csv'
    raw_path = 'raw_shareholder_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CUSTOMER"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_director_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_director.csv'
    raw_path = 'raw_director_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CUSTOMER"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_loan_owner_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_loan_owner.csv'
    raw_path = 'raw_loan_owner_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} )
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CIF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_loan_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_loan.csv'
    raw_path = 'raw_loan_file\\'
    cif_file = 'match_loan_owner.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"LOAN_KEY_REF":str} ,delimiter="|")
    maskedCIF = df1["LOAN_KEY_REF"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["LOAN_KEY_REF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_pn_ticket_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_pn_ticket.csv'
    raw_path = 'raw_pn_ticket_file\\'
    cif_file = 'match_loan_owner.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"LOAN_KEY_REF":str} ,delimiter="|")
    maskedCIF = df1["LOAN_KEY_REF"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["LOAN_KEY_REF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_pledge_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_pledge.csv'
    raw_path = 'raw_pledge_file\\'
    cif_file = 'criteria_cif.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CIF__c":str} ,delimiter="|")
    maskedCIF = df1["CIF__c"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CIF"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_pledge_header_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_pledge_header.csv'
    raw_path = 'raw_pledge_header_file\\'
    cif_file = 'match_pledge.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"PID":str} ,delimiter="|")
    maskedCIF = df1["PID"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["PID"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_collateral_pid_cid_aid_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_collateral_pid_cid_aid.csv'
    raw_path = 'raw_collateral_pid_cid_aid_file\\'
    cif_file = 'match_pledge.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"PID":str} ,delimiter="|")
    maskedCIF = df1["PID"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["PID"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_collateral_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_collateral.csv'
    raw_path = 'raw_collateral_file\\'
    cif_file = 'match_collateral_pid_cid_aid.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"CID":str} ,delimiter="|")
    maskedCIF = df1["CID"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["CID"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def _get_appraisal_file_():
    chunkrows   = 100000
    isNewFile = True
    result_file_name = 'match_appraisal.csv'
    raw_path = 'raw_appraisal_file\\'
    cif_file = 'match_collateral_pid_cid_aid.csv'
    df1 = pd.read_csv(INPUT_PATH+ cif_file,dtype={"AID":str} ,delimiter="|")
    maskedCIF = df1["AID"]
    for fileName in _file_all_file_name_(raw_path):
        print(fileName)
        df2 = pd.read_csv(INPUT_PATH+raw_path+fileName,dtype="str",delimiter="|", iterator=True, chunksize=chunkrows )
        for chunk in df2: 
            result = chunk[chunk["AID"].isin(maskedCIF)]
            if(len(result)>0 ):
                if( isNewFile ):
                    result.to_csv(RESULT_PATH + result_file_name,sep="|", index=None)
                    isNewFile = False
                else :
                    result.to_csv( RESULT_PATH + result_file_name,sep="|", mode='a',header=None, index=None)

def main():
    _get_customer_file_()

        

main()