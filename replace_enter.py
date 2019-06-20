#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import date
import sys
import os

INPUT_PATH 	= 'in/'
RESULT_PATH = 'out/'


def replace_file():
	for csvFile in os.listdir(INPUT_PATH):
		df = pd.read_csv(INPUT_PATH + csvFile , dtype='str')
		columns = df.columns.values
		print(df.columns.values)
		for column in columns:
			df[column] = df[column].replace('\r','')
			df[column] = df[column].replace('\n','')
		df.to_csv( RESULT_PATH+csvFile.replace('.csv','.dat') ,mode="w", encoding="cp874")
		create_ctl(csvFile,len(df))

def create_ctl(file_name,file_size):
	ctl_data = ''
	ctl_data = ctl_data+_get_file_time_stamp(file_name)+_get_start_time()+_get_end_time()+_get_file_qualify()+_get_file_size(file_size)
	f = open(RESULT_PATH+file_name.replace('.csv','.ctl'),"w+",encoding="utf-8" )
	f.write(ctl_data)
	f.close()

def _get_file_time_stamp(filePath):
	dt_timestamp = datetime.fromtimestamp(os.path.getmtime(INPUT_PATH +filePath))
	return str(dt_timestamp)
def _get_start_time():
	return str(date.today())+" 00:00:00.000000"
def _get_end_time():
	return str(date.today())+" 23:59:59.999999"
def _get_file_qualify():
	return "0000000001"
def _get_file_size(file_size):
	return str(file_size).zfill(10)

replace_file()