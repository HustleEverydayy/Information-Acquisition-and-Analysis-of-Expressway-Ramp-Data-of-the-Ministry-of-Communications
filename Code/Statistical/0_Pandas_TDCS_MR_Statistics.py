# -*- coding: utf-8 -*-
"""
Created on Mon Oct  5 12:38:52 2020

@author: jdwang
"""


SrcDir = "../SRC/"
SrcFile = 'part-r-00000_TDCS_201611_201910_03F1779S_03F2129S_G8_Key_TotalTime_TimeIntervals_TFCFLength_ClassFrequency_1_0'
SrcLocation = SrcDir +SrcFile

'''
file_input = open (SrcLocation)
data = file_input.read()
file_input.close()
'''
with open(SrcLocation) as file_input:
    data = file_input.readlines()
    
for line in data:   
    print (line)
    
