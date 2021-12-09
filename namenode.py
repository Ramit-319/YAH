#!/usr/bin/env python3
import json
import os
import sys
import shutil
import logging
import ast
import functions
import threading
import time

config = functions.get_config()
dpath = config["path_to_datanodes"]
dnum = config["num_datanodes"]
syncp = config["sync_period"]

def replicate(deleted_files,cwd):
	for i in deleted_files:
		for j in range(1,dnum+1):
			os.chdir(os.path.join(dpath,"{}".format(j)))
			filelist = os.listdir(".")
			if i in filelist:
				with open(os.path.join(os.getcwd(),i),'r') as afile, open(os.path.join(cwd,i),'w') as dfile:
		#read content from first file
					for line in afile:
			#append content to second file
						dfile.write(line)
	
						
	

def check():
	while True:
		try:
			curr_dir = os.getcwd()
			for i in range(1,dnum+1):
				os.chdir(os.path.join(dpath,"{}".format(i)))
				filelist = os.listdir(".")
				curr_dict = functions.get_status_datanodes()
				curr_list = curr_dict[i][1]
				if set(curr_list) != set(filelist):
					deleted_files = set(curr_list) - set(filelist)
					#print(deleted_files)
					replicate(deleted_files,os.getcwd())
				os.chdir(curr_dir)	
			time.sleep(syncp)
		except:
			break
				
			
threading.Thread(target=check).start()
