#!/usr/bin/env python3
import json
import os
import sys
import shutil
import functions
import threading
import time
import pickle

config = functions.get_config()
sync_per = config["sync_period"]
#sync_per = 20
#fs_path = config['fs_path']
#path = os.path.join(fs_path,'namenode')
#print(path)

def copy_content():
	while True:
		try: 
			with open(config["path_to_namenodes"] + '/' + 'namenode.txt','rb') as nfile, open(config["namenode_checkpoints"] + '/' + 'namenode_checkpoints.txt','wb') as cfile:
		#read content from first file
				temp = pickle.load(nfile)
				pickle.dump(temp,cfile)
			time.sleep(sync_per)
		except:
			pass
	
def check_nn():
	while True:
		try:
			if (os.path.exists(config["path_to_namenodes"] + '/' + 'namenode.txt')) == False:
				#print("hello")
				with open(config["path_to_namenodes"] + '/' + 'namenode.txt','wb') as nfile, open(config["namenode_checkpoints"] + '/' + 'namenode_checkpoints.txt','rb') as cfile:
			#read content from first file
					temp = pickle.load(cfile)
					pickle.dump(temp,nfile)
			time.sleep(1)
		except:
			break

        
threading.Thread(target=copy_content).start() # Task_One()
threading.Thread(target=check_nn).start() # Task_Two()

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
