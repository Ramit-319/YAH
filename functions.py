#!/usr/bin/env python3
import json
import os
import sys
import shutil
import logging
import ast
import pickle


formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
def setup_logger(name, log_file, level=logging.DEBUG):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def get_config(file = '/home/pes2ug19cs319/yah/hay/Current_Config.json'):
	os.chdir(os.getcwd())
	with open(file,'r') as f:
		config = json.loads(f.read())
	return config


def configure(config):
	if config:
		f1 = open(config[0])
	else:
		f1 = open('Default.json')
	j = json.loads(f1.read())


	#Datanodes config
	dnum = j['num_datanodes']
	cwd = os.getcwd()
	dpath = j['path_to_datanodes']
	if not os.path.exists(dpath):
		os.makedirs(dpath)
		
		for i in range(0, dnum):
			os.mkdir(dpath + "/{}".format(i+1))
	else:
		print("DFS already exists")
		return

	#Namenode config
	npath = j['path_to_namenodes']
	if not os.path.exists(npath):
		os.makedirs(npath)
	open(os.path.join(npath,"namenode.txt"),'w')
	
	
	#datanode log file
	dlogpath = j['datanode_log_path']
	if not os.path.exists(dlogpath):
		os.makedirs(dlogpath)
	for i in range(0,dnum):
		logger = setup_logger('logger'+str(i+1), os.path.join(dlogpath,"logger{}.log".format(i+1)))
		logger.info("Datanode {} was created".format(i+1))
	
	#namenode log file
	nlogpath = j['namenode_log_path']
	if not os.path.exists(nlogpath):
		os.makedirs(nlogpath)
	open(os.path.join(nlogpath,"namenode_log.log"),'w')
	logger = setup_logger('namenode_log', os.path.join(nlogpath,"namenode_log.log"))
	logger.info("Namenode was created")
	datanode_num={}
	for i in range(0,dnum):
		datanode_num[i+1]=[0,[]]
	logger.debug("{}".format(datanode_num))
	
	#namenode checkpoints
	ncpoints = j['namenode_checkpoints']
	if not os.path.exists(ncpoints):
		os.makedirs(ncpoints)
	open(os.path.join(ncpoints,"namenode_checkpoints.txt"),'w')
	
	#filesystem
	fs_path = j['fs_path']
	if not os.path.exists(fs_path):
		os.makedirs(fs_path)
	
	#dfs configuration
	dfs_config = j['dfs_setup_config']
	if not os.path.exists(dfs_config):
		i = dfs_config.rindex('/')
		dfs_config_dir = dfs_config[:i]
		if not os.path.exists(dfs_config_dir):
			os.makedirs(dfs_config_dir)
		open(os.path.join(dfs_config_dir,dfs_config[i+1:]),'w')
	with open(dfs_config,'w') as f2:
		json.dump(j,f2)
	f1.close()
	print('Configuration Saved.')

def dict_equal(checkdict, dictionary, dnum):
	for i in range(1,dnum+1):
		if set(checkdict[i][1])!=set(dictionary[i][1]):
			return False
	return True

def load_dfs(file):
	if not file:
		if not os.path.exists('Current_Config.json'):
			print('No dfs load configuration given. No recently used dfs configuration available')
			return
		dfs_config_file = open('Current_Config.json')
	else:
		dfs_config_file = open(file[0])
	j = json.loads(dfs_config_file.read())
	
	with open('Current_Config.json','w') as f:
		json.dump(j,f)
	if len(file) == 2:
		return
	print('Loaded Configuration:')
	for x,y in j.items():
		print(x,y)
	
	#verification
	f = input('Format dfs? (yes/no) :')
	#flag = False
	if f.lower() != 'yes':
		dpath = j["path_to_datanodes"]
		ogpath = os.getcwd()
		dnum = j['num_datanodes']
		checkdict = {}
		for i in range(1,dnum+1):
			checkdict[i]=[0,[]]
		for i in range(1,dnum+1):
			os.chdir(os.path.join(dpath,"{}".format(i)))
			dirlen = len([name for name in os.listdir(".") if os.path.isfile(name)])
			filelist = os.listdir(".")
			checkdict[i][0]=dirlen
			for file in filelist:
				checkdict[i][1].append(file)
		os.chdir(ogpath)
		dictionary = get_status_datanodes()
		if dict_equal(checkdict,dictionary,dnum):
			print("Your DFS has been loaded")
			print('Starting namenodes...')
			os.system('python3 namenode.py & python3 Secondary_namenode.py &')
			return
		print("The datanode metadata given in the namenode is inconsistent, please format the dfs")
			
	#formating the datanodes
	folder = j['path_to_datanodes']
	for dirs in os.listdir(folder):
		fdr = os.path.join(folder,dirs)
		for filename in os.listdir(fdr):
			file_path = os.path.join(fdr, filename)
			try:
				if os.path.isfile(file_path) or os.path.islink(file_path):
				    os.unlink(file_path)
				elif os.path.isdir(file_path):
				    shutil.rmtree(file_path)
			except Exception as e:
				print('Failed to delete %s. Reason: %s' % (file_path, e))
				
	#formatting the namenode 
	with open(os.path.join(j['path_to_namenodes'],"namenode.txt"),'w') as _:
		pass
		
	#formatting file system
	fs_path = j['fs_path']
	for filename in os.listdir(fs_path):
		file_path = os.path.join(fs_path, filename)
		print(file_path)
		try:
			if os.path.isfile(file_path) or os.path.islink(file_path):
				os.unlink(file_path)
			elif os.path.isdir(file_path):
				shutil.rmtree(file_path)
			
		except Exception as e:
			print('Failed to delete %s. Reason: %s' % (file_path, e))
	
	#formatting datanode logs
	cwd = os.getcwd()
	dnum = j['num_datanodes']
	dlogpath = j['datanode_log_path']
	dlogs = os.listdir(dlogpath)
	os.chdir(dlogpath)
	for i in dlogs:
		os.remove(i)
	os.chdir(cwd)
	for i in range(0,dnum):
		logger = setup_logger('logger'+str(i+1), os.path.join(dlogpath,"logger{}.log".format(i+1)))
		logger.info("Datanode {} was created".format(i+1))
	
	nlogpath = j['namenode_log_path']
	open(os.path.join(nlogpath,"namenode_log.log"),'w')
	logger = setup_logger('namenode_log', os.path.join(nlogpath,"namenode_log.log"))
	logger.info("Namenode was created")
	datanode_num={}
	dnum = j['num_datanodes']
	for i in range(0,dnum):
		datanode_num[i+1]=[0,[]]
	logger.debug("{}".format(datanode_num))
	print('Starting namenodes...')
	os.system('python3 namenode.py & python3 Secondary_namenode.py &')
	print('Format complete')

	

def get_status_datanodes():
	config = get_config()
	nlogpath = config["namenode_log_path"]
	with open(os.path.join(nlogpath,"namenode_log.log"), 'r') as logfile:
		logfile = logfile.readlines()
	dictionary = {}
	logfile.reverse()
	for line in logfile:
		if "DEBUG" in line:
			dictionary = ast.literal_eval(line.split("DEBUG")[1].strip())
			break
	return dictionary
	
def get_datanodes_info():
	config = get_config()
	dnum = config['num_datanodes']
	dpath = config['path_to_datanodes']
	checkdict = {}
	for i in range(1,dnum+1):
		os.chdir(os.path.join(dpath,"{}".format(i)))
		dirlen = len([name for name in os.listdir(".") if os.path.isfile(name)])
		checkdict[i] = dirlen
	return checkdict
	
def hash_fucntion(filenum,rep):
	config = get_config()
	dnum = config['num_datanodes']
	dsize = config['datanode_size']
	checkdict = get_datanodes_info()
	h = (filenum + rep) % dnum 
	count = 1
	while checkdict[h+1] == dsize and count <= dnum:
		h = (h+1) % dnum 
		count += 1
	if count > dnum:
		return False
	return (h + 1)
	

def put(source,dest):
	config = get_config()
	nlogpath = config["namenode_log_path"]
	dlogpath = config["datanode_log_path"]
	file_number = 1
	file_name,exe = os.path.splitext(os.path.basename(source))
	fs_path = config['fs_path']
	destination = os.path.join(fs_path,dest)
	
	
	dictionary = get_status_datanodes()
	
	if not os.path.exists(destination):
		print('Path to file system does not exist')
		return 
	
	os.chdir(destination)
	with open(file_name+exe,'w') as _:
		pass
	try:
		with open(os.path.join(config['path_to_namenodes'],'namenode.txt'),'rb') as nfile:
			temp = pickle.load(nfile)
	except:
		temp = []
			
	with open(source) as input_file:
		chunk = input_file.read(config['block_size'])
		mappings = []
		while chunk:
			reps = []
			for i in range(config['replication_factor']):
				hashvalue = hash_fucntion(file_number,i)
				if not hashvalue:
					print('All datanodes full. Aborting')
				cfilename = config['path_to_datanodes'] + '/' + str(hashvalue)
				os.chdir(cfilename)
				with open(file_name + str(file_number) + exe,'w') as chunk_file:
					chunk_file.write(chunk)
					
				logger = setup_logger('logger'+str(hashvalue), os.path.join(dlogpath,"logger{}.log".format(str(hashvalue))))
				logger.info("Datanode {} got a new block".format(str(hashvalue)))
				dictionary[hashvalue][0]+=1
				dictionary[hashvalue][1].append(file_name + str(file_number) + exe)
				
				reps.append(os.path.join(cfilename,file_name) + str(file_number) + exe)
			mappings.append(reps)
			file_number += 1
			chunk = input_file.read(config['block_size'])	
	temp.append([file_name+exe,dest,mappings])
	with open(os.path.join(config['path_to_namenodes'],'namenode.txt'),'wb') as nfile:
		pickle.dump(temp,nfile)
		
	logger = setup_logger('namenode_log', os.path.join(nlogpath,"namenode_log.log"))
	logger.info("Datanode block sizes have changed")
	logger.debug("{}".format(dictionary))

def mkdir(dir_name):
	config = get_config()
	fs_path = config['fs_path']
	os.chdir(fs_path)
	if os.path.exists(dir_name):
		print('Directory exists')
		return 
	os.makedirs(dir_name)

def cat(file):
	config = get_config()
	directory, filename = os.path.split(file)
	with open(os.path.join(config['path_to_namenodes'],'namenode.txt'),'rb') as nfile:
		temp = pickle.load(nfile)
	for files in temp:
		if files[0] == filename and files[1][:-1]== directory:
			for blocks in files[2]:
				flag = False
				for reps in blocks:
					if os.path.exists(reps):
						flag = True
						break
				if not flag:
					print('Block is missing')
					return
				with open(reps,'r') as f:
					print(f.read(),end='')
			return
	print('File not found')			
	
def ls(dir_name):
	dirname = ''
	if dir_name:
		dirname = dir_name[0]
	config = get_config()
	fs_path = config['fs_path']
	path = os.path.join(fs_path,dirname)
	
	if os.path.exists(path) == False:
		print('No such directory exists')
		return 
	else:
		dir = os.listdir(path)
		for i in dir:
			print(i)


def rm(file):
	try:
		config = get_config()
		nlogpath = config["namenode_log_path"]
		dlogpath = config["datanode_log_path"]
		directory, filename = os.path.split(file)
		os.remove(os.path.join(config['fs_path'],file))
		dictionary = get_status_datanodes()
		with open(os.path.join(config['path_to_namenodes'],'namenode.txt'),'rb') as nfile:
			temp = pickle.load(nfile)
		for files in temp:
			if files[0] == filename and files[1][:-1] == directory:
				for blocks in files[2]:
					for reps in blocks:
						dnodenum = int(os.path.split(reps)[-2][-1])
						dnodename = os.path.split(reps)[-1]
						os.remove(reps)
						logger = setup_logger('logger'+str(dnodenum), os.path.join(dlogpath,"logger{}.log".format(dnodenum)))
						logger.info("Deleted a block from Datanode {}".format(dnodenum))
						dictionary[dnodenum][0]-=1
						dictionary[dnodenum][1].remove(dnodename)
				logger = setup_logger('namenode_log', os.path.join(nlogpath,"namenode_log.log"))
				logger.info("Datanode blocks were deleted.")
				logger.debug("{}".format(dictionary))
				temp.remove(files)
				with open(os.path.join(config['path_to_namenodes'],'namenode.txt'),'wb') as nfile:
					pickle.dump(temp,nfile) 
				return
		
	except OSError as error:
		print(error)
		print("File '% s' cannot be removed" % filename)
	print('No such file found')

def rmdir(dir_name):
	config = get_config()
	fs_path = config['fs_path']
	os.chdir(fs_path)
	try:
    		os.rmdir(dir_name)
    		
    		print("Directory '% s' has been removed successfully" % dir_name)
	except OSError as error:
		print(error)
		print("Directory '% s' can not be removed" % dir_name)	
