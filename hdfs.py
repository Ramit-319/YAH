import functions
import argparse
import os
my_parser = argparse.ArgumentParser()
my_parser.add_argument('--mapper','-m', required=False)
my_parser.add_argument('--reducer','-r', required=False)
my_parser.add_argument('--config','-c', required=False)
my_parser.add_argument('--input','-i', required=False)
my_parser.add_argument('--output','-o', required=False)

my_parser.add_argument('-setup', required=False,nargs='*')
my_parser.add_argument('-cat', required=False)
my_parser.add_argument('-put', nargs=2, metavar=('source_file','destination'), required=False)
my_parser.add_argument('-mkdir', required=False)
my_parser.add_argument('-load', required=False, nargs='*')
my_parser.add_argument('-rmdir', required=False)
my_parser.add_argument('-ls', required=False,nargs='*')
my_parser.add_argument('-rm', required=False)

args = my_parser.parse_args()
mapper = args.mapper
reducer = args.reducer
config = args.config
inp = args.input
out = args.output
if mapper:
	 #changes - exe is always a text file and also the name of the output?
	 #os.remove('output.txt')
	 os.system('python3 hdfs.py -load '+config+' skip')
	 os.system('python3 hdfs.py -cat '+inp+' | python3 '+mapper+' | sort -k 1,1 | python3 '+reducer+' > output.txt')
	 loc = os.path.join(os.getcwd(),'output.txt')
	 os.system('python3 hdfs.py -put '+loc+' '+out)
	 os.remove('output.txt')
	
elif not args.setup == None:
	functions.configure(args.setup)
	
elif not args.load == None:
	functions.load_dfs(args.load) # change this later in the main file
	
elif args.put:
	functions.put(args.put[0],args.put[1])
	
elif args.mkdir:
	functions.mkdir(args.mkdir)
	
elif args.cat:
	functions.cat(args.cat)
	
elif args.rm:
	functions.rm(args.rm)

elif not args.ls == None:
	functions.ls(args.ls)
	
elif args.rmdir:
	functions.rmdir(args.rmdir)
	
	
