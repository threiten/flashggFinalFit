#!/usr/bin/env python

import os
import numpy
import sys
import fnmatch
from copy import deepcopy as copy
import re

from optparse import OptionParser
from optparse import OptionGroup


from Queue import Queue

from threading import Thread, Semaphore
from multiprocessing import cpu_count
import subprocess

class Wrap:
    def __init__(self, func, args, queue):
        self.queue = queue
        self.func = func
        self.args = args
        
    def __call__(self):
        ret = self.func( *self.args )
        self.queue.put( ret  )

    
class Parallel:
    def __init__(self,ncpu):
        self.running = Queue(ncpu)
        self.returned = Queue()
        self.njobs = 0
  
    def run(self,cmd,args):
        wrap = Wrap( self, (cmd,args), self.returned )
        self.njobs += 1
        thread = Thread(None,wrap)
        thread.start()
        
    def __call__(self,cmd,args):
        if type(cmd) == str:
            print cmd
            for a in args:
                cmd += " %s " % a
            args = (cmd,)
            cmd = commands.getstatusoutput
        self.running.put((cmd,args))
        ret = cmd( *args ) 
        self.running.get()
        self.running.task_done()
        return ret

def getFilesFromDatacard(datacard):
    card = open(datacard,"r")
    files = set()
    for l in card.read().split("\n"):
        if l.startswith("shape"):
            toks = [t for t in l.split(" ") if t != "" ]
            files.add(toks[3])
    files = list(files)
    ret = files[0]
    for f in files[1:]:
        ret += ",%s" % f
    return ret

parser = OptionParser()
parser.add_option("-i","--infile",help="Signal Workspace")
parser.add_option("-q","--queue",default="espresso",help="Which batch queue")
parser.add_option("--runLocal",default=False,action="store_true",help="Run locally")
parser.add_option("--batch",default="HTCONDOR",help="Which batch system to use (HTCONDOR,IC)")
parser.add_option("--outDir",default=None)
parser.add_option("--procs",default=None)
parser.add_option("--flashggCats",default=None)
parser.add_option("--expected",type="int",default=None)
(opts,args) = parser.parse_args()

defaults = copy(opts)
print "INFO - queue ", opts.queue
def system(exec_line):
  #print "[INFO] defining exec_line"
  #if opts.verbose: print '\t', exec_line
  os.system(exec_line)


#def strtodict(lstr):
#  print "[INFO] string to dictionariy"
#  retdict = {}
#  if not len(lstr): return retdict
#  objects = lstr.split(':')
#  for o in objects:
#    k,vs = o.split('[')
#    vs = vs.rstrip(']')
#    vs = vs.split(',')
#    retdict[k] = [float(vs[0]),float(vs[1])]
#  return retdict
#
#catRanges = strtodict(opts.catRanges)

#def writePreamble(sub_file):
#  #print "[INFO] writing preamble"
#  sub_file.write('#!/bin/bash\n')
#  sub_file.write('touch %s.run\n'%os.path.abspath(sub_file.name))
#  sub_file.write('cd %s\n'%os.getcwd())
#  sub_file.write('eval `scramv1 runtime -sh`\n')
#  sub_file.write('cd -\n')
#  sub_file.write('number=$RANDOM\n')
#  sub_file.write('mkdir -p scratch_$number\n')
#  sub_file.write('cd scratch_$number\n')

def writePreamble(sub_file):
  #print "[INFO] writing preamble"
  sub_file.write('#!/bin/bash\n')
  if (opts.batch == "T3CH"):
      sub_file.write('set -x\n')
  sub_file.write('touch %s_${1}.run\n'%os.path.abspath(sub_file.name))
  sub_file.write('cd %s\n'%os.getcwd())
  if (opts.batch == "T3CH"):
      sub_file.write('source $VO_CMS_SW_DIR/cmsset_default.sh\n')
      sub_file.write('source /mnt/t3nfs01/data01/swshare/glite/external/etc/profile.d/grid-env.sh\n')
      sub_file.write('export SCRAM_ARCH=slc6_amd64_gcc481\n')
      sub_file.write('export LD_LIBRARY_PATH=/swshare/glite/d-cache/dcap/lib/:$LD_LIBRARY_PATH\n')
      sub_file.write('set +x\n') 
  sub_file.write('eval `scramv1 runtime -sh`\n')
  if (opts.batch == "T3CH"):
      sub_file.write('set -x\n') 
  sub_file.write('cd -\n')
  if (opts.batch == "T3CH" ) : sub_file.write('cd $TMPDIR\n')
  sub_file.write('number=$RANDOM\n')
  sub_file.write('mkdir -p scratch_$number\n')
  sub_file.write('cd scratch_$number\n')

def writePostamble(sub_file, exec_line, nJobs=None):

  #print "[INFO] writing to postamble"
  sub_file.write('%s\n'%exec_line)
  sub_file.write('retval=$?\n')
  #sub_file.write('\t mv higgsCombine*.root %s\n'%os.path.abspath(opts.outDir))
  sub_file.write('if [[ $retval == 0 ]]; then\n')
  sub_file.write('\t touch %s_${1}.done\n'%os.path.abspath(sub_file.name))
  sub_file.write('else\n')
  sub_file.write('\t touch %s_${1}.fail\n'%os.path.abspath(sub_file.name))
  sub_file.write('fi\n')
  sub_file.write('rm -f %s_${1}.run\n'%os.path.abspath(sub_file.name))
  sub_file.write('rm -rf scratch_$number\n')
  sub_file.write('exit $retval\n')
  sub_file.close()
  system('chmod +x %s'%os.path.abspath(sub_file.name))
  if opts.queue:
    system('rm -f %s*.done'%os.path.abspath(re.sub("\.sh","",os.path.abspath(sub_file.name))))
    system('rm -f %s*.fail'%os.path.abspath(re.sub("\.sh","",os.path.abspath(sub_file.name))))
    system('rm -f %s*.log'%os.path.abspath(re.sub("\.sh","",os.path.abspath(sub_file.name))))
    system('rm -f %s*.err'%os.path.abspath(re.sub("\.sh","",os.path.abspath(sub_file.name))))
    if (opts.batch == "IC"):
        system('qsub -q %s -o %s.log -e %s.err %s > out.txt'%(opts.queue,os.path.abspath(sub_file.name),os.path.abspath(sub_file.name),os.path.abspath(sub_file.name)))

    elif (opts.batch == "T3CH"):
        system('qsub -q %s -o %s.log -e %s.err %s > out.txt'%(opts.queue,os.path.abspath(sub_file.name),os.path.abspath(sub_file.name),os.path.abspath(sub_file.name)))

    elif(opts.batch == "HTCONDOR" ):
      sub_file_name = re.sub("\.sh","",os.path.abspath(sub_file.name))
      HTCondorSubfile = open("%s.sub"%sub_file_name,'w')
      HTCondorSubfile.write('+JobFlavour = "%s"\n'%(opts.queue))
      HTCondorSubfile.write('\n')
      HTCondorSubfile.write('executable  = %s.sh\n'%sub_file_name)
      HTCondorSubfile.write('output  = %s_$(ProcId).out\n'%sub_file_name)
      HTCondorSubfile.write('error  = %s_$(ProcId).err\n'%sub_file_name)
      HTCondorSubfile.write('log  = %s_$(ProcId).log\n'%sub_file_name)
      HTCondorSubfile.write('arguments = $(ProcId)\n')
      HTCondorSubfile.write('\n')
      HTCondorSubfile.write('max_retries = 1\n')
      HTCondorSubfile.write('queue {}\n'.format(nJobs))
      subP = subprocess.Popen("condor_submit "+HTCondorSubfile.name,
                             shell=True, # bufsize=bufsize,
                             # stdin=subprocess.PIPE,
                             # stdout=subprocess.PIPE,
                             # stderr=subprocess.PIPE,
                             close_fds=True)
      HTCondorSubfile.close()
      subP.wait()
      
  if opts.runLocal:
     system('bash %s'%os.path.abspath(sub_file.name))

#######################################

  
system('mkdir -p %s/fTestJobs/outputs'%opts.outDir)
counter=0
if opts.batch != "HTCONDOR":
    for proc in  opts.procs.split(","):
        for cat in opts.flashggCats.split(","):
            print "job ", counter , " - ", proc, " - ", cat
            file = open('%s/fTestJobs/sub%d.sh'%(opts.outDir,counter),'w')
            writePreamble(file)
            counter += 1
            exec_line = "%s/bin/signalFTest -i %s  -p %s -f %s --considerOnly %s -o %s/%s --datfilename %s/%s/fTestJobs/outputs/config_%d.dat" %(os.getcwd(), opts.infile,proc,opts.flashggCats,cat,os.getcwd(),opts.outDir,os.getcwd(),opts.outDir, counter)

            writePostamble(file,exec_line) #includes submission
            file.close()
            
elif opts.batch == "HTCONDOR":
    mapp = ''
    for proc in  opts.procs.split(","):
        for cat in opts.flashggCats.split(","):
            mapp += '{} '.format(proc)
            mapp += '{} '.format(cat)
            counter += 1

    jobFile = open('%s/fTestJobs/subCondor.sh'%(opts.outDir),'w')
    writePreamble(jobFile)
    jobFile.write('declare -a jobMap=({})\n'.format(mapp))
    jobFile.write('let "PROCID = 2*${1}"\n')
    jobFile.write('let "CATID = 2*${1} + 1"\n')
    exec_line = "%s/bin/signalFTest -i %s  -p ${jobMap[${PROCID}]} -f %s --considerOnly ${jobMap[${CATID}]} -o %s/%s --datfilename %s/%s/fTestJobs/outputs/config_${1}.dat" %(os.getcwd(), opts.infile,opts.flashggCats,os.getcwd(),opts.outDir,os.getcwd(),opts.outDir)
    writePostamble(jobFile, exec_line, counter)
    jobFile.close()
