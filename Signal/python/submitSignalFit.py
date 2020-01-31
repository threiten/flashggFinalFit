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
parser.add_option("-d","--datfile",help="dat file")
parser.add_option("-s","--systdatfile",help="systematics dat file")
parser.add_option("--mhLow",default="120",help="mh Low")
parser.add_option("--mhHigh",default="130",help="mh High")
parser.add_option("-q","--queue",default="espresso",help="Which batch queue")
parser.add_option("--runLocal",default=False,action="store_true",help="Run locally")
parser.add_option("--batch",default="HTCONDOR",help="Which batch system to use (HTCONDOR,IC)")
parser.add_option("--changeIntLumi",default="1.")
parser.add_option("-o","--outfilename",default=None)
parser.add_option("-p","--outDir",default="./")
parser.add_option("--procs",default=None)
parser.add_option("-f","--flashggCats",default=None)
parser.add_option("--bs",default=5.14)
parser.add_option("--shiftOffDiag",default=False,action="store_true")
parser.add_option("--noSkip",default=False,action="store_true")
parser.add_option("--MHref",default=None)
parser.add_option("--expected",type="int",default=None)
parser.add_option("--refProc",default="",help="ref replacement process")
parser.add_option("--refProcDiff",default="",help="ref replacement process for differentials")
parser.add_option("--refTagDiff",default="",help="ref replacement tag for differentials")
parser.add_option("--refTagWV",default="",help="ref replacement tag for WV")
parser.add_option("--refProcWV",default="",help="ref replacement proc for WV")
parser.add_option("--normalisationCut",default="",help="cut on datasets for final normalisation")
parser.add_option("--skipSecondaryModels",default=False,action="store_true",help="Turn off creation of all additional models")
(opts,args) = parser.parse_args()

defaults = copy(opts)
print "INFO - queue ", opts.queue
def system(exec_line):
  #print "[INFO] defining exec_line"
  #if opts.verbose: print '\t', exec_line
  os.system(exec_line)

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
  if (opts.batch == "T3CH" ) : 
      sub_file.write('cd $TMPDIR\n')
  sub_file.write('number=$RANDOM\n')
  sub_file.write('mkdir -p scratch_$number\n')
  sub_file.write('cd scratch_$number\n')

def writePostamble(sub_file, exec_line, nJobs=None):

  #print "[INFO] writing to postamble"
  sub_file.write('echo "PREPARING TO RUN "\n')
  sub_file.write('%s\n'%exec_line)
  sub_file.write('retval=$?\n')
  #sub_file.write('\t mv higgsCombine*.root %s\n'%os.path.abspath(opts.outDir))
  sub_file.write('if [[ $retval == 0 ]]; then\n')
  #sub_file.write('\t mv higgsCombine*.root %s\n'%os.path.abspath(opts.outDir))
  sub_file.write('\t echo "DONE" \n')
  sub_file.write('\t touch %s_${1}.done\n'%os.path.abspath(sub_file.name))
  sub_file.write('else\n')
  sub_file.write('\t echo "FAIL" \n')
  sub_file.write('\t touch %s_${1}.fail\n'%os.path.abspath(sub_file.name))
  sub_file.write('fi\n')
  sub_file.write('cd -\n')
  sub_file.write('\t echo "RM RUN "\n')
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

    if (opts.batch == "LSF"):
        system('bsub -q %s -o %s.log %s'%(opts.queue,os.path.abspath(sub_file.name),os.path.abspath(sub_file.name)))
    elif (opts.batch == "T3CH"): 
        system('qsub -q %s -o %s.log -e %s.err %s'%(opts.queue,os.path.abspath(sub_file.name),os.path.abspath(sub_file.name),os.path.abspath(sub_file.name)))
    elif (opts.batch == "IC"): 
        system('qsub -q %s -o %s.log -e %s.err %s'%(opts.queue,os.path.abspath(sub_file.name),os.path.abspath(sub_file.name),os.path.abspath(sub_file.name)))
      #print "system(",'qsub -q %s -o %s.log -e %s.err %s '%(opts.queue,os.path.abspath(sub_file.name),os.path.abspath(sub_file.name),os.path.abspath(sub_file.name)),")"
    elif (opts.batch == "IC"): 
        system('qsub -q %s -l h_rt=0:20:0 -o %s.log -e %s.err %s'%(opts.queue,os.path.abspath(sub_file.name),os.path.abspath(sub_file.name),os.path.abspath(sub_file.name)))
    elif( opts.batch == "HTCONDOR" ):
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

  
system('mkdir -p %s/SignalFitJobs/outputs'%opts.outDir)
print ('mkdir -p %s/SignalFitJobs/outputs'%opts.outDir)
counter=0
mhrefopt = "--MHref %s"  %opts.MHref if opts.MHref else ""
offdiagopt = ""
noskipopt = ""
refProcOpt = ""
refProcDiffOpt = ""
refTagDiffOpt = ""
refTagWVOpt = ""
refProcWVOpt = ""
normCutOpt = ""
skipSecModOpt = ""

if opts.shiftOffDiag:
    offdiagopt = " --shiftOffDiag 1"
if opts.noSkip:
    noskipopt = " --noSkip 1"
if opts.refProc:
    refProcOpt = " --refProc "+str(opts.refProc)
if opts.refProcDiff:
    refProcDiffOpt = " --refProcDiff "+str(opts.refProcDiff)
if opts.refTagDiff:
    refTagDiffOpt = " --refTagDiff "+str(opts.refTagDiff)
if opts.refTagWV:
    refTagWVOpt = " --refTagWV "+str(opts.refTagWV)
if opts.refProcWV:
    refProcWVOpt = " --refProcWV "+str(opts.refProcWV)
if opts.normalisationCut:
    normCutOpt = " --normalisationCut "+str(opts.normalisationCut)
if opts.skipSecondaryModels:
    skipSecModOpt = " --skipSecondaryModels "

if opts.batch != "HTCONDOR":
    for proc in  opts.procs.split(","):
        for cat in opts.flashggCats.split(","):
            print "job ", counter , " - ", proc, " - ", cat
            file = open('%s/SignalFitJobs/sub%d.sh'%(opts.outDir,counter),'w')
            writePreamble(file)
            counter =  counter+1
            bsRW=0
            if (float(opts.bs)==0):
                bsRW=0
            else:
                bsRW=1
            if opts.systdatfile:
                exec_line = "%s/bin/SignalFit --verbose 3 -i %s -d %s/%s  --mhLow=%s --mhHigh=%s -s %s/%s --procs %s -o  %s/%s -p %s/%s -f %s --changeIntLumi %s --binnedFit 1 --nBins 320 --split %s,%s --beamSpotReweigh %d --dataBeamSpotWidth %f %s %s %s %s %s %s %s %s %s %s" %(os.getcwd(), opts.infile,os.getcwd(),opts.datfile,opts.mhLow, opts.mhHigh, os.getcwd(),opts.systdatfile, opts.procs,os.getcwd(),opts.outfilename.replace(".root","_%s_%s.root"%(proc,cat)), os.getcwd(),opts.outDir, opts.flashggCats ,opts.changeIntLumi, proc,cat,bsRW,float(opts.bs), mhrefopt, offdiagopt,noskipopt,refProcOpt,refProcDiffOpt,refTagDiffOpt,refTagWVOpt,refProcWVOpt,normCutOpt,skipSecModOpt)
            else:
                exec_line = "%s/bin/SignalFit --verbose 3 -i %s -d %s/%s  --mhLow=%s --mhHigh=%s  --procs %s -o  %s/%s -p %s/%s -f %s --changeIntLumi %s --binnedFit 1 --nBins 320 --split %s,%s --beamSpotReweigh %d --dataBeamSpotWidth %f %s %s %s %s %s %s %s %s %s %s" %(os.getcwd(), opts.infile,os.getcwd(),opts.datfile,opts.mhLow, opts.mhHigh, opts.procs,os.getcwd(),opts.outfilename.replace(".root","_%s_%s.root"%(proc,cat)), os.getcwd(),opts.outDir, opts.flashggCats ,opts.changeIntLumi, proc,cat,bsRW,float(opts.bs), mhrefopt, offdiagopt, noskipopt, refProcOpt,refProcDiffOpt,refTagDiffOpt,refTagWVOpt,refProcWVOpt,normCutOpt, skipSecModOpt)
                #exec_line = "%s/bin/SignalFit -i %s  -p %s -f %s --considerOnly %s -o %s/%s --datfilename %s/%s/SignalFitJobs/outputs/config_%d.dat" %(os.getcwd(), opts.infile,proc,opts.flashggCats,cat,os.getcwd(),opts.outDir,os.getcwd(),opts.outDir, counter)
                #print exec_line
            writePostamble(file,exec_line) #includes submission

elif opts.batch == "HTCONDOR":
    mapp = ''
    for proc in  opts.procs.split(","):
        for cat in opts.flashggCats.split(","):
            mapp += '{} '.format(proc)
            mapp += '{} '.format(cat)
            counter += 1

    jobFile = open('%s/SignalFitJobs/subCondor.sh'%(opts.outDir),'w')
    writePreamble(jobFile)
    jobFile.write('declare -a jobMap=({})\n'.format(mapp))
    jobFile.write('let "PROCID = 2*${1}"\n')
    jobFile.write('let "CATID = 2*${1} + 1"\n')
    bsRW=0
    if (float(opts.bs)==0):
        bsRW=0
    else:
        bsRW=1
    if opts.systdatfile:
        exec_line = "%s/bin/SignalFit --verbose 3 -i %s -d %s/%s  --mhLow=%s --mhHigh=%s -s %s/%s --procs %s -o  %s/%s_${jobMap[${PROCID}]}_${jobMap[${CATID}]}.root -p %s/%s -f %s --changeIntLumi %s --binnedFit 1 --nBins 320 --split ${jobMap[${PROCID}]},${jobMap[${CATID}]} --beamSpotReweigh %d --dataBeamSpotWidth %f %s %s %s %s %s %s %s %s %s %s" %(os.getcwd(), opts.infile,os.getcwd(),opts.datfile,opts.mhLow, opts.mhHigh, os.getcwd(),opts.systdatfile, opts.procs,os.getcwd(),opts.outfilename.replace(".root",""), os.getcwd(),opts.outDir, opts.flashggCats ,opts.changeIntLumi,bsRW,float(opts.bs), mhrefopt, offdiagopt,noskipopt,refProcOpt,refProcDiffOpt,refTagDiffOpt,refTagWVOpt,refProcWVOpt,normCutOpt,skipSecModOpt)
    else:
        exec_line = "%s/bin/SignalFit --verbose 3 -i %s -d %s/%s  --mhLow=%s --mhHigh=%s  --procs %s -o %s/%s_${jobMap[${PROCID}]}_${jobMap[${CATID}]}.root -p %s/%s -f %s --changeIntLumi %s --binnedFit 1 --nBins 320 --split ${jobMap[${PROCID}]},${jobMap[${CATID}]} --beamSpotReweigh %d --dataBeamSpotWidth %f %s %s %s %s %s %s %s %s %s %s" %(os.getcwd(), opts.infile,os.getcwd(),opts.datfile,opts.mhLow, opts.mhHigh, opts.procs,os.getcwd(),opts.outfilename.replace(".root",""), os.getcwd(),opts.outDir, opts.flashggCats ,opts.changeIntLumi,bsRW,float(opts.bs), mhrefopt, offdiagopt, noskipopt, refProcOpt,refProcDiffOpt,refTagDiffOpt,refTagWVOpt,refProcWVOpt,normCutOpt, skipSecModOpt)
        #exec_line = "%s/bin/SignalFit -i %s  -p %s -f %s --considerOnly %s -o %s/%s --datfilename %s/%s/SignalFitJobs/outputs/config_%d.dat" %(os.getcwd(), opts.infile,proc,opts.flashggCats,cat,os.getcwd(),opts.outDir,os.getcwd(),opts.outDir, counter)
        #print exec_line
    writePostamble(jobFile, exec_line, counter) #includes submission
    jobFile.close()



