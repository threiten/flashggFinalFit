#!/usr/bin/env python

# Script adapted from original by Matt Kenzie.
# Used for Dry Run of Dec 2015 Hgg analysis.

###############################################################################
## IMPORTS ####################################################################
###############################################################################
import os,sys,copy,math
import root_numpy
import pandas as pd
import numpy as np
import pickle as pkl
###############################################################################

###############################################################################
## PARSE ROOT MACROS  #########################################################
###############################################################################
import ROOT as r
#if options.quadInterpolate:
#  r.gROOT.ProcessLine(".L quadInterpolate.C+g")
#  from ROOT import quadInterpolate
print "ROOT imported as r"
#r.gROOT.ProcessLine(".L $CMSSW_BASE/lib/$SCRAM_ARCH/libHiggsAnalysisCombinedLimit.so")
#print "combine libraries imported"
#r.gROOT.ProcessLine(".L ../libLoopAll.so")
###############################################################################

###############################################################################
## WSTFileWrapper  ############################################################
###############################################################################

class WSTFileWrapper:
   #self.fnList = [] # filename list
   #self.fileList = [] #file list
   #self.wsList = [] #workspace list
   
   def Print(self):
      for f in self.fileList:
         print f.GetName()
      for ws in self.wsList:
         ws.Print()

   def __init__(self, files,wsname):
    self.fnList = files # [1]       
    self.fileList = []
    self.wsList = [] #now list of ws names...
    #print files
    for fn in self.fnList: # [2]
        f = r.TFile.Open(fn) 
        self.fileList.append(f)
        thing = f.Get(wsname)
        thing.Print()
        self.wsList.append(self.fileList[-1].Get(wsname))
        f.Close()

   def data(self,dataName):
        result = None
        complained_yet = False
        for i in range(len(self.fnList)):
          this_result_obj = self.wsList[i].data(dataName);
          if ( result and this_result_obj and (not complained_yet) ):
            complained_yet = True
          if this_result_obj: # [3]
             result = this_result_obj
        return result 
   
   def var(self,varName):
        result = None
        complained_yet =0 
        for i in range(len(self.fnList)):
          this_result_obj = self.wsList[i].var(varName);
          if this_result_obj: # [3]
             result = this_result_obj
                
        return result 


###############################################################################


###############################################################################
## OPTION PARSING  ############################################################
###############################################################################
# from optparse import OptionParser
from argparse import ArgumentParser
# parser = OptionParser()
parser = ArgumentParser()
parser.add_argument("-i","--infilename", nargs='+', help="Input file (binned signal from flashgg)")
parser.add_argument("--rateFiles", nargs='+', help="Files for rescaling prediction btw years")
parser.add_argument("-o","--outfilename",default="cms_hgg_datacard.txt",help="Name of card to print (default: %default)")
parser.add_argument("-p","--procs",default="ggh,vbf,wh,zh,tth",help="String list of procs (default: %default)")
parser.add_argument("-c","--cats",default="UntaggedTag_0,UntaggedTag_1,UntaggedTag_2,UntaggedTag_3,UntaggedTag_4,VBFTag_0,VBFTag_1,VBFTag_2",help="Flashgg Categories (default: %default)")
parser.add_argument("--photonCatScales",default="HighR9EE,LowR9EE,HighR9EB,LowR9EB,Gain1EB,Gain6EB",help="String list of photon scale nuisance names - WILL NOT correlate across years (default: %default)")
parser.add_argument("--photonCatScalesCorr",default="MaterialCentralBarrel,MaterialOuterBarrel,MaterialForward",help="String list of photon scale nuisance names - WILL correlate across years (default: %default)")
parser.add_argument("--photonCatSmears",default="HighR9EE,LowR9EE,HighR9EBRho,LowR9EBRho,HighR9EBPhi,LowR9EBPhi",help="String list of photon smearing nuisance names - WILL NOT correlate across years (default: %default)")
parser.add_argument("--photonCatSmearsCorr",default="",help="String list of photon smearing nuisance names - WILL correlate across years (default: %default)")
parser.add_argument("--globalScales",default="NonLinearity:0.001,Geant4:0.0005,LightColl:0.0005,Absolute:0.0001",help="String list of global scale nuisances names with value separated by a \':\' - WILL NOT correlate across years (default: %default)")
parser.add_argument("--globalScalesCorr",default="",help="String list of global scale nuisances names with value separated by a \':\' - WILL correlate across years (default: %default)")
parser.add_argument("--toSkip",default="",help="proc:cat which are to skipped e.g ggH:11,qqH:12 etc. (default: %default)")
parser.add_argument("--theoryNormFactors",default="", help="if provided, will apply normalisation weights per process and per PDF, QCD Scale and alphaS weight.")
parser.add_argument("--isMultiPdf",default=False,action="store_true")
parser.add_argument("--submitSelf",default=False,action="store_true",help="Tells script to submit itself to the batch")
parser.add_argument("--justThisSyst",default="",help="Only calculate the line corresponding to thsi systematic")
parser.add_argument("--simplePdfWeights",default=False,action="store_true",help="Condense pdfWeight systematics into 1 line instead of full shape systematic" )
parser.add_argument("--scaleFactors",help="Scale factor for spin model pass as e.g. gg_grav:1.351,qq_grav:1.027")
parser.add_argument("--quadInterpolate",type=int,default=0,help="Do a quadratic interpolation of flashgg templates back to 1 sigma from this sigma. 0 means off (default: %default)")
parser.add_argument("--mass",type=int,default=125,help="Mass at which to calculate the systematic variations (default: %default)")
parser.add_argument("--ext",type=str,default="mva",help="file extension")
parser.add_argument("--intLumi",type=float,default=-1.,help="lumi")
parser.add_argument("--intLumi16",type=float,help="lumi")
parser.add_argument("--intLumi17",type=float,help="lumi")
parser.add_argument("--intLumi18",type=float,help="lumi")
parser.add_argument("--fullRunII", action='store_true',help="Make systematics year dependent", default=False)
parser.add_argument("--differential",action="store_true",dest="differential",help="differential settings")
parser.add_argument("--statonly",action="store_true",dest="statonly",help="ignore systematics")
parser.add_argument("--debugcats",action="store_true",dest="debugcats",help="hack to redefine and debug categories")
parser.add_argument("--constrainedMass",action="store",type=float, nargs=2, dest="constrainedMass",help="Write mass as nuisance parameter with first value given takes as central value, second as uncertainty")
options=parser.parse_args()
allSystList=[]
if options.submitSelf :
  options.justThisSyst="batch_split"
if (options.theoryNormFactors != ""):
   #import options.theoryNormFactors as th_norm
   print "IMPORTING theory norm factors %s",
   exec("import %s as th_norm"%options.theoryNormFactors.replace(".py","")) 
###############################################################################


###############################################################################
## FILE I/O ###################################################################
###############################################################################
#inFile = r.TFile.Open(options.infilename)
outFile = open(options.outfilename,'w')
###############################################################################

###############################################################################
## PROCS HANDLING & DICT ######################################################
###############################################################################
# convert flashgg style to combine style process
#combProc = {'ggH':'ggH','VBF':'qqH','ggh':'ggH','vbf':'qqH','wzh':'VH','wh':'WH','zh':'ZH','tth':'ttH','bkg_mass':'bkg_mass','gg_grav':'ggH_ALT','qq_grav':'qqbarH_ALT'}
combProc = {'InsideAcceptance':'InsideAcceptance', 'OutsideAcceptance':'OutsideAcceptance', 'bkg_mass':'bkg_mass'}
print combProc
#flashggProc = {'ggH':'ggh','qqH':'vbf','VH':'wzh','WH':'wh','ZH':'zh','ttH':'tth','bkg_mass':'bkg_mass','ggH_ALT':'gg_grav','qqbarH_ALT':'qq_grav'}
flashggProc = {'InsideAcceptance':'InsideAcceptance', 'OutsideAcceptance':'OutsideAcceptance', 'bkg_mass':'bkg_mass'}
print flashggProc
#procId = {'ggH':0,'qqH':-1,'VH':-2,'WH':-2,'ZH':-3,'ttH':-4,'ggH_ALT':-5,'qqbarH_ALT':-6,'bkg_mass':1}
procId = {'InsideAcceptance':0,'OutsideAcceptance':-1,'bkg_mass':1}
print procId
bkgProcs = ['bkg_mass'] #what to treat as background
print bkgProcs
#Determine if VH or WZH
splitVH=False
if 'wzh'in options.procs.split(','):
  splitVH=False
if 'wh' in options.procs.split(',') and 'zh' in options.procs.split(','):
  splitVH=True
#split procs vector
stringProcs = options.procs
print stringProcs
listProcs=[]
procIdx=-1;
for p in stringProcs.split(','):
   if p in combProc.keys():
      listProcs.append(combProc[p]) 
   else:
      listProcs.append(p)
   if options.differential:
      procId[p]=procIdx
      procIdx=procIdx-1
options.procs=listProcs
options.procs.append('bkg_mass')
options.toSkip = options.toSkip.split(',')
f_binsToSkip = open( ("../Signal/outdir_"+str(options.ext)+"/sigfit/binsToSkipInDatacard.txt"), 'r')
linesToSkip = f_binsToSkip.readlines()
for line in linesToSkip:
   thisBin = str(line.split('	')[0])+":"+str(line.split('	')[1].rstrip("\n"))
   print thisBin
   options.toSkip.append(thisBin)
print "###list of bins to be skipped"
print options.toSkip
###############################################################################

###############################################################################
## CATEGORISE TAGS FOR CONSIDERATION ##########################################
###############################################################################
#split cats
options.cats = options.cats.split(',')
# cat types
incCats     =[] #Untagged
dijetCats   =[] #VBF 
tthCats  =[]
tthLepCat  =[]
tthHadCat  =[]
vhHadCat    =[]
tightLepCat=[]
looseLepCat=[]
metCat=[]
reweightedDatasets = {}
#fill
for i in range(len(options.cats)):
  if "Untagged" in options.cats[i]:
    incCats.append(options.cats[i])
  if "VBF" in options.cats[i]:
    dijetCats.append(options.cats[i])
  if "TTHLeptonic" in options.cats[i]:
     tthLepCat.append(options.cats[i])
  if "TTHHadronic" in options.cats[i]:
     tthHadCat.append(options.cats[i])
  if "TTH" in options.cats[i]:
     tthCats.append(options.cats[i])
  if "VHHadronic" in options.cats[i]:
     vhHadCat.append(options.cats[i])
  if "VHTight" in options.cats[i]:
     tightLepCat.append(options.cats[i])
  if "VHLoose" in options.cats[i]:
     looseLepCat.append(options.cats[i])
  if "VHEt" in options.cats[i]:
     metCat.append(options.cats[i])
#summary 
print "[INFO] flashgg cats:"
print "--> incCats " , incCats
print "--> dijetCats " , dijetCats
print "--> tthLepCats " , tthCats
print "--> tthLepCats " , tthLepCat
print "--> tthHadCats " , tthHadCat
print "--> vhHadCats " , vhHadCat
print "--> tightLepCats " , tightLepCat
print "--> looseLepCats " , looseLepCat
print "--> metCat " , metCat
###############################################################################

###############################################################################
## PHOTON SMEAR/SCALE SYSTEMATICS ## ##########################################
###############################################################################
if options.photonCatScales=='' or options.statonly: options.photonCatScales = []
else: options.photonCatScales = options.photonCatScales.split(',')
if options.photonCatScalesCorr=='' or options.statonly: options.photonCatScalesCorr = []
else: options.photonCatScalesCorr = options.photonCatScalesCorr.split(',')
if options.photonCatSmears=='' or options.statonly: options.photonCatSmears = []
else: options.photonCatSmears = options.photonCatSmears.split(',')
if options.photonCatSmearsCorr=='' or options.statonly: options.photonCatSmearsCorr = []
else: options.photonCatSmearsCorr = options.photonCatSmearsCorr.split(',')
if options.globalScales=='' or options.statonly: options.globalScales = []
else: options.globalScales = options.globalScales.split(',')
if options.globalScalesCorr=='' or options.statonly: options.globalScalesCorr = []
else: options.globalScalesCorr = options.globalScalesCorr.split(',')
###############################################################################

###############################################################################
## OPEN WORKSPACE AND EXTRACT INFO # ##########################################
sqrts=13
print options.infilename
inWS = WSTFileWrapper(options.infilename,"cms_hgg_%sTeV"%sqrts)
#inWS = inFile.Get('wsig_13TeV')
if (inWS==None) : inWS = inFile.Get('tagsDumper/cms_hgg_%sTeV'%sqrts)
intL = inWS.var('IntLumi').getVal() if options.intLumi <=0 else options.intLumi*1000.

if options.intLumi16:
   intL16 = options.intLumi16*1000
else:
   intL16 = None
   
if options.intLumi17:
   intL17 = options.intLumi17*1000
else:
   intL17 = None
   
if options.intLumi18:
   intL18 = options.intLumi18*1000
else:
   intL18 = None
   
#intL=1290
#intL = 2690
#sqrts = inWS.var('IntLumi').getVal() #FIXME
print "[INFO] Get Intlumi from file, value : ", intL," pb^{-1}", " sqrts ", sqrts
###############################################################################

###############################################################################
## SHAPE SYSTEMATIC SETUP  ####################################################
###############################################################################
file_ext = options.ext
dataFile = 'CMS-HGG_%s_%dTeV_multipdf_$CAT.root'%(file_ext,sqrts)
bkgFile = 'CMS-HGG_%s_%dTeV_multipdf_$CAT.root'%(file_ext,sqrts)
dataWS = 'multipdf'
bkgWS = 'multipdf'
#sigFile = 'CMS-HGG_%s_%dTeV_sigfit.root'%(file_ext,sqrts)
sigFile = 'CMS-HGG_sigfit_%s_$PROC_$CAT.root'%(file_ext)
#print "making sigfile " ,sigFile
sigWS = 'wsig_%dTeV'%(sqrts)
# file detaisl: for FLashgg always use unbinned signal and multipdf
fileDetails = {}
fileDetails['data_obs'] = [dataFile,dataWS,'roohist_data_mass_$CHANNEL']
fileDetails['bkg_mass']  = [bkgFile,bkgWS,'CMS_hgg_$CHANNEL_%dTeV_bkgshape'%sqrts]
fileDetails['ggH']       = [sigFile.replace('$PROC',"ggh"),sigWS,'hggpdfsmrel_%dTeV_ggh_$CHANNEL'%sqrts]
fileDetails['qqH']       = [sigFile.replace('$PROC',"vbf"),sigWS,'hggpdfsmrel_%dTeV_vbf_$CHANNEL'%sqrts]
fileDetails['InsideAcceptance']       = [sigFile.replace('$PROC',"InsideAcceptance"),sigWS,'hggpdfsmrel_%dTeV_InsideAcceptance_$CHANNEL'%sqrts]
fileDetails['OutsideAcceptance']       = [sigFile.replace('$PROC',"OutsideAcceptance"),sigWS,'hggpdfsmrel_%dTeV_OutsideAcceptance_$CHANNEL'%sqrts]
fileDetails['qqH']       = [sigFile.replace('$PROC',"vbf"),sigWS,'hggpdfsmrel_%dTeV_vbf_$CHANNEL'%sqrts]
if splitVH:
  fileDetails['WH']       =  [sigFile.replace('$PROC',"wh"),sigWS,'hggpdfsmrel_%dTeV_wh_$CHANNEL'%sqrts]
  fileDetails['ZH']       =  [sigFile.replace('$PROC',"zh"),sigWS,'hggpdfsmrel_%dTeV_zh_$CHANNEL'%sqrts]
else:
  fileDetails['VH']       =  [sigFile.replace('$PROC',"wzh"),sigWS,'hggpdfsmrel_%dTeV_wzh_$CHANNEL'%sqrts]
fileDetails['ttH']       = [sigFile.replace('$PROC',"tth"),sigWS,'hggpdfsmrel_%dTeV_tth_$CHANNEL'%sqrts]
for p in options.procs:
   if p != "bkg_mass":
      fileDetails[p] = [sigFile.replace('$PROC',p),sigWS,'hggpdfsmrel_%dTeV_%s_$CHANNEL'%(sqrts,p)]
###############################################################################

###############################################################################
## THEORY SYSTEMATIC SETUP & TOOL #############################################
###############################################################################
# theory systematics arr=[up,down]
# --> globe info these come in specific types (as must be correlated with combination)
# -- globe info  - see https://twiki.cern.ch/twiki/bin/viewauth/CMS/HiggsWG/HiggsCombinationConventions
if not options.statonly:
   theorySyst = {}
   # theorySyst['scaleWeights'] = [1,2,3,4,6,8,"replicas"] #5,7 unphysical
   theorySyst['scaleWeights'] = [4,8,"asym"]
   theorySyst['alphaSWeights'] = [0,1,"asym"]
   theorySyst['pdfWeights'] = [1,60,"sym"]
   # theorySyst['pdfWeights_16'] = [1,60,"sym"]
   # theorySyst['pdfWeights_17_18'] = [1,60,"sym"]
   
   theorySystAbsScale={}
   theorySystAbsScale['names'] = ["QCDscale_qqbar_up","QCDscale_gg_up","QCDscale_qqbar_down","QCDscale_gg_down","pdf_alphaS_qqbar","pdf_alphaS_gg","pdf_qqbar","pdf_gg","alphaS_qqbar","alphaS_gg"] #QCD scale up, QCD scale down, PDF+alpha S, PDF, alpha S 
   #theorySystAbsScale['names_to_consider'] = ["QCDscale_qqbar_up","QCDscale_gg_up","QCDscale_qqbar_down","QCDscale_gg_down","pdf_alphaS_qqbar","pdf_alphaS_gg"] #QCD scale up, QCD scale down, PDF+alpha S, PDF, alpha S 
   theorySystAbsScale['names_to_consider'] = []
   theorySystAbsScale['ggH'] = [0.0,0.076,0.0,-0.081,0.0,0.031,0,0,0.018,0,0,0.025] # GGH is a _gg process
   theorySystAbsScale['qqH'] = [0.004,0.0,-0.003,0.0,0.021,0.0,0.021,0.0,0.005,0.0] # VBF is a _qqbar process 
   theorySystAbsScale['WH'] = [0.005,0.0,-0.007,0.0,0.019,0.0,0.017,0.0,0.009,0.0] # WZH is a _qqbar process, cporrelated with VBF
   theorySystAbsScale['ZH'] = [0.038,0.0,-0.031,0.0,0.016,0.0,0.013,0.0,0.009,0.0] # WZH is a _11bar process correlated with VBF 
   theorySystAbsScale['ttH'] = [0.0,-0.058,0.0,0.092,0.0,-0.036,0.0,-0.032,0.0,-0.020]  # TTH should be a _gg process anticorrelated with GGH


def getNominalDFs():

   r.gErrorIgnoreLevel = r.kError
   ret = {}
   for c in options.cats:
      for p in options.procs:
         if 'bkg' in p:
            continue
         
         datasetname = "%s_%d_13TeV_%s_%s"%(p.split("_",1)[0],options.mass,p.split("_",1)[1],c) if "_" in p else "%s_%d_13TeV_%s"%(p,options.mass,c)
         print(datasetname)
         ds = inWS.data(datasetname).Clone()
         weightVec = []
         for i in range(ds.numEntries()):
            ds.get(i)
            weightVec.append(ds.weight())
         ds.convertToTreeStore()
         df = pd.DataFrame(root_numpy.tree2array(ds.tree()))
         df['weight'] = np.array(weightVec)
         if 'JetBTagCutWeightCentral' in df.columns:
            df['JetBTagCutWeight'] = df['JetBTagCutWeightCentral']
         df = df.loc[abs(df['scaleWeights0'])>0.05, :]
         df = df.loc[abs(df['pdfWeights0'])>0.05, :]
         ret[datasetname] = df

   return ret
      
#yprinting function
def printTheorySysts():
  # as these are antisymmetric lnN systematics - implement as [1/(1.+err_down)] for the lower and [1.+err_up] for the upper
  print '[INFO] Theory...'
  for systName, systDetails in theorySyst.items():
    print "[INFO] processing ", systName ," from list ",theorySyst
    if "replicas" in systDetails[-1] :
        name="CMS_hgg_"+systName
        if (not "Theory" in allSystList ) :allSystList.append("Theory")
        if (not options.justThisSyst=="") :
          if (not options.justThisSyst=="Theory"): continue
        outFile.write('%-35s  lnN   '%(name))
        for ic in range(len(options.cats)):
          c = options.cats[ic]
          for ip in range(len(options.procs)):
            p = options.procs[ip]
##             if options.debugcats && ip != len(options.procs)-1: ###take diagonals only, except for the last one (Outside acceptance)
##                p = options.procs[ip]
###            if "bkg" in flashggProc[p] : 
            if "bkg" in p : 
              outFile.write('- ')
              continue
            else:
               print "blablabla"
               ### outFile.write(getFlashggLineTheoryEnvelope(flashggProc[p],c,systName,systDetails))
               outFile.write(getFlashggLineTheoryEnvelope(p,c,systName,systDetails))
        outFile.write('\n')
    else: #sym or asym uncertainties
      #print "consider ", systName
      asymmetric=("asym" in systDetails[-1])
      thSystEntries = range(systDetails[0], systDetails[1]) if not asymmetric else range(0, len(systDetails[:-1])-1)
      asymInds = None if not asymmetric else systDetails[:-1]
      years = []
      for it in systName.split('_'):
         if it in ['16', '17', '18']:
            years.append(it)
      if len(years) == 0:
         years = ['16', '17', '18']
      print(years)
      for i in thSystEntries:
        name="CMS_hgg_"+systName+str(i) if not asymmetric else "CMS_hgg_"+systName #FIXME
        if (not "Theory" in allSystList ) :allSystList.append("Theory")
        if (not options.justThisSyst=="") :
          if (not options.justThisSyst=="Theory"): continue
        if (i%1==0) : print "[INFO] processing ", name
        outFile.write('%-35s  lnN   '%(name))
        print "CATEGORIES"
        print options.cats
        for c in options.cats: 
          for p in options.procs: 
            if '%s:%s'%(p,c) in options.toSkip: continue
###            if "bkg" in flashggProc[p] : 
            if "bkg" in p : 
              outFile.write('- ')
              continue
            elif c[-2:] not in years:
              outFile.write('- ')
              continue
            else:
###              outFile.write(getFlashggLineTheoryWeights(flashggProc[p],c,systName,i,asymmetric))
               # tbw = getFlashggLineTheoryWeights(p,c,systName,i,asymmetric,asymInds)
               normalizeAccrProc = True if systName in ['scaleWeights', 'alphaSWeights'] else False
               tbw = getTheoryWeightsLineFast(p,c,systName.split("_")[0],i,asymmetric,asymInds,normalizeAccrProc)
               print "This is what we get back from getFlashggLineTheoryWeights"
               print tbw
###              outFile.write(getFlashggLineTheoryWeights(p,c,systName,i,asymmetric))
               outFile.write(tbw)
        outFile.write('\n')
      outFile.write('\n')
  
  #absolute scales for theory uncertainties.
  for syst in theorySystAbsScale['names_to_consider'] :
    if (not "Theory" in allSystList ) :allSystList.append("Theory")
    if (not options.justThisSyst=="") :
      if (not options.justThisSyst=="Theory"): continue
    #print  "DEBUG consider name ", syst
    asymmetric= False
    if "_up" in syst : asymmetric= True
    if "_down" in syst : continue #already considered as part of "_up"
    outFile.write('%-35s  lnN   '%(syst.replace("_up",""))) # if it doesn;t contain "_up", the replace has no effect anyway 
    for c in options.cats:
      for p in options.procs:
###            if "bkg" in flashggProc[p] : 
            if "bkg" in p : 
              outFile.write('- ')
              continue
            else:
              value = 1+theorySystAbsScale[p][theorySystAbsScale['names'].index(syst)] 
              if asymmetric :
                valueDown = 1+theorySystAbsScale[p][theorySystAbsScale['names'].index(syst.replace("_up","_down"))]
                if value==1.0 and valueDown==1.0 :
                  outFile.write("- ")
                else:
                  outFile.write("%1.3f/%1.3f "%(value,valueDown))
              else :
                if value==1.0 :
                  outFile.write("- ")
                else:
                  outFile.write("%1.3f "%(value))
    outFile.write('\n')

def getTheoryWeightsLineFast(proc,cat,name,i,asymmetric,asymInds=None,normalizeAccrProc=False):

   datasetname = "%s_%d_13TeV_%s_%s"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],cat) if "_" in proc else "%s_%d_13TeV_%s"%(proc,options.mass,cat)
   df = nominalDFDict[datasetname]
   centralWStr = 'scaleWeights0' if asymmetric else '{}0'.format(name)
   if asymmetric:
      wDown = df.eval('{}{}*(weight/{})'.format(name, asymInds[0], centralWStr))
      wUp = df.eval('{}{}*(weight/{})'.format(name, asymInds[1], centralWStr))
      downS = 1+((wDown.sum() - df['weight'].sum())/df['weight'].sum())
      upS = 1+((wUp.sum() - df['weight'].sum())/df['weight'].sum()) 
      shifts = np.array([downS, upS])
      if normalizeAccrProc:
         procWeightsShiftDown = np.concatenate([nominalDFDict["%s_%d_13TeV_%s_%s"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],catH) if "_" in proc else "%s_%d_13TeV_%s"%(proc,options.mass,catH)].eval('{}{}*(weight/{})'.format(name, asymInds[0], centralWStr)).values for catH in options.cats]).sum()
         procWeightsShiftUp = np.concatenate([nominalDFDict["%s_%d_13TeV_%s_%s"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],catH) if "_" in proc else "%s_%d_13TeV_%s"%(proc,options.mass,catH)].eval('{}{}*(weight/{})'.format(name, asymInds[1], centralWStr)).values for catH in options.cats]).sum()
         procWeightsNominal = np.concatenate([nominalDFDict["%s_%d_13TeV_%s_%s"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],catH) if "_" in proc else "%s_%d_13TeV_%s"%(proc,options.mass,catH)]['weight'].values for catH in options.cats]).sum()
         print 'procWeightsNom ', procWeightsNominal, 'procweightsDown ', procWeightsShiftDown, 'procWeightsUp ', procWeightsShiftUp
         shifts *= np.array([(procWeightsNominal/procWeightsShiftDown),(procWeightsNominal/procWeightsShiftUp)])
         print 'scaleWeights norma ', shifts
   else:
      wShift = df.eval('{}{}*(weight/{})'.format(name, i, centralWStr))
      sShift = 1+((wShift.sum() - df['weight'].sum())/df['weight'].sum()) 
      shifts = np.array([sShift, sShift])
      if normalizeAccrProc:
         procWeightsShift = np.concatenate([nominalDFDict["%s_%d_13TeV_%s_%s"%(procH.split("_",1)[0],options.mass,procH.split("_",1)[1],cat) if "_" in procH else "%s_%d_13TeV_%s"%(procH,options.mass,cat)].eval('{}{}*(weight/{})'.format(name, i, centralWStr)) for procH in procsH]).sum()
         procWeightsNominal =  np.concatenate([nominalDFDict["%s_%d_13TeV_%s_%s"%(procH.split("_",1)[0],options.mass,procH.split("_",1)[1],cat) if "_" in procH else "%s_%d_13TeV_%s"%(procH,options.mass,cat)]['weight'] for procH in procsH]).sum()
         shifts *= (procWeightsNominal/procWeightsShift)
   if any(shifts>10.) or any(shifts<0.1) or any(np.isnan(shifts)):
      print('Systematic shifts for dataset {} dont make sense: {}'.format(datasetname, shifts))
      return '- '
   
   if shifts[0] == shifts[1] and shifts[0] != 1.:
      ret = '{0:.3f} '.format(shifts[0])
   elif shifts[0] == 1 and shifts[1] == 1:
      ret = '- '
   else:
      ret = '{0:.3f}/{1:.3f} '.format(shifts[0],shifts[1])
      
   return ret

      
## pdf weights printing tool 
def getFlashggLineTheoryWeights(proc,cat,name,i,asymmetric,asymInds=None):
  print "name"
  print name
  n = i
  m = i
  ad_hoc_factor =1.
  theoryNormFactor_n=1. #up
  theoryNormFactor_m=1. #down
  if ( asymmetric ) : 
    ad_hoc_factor=1.5
    n = asymInds[0]
    m = asymInds[1]
  if (options.theoryNormFactors != ""):
     print proc,name.replace("Weight","")
     values = eval("th_norm.%s_%s"%(proc,name.replace("Weight","")))
     #print ("th_norm.%s_%s"%(proc,name.replace("Weight","")))
     theoryNormFactor_n= 1/values[n] #up
     theoryNormFactor_m= 1/values[m] #down
  
  mass = inWS.var("CMS_hgg_mass")
  mass.Print()
  weight = r.RooRealVar("weight","weight",0)
  print "%s%d"%(name,n)
  print "%s%d"%(name,m)
  weight_up = inWS.var("%s%d"%(name,n))
  weight_down = inWS.var("%s%d"%(name,m))
  weight_central = inWS.var("centralObjectWeight") 
  weight_sumW = inWS.var("sumW") 
  
  weight_up.Print()
  weight_down.Print()
  weight_central.Print() 
###  weight_sumW.Print() 

  #data_nominal = inWS.data("%s_%d_13TeV_%s"%(proc,options.mass,cat))
  # print options.infilename
  print "%s_%d_13TeV_%s"%(proc,options.mass,cat)
#  inWS.Print()
  # data_nominal= inWS.data("%s_%d_13TeV_%s_pdfWeights"%(proc,options.mass,cat))
  datasetname = "%s_%d_13TeV_%s_%s"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],cat) if "_" in proc else "%s_%d_13TeV_%s"%(proc,options.mass,cat)
  print "looking for "
  print datasetname
  data_nominal= inWS.data(datasetname)
  data_nominal.Print()
  #data_nominal= inWS.data("%s_%d_13TeV_%s"%(proc,options.mass,cat))
  data_nominal_sum = data_nominal.sumEntries()
  print data_nominal_sum
  if (data_nominal_sum <= 0.):#obsolete comment  ###WARNING: quick and dirty fix to skip pdf weight part
      print "[WARNING] This dataset has 0 or negative sum of weight. Systematic calulcxation meaningless, so list as '- '"
      line = '- '
      print "return at line 427"
      return line
  #data_nominal_num = data_nominal.numEntries()
  data_up = data_nominal.emptyClone();
  data_down = data_nominal.emptyClone();
  data_nominal_new = data_nominal.emptyClone();
  zeroWeightEvents=0.
  for i in range(0,int(data_nominal.numEntries())):
    mass.setVal(data_nominal.get(i).getRealValue("CMS_hgg_mass"))
    w_nominal =data_nominal.weight()
    # print "theoryNormFactor_m"
    # print theoryNormFactor_m
    # print "theoryNormFactor_n"
    # print theoryNormFactor_n
    # print "getRealValue n"
    # print data_nominal.get(i).getRealValue("%s%d"%(name,n))
    # print "getRealValue m"
    # print data_nominal.get(i).getRealValue("%s%d"%(name,m))
    w_up = theoryNormFactor_m*data_nominal.get(i).getRealValue("%s%d"%(name,n))
    w_down = theoryNormFactor_n*data_nominal.get(i).getRealValue("%s%d"%(name,m))
    # w_central = data_nominal.get(i).getRealValue(weight_central.GetName())
    # w_central = data_nominal.get(i).getRealValue("scaleWeights0") #sneaky fix as it doesn't look like central weight is beign propagated correctly in these cases.
    w_central = data_nominal.get(i).getRealValue("{}0".format(name)) if not asymmetric else data_nominal.get(i).getRealValue("scaleWeights0")
    ####SUPER sneaky fix as it doesn't look like central weight is beign propagated correctly in these cases.
   ## w_central = data_nominal.get(i).getRealValue("sumW") ####SUPER sneaky fix as it doesn't look like central weight is beign propagated correctly in these cases.
    sumW = data_nominal.get(i).getRealValue("sumW")
    if (w_central==0. or w_nominal==0. or math.isnan(w_down) or math.isnan(w_up) or w_down==0. or w_up==0.): 
        zeroWeightEvents=zeroWeightEvents+1.0
        if (zeroWeightEvents%1==0):
          print "[WARNING] skipping one event where weight is identically 0 or nan, causing  a seg fault, occured in ",(zeroWeightEvents/data_nominal.numEntries())*100 , " percent of events"
          print " WARNING] syst ", name,n, " ","procs/cat  " , proc,",",cat , " entry " , i, " w_nom ", w_nominal , "  w_up " , w_up , " w_down ", w_down ,"w_central ", w_central
          #exit(1)
        continue
    elif ( abs(w_central/w_down) <0.01 or abs(w_central/w_down) >100 ) :
         zeroWeightEvents=zeroWeightEvents+1.0
         if (zeroWeightEvents%1==0):
          print "[WARNING] skipping one event where weight is identically 0 or nan, causing  a seg fault, occured in ",(zeroWeightEvents/data_nominal.numEntries())*100 , " percent of events"
          print " [WARNING] syst ", name,n, " ","procs/cat  " , proc,",",cat , " entry " , i, " w_nom ", w_nominal , "  w_up " , w_up , " w_down ", w_down ,"w_central ", w_central
          #exit(1)
         continue
    weight_down.setVal(w_nominal*(w_down/w_central))
    # print "weight"
    weight_up.setVal(w_nominal*(w_up/w_central))
    data_up.add(r.RooArgSet(mass,weight_up),weight_up.getVal())
    data_down.add(r.RooArgSet(mass,weight_down),weight_down.getVal())
    data_nominal_new.add(r.RooArgSet(mass,weight),w_nominal)
  if (data_up.sumEntries() <= 0. or data_down.sumEntries() <= 0. ):
      print "[WARNING] This dataset has 0 or negative sum of weight. Systematic calulcxation meaningless, so list as '- '"
      line = '- '
      print "return at line 474"
      return line
  data_nominal_new.Print()
  data_down.Print()
  data_up.Print()
  print ad_hoc_factor
  systVals = interp1SigmaDataset(data_nominal_new,data_down,data_up,ad_hoc_factor)
  print "systVals"
  print systVals
  if (math.isnan(systVals[0]) or math.isnan(systVals[1]) or systVals[0]<0.2 or systVals[1]<0.2 ): 
    print "ERROR look at the value of these uncertainties!! systVals[0] ", systVals[0], " systVals[1] ", systVals[1]
    print "data Nominal"
    data_nominal_new.Print()
    print "data down "
    data_down.Print()
    print "data up "
    data_up.Print()
    exit (1)
  if (systVals[0] >10) : 
    print "ERROR look at the value of these uncertainties!! systVals[0] ", systVals[0], " systVals[1] ", systVals[1]
    print "data_nominal_new"
    data_nominal_new.Print()
    print "data_down"
    data_down.Print()
    print "data_up"
    data_up.Print()
    print "ad_hoc_factor"
    ad_hoc_factor
    exit (1)
  if ((systVals[1] >10)) : 
    print "data_nominal_new"
    data_nominal_new.Print()
    print "data_down"
    data_down.Print()
    print "data_up"
    data_up.Print()
    print "ad_hoc_factor"
    ad_hoc_factor
    print "ERROR look at the value of these uncertainties!! systVals[0] ", systVals[0], " systVals[1] ", systVals[1]
    exit (1)
  if systVals[0]==1 and systVals[1]==1:
      line = '- '
  elif (asymmetric):
      if systVals[0] < 1 and systVals[1] <1 :
        print "alpha S --- both systVals[0] ", systVals[0] , " and systVals[1] ", systVals[1] , " are less than 1 !"
        print "data_nominal_new"
        data_nominal_new.Print()
        print "data_down"
        data_down.Print()
        print "data_up"
        data_up.Print()
        print "ad_hoc_factor", ad_hoc_factor
        # exit(1)
      line = '%5.3f/%5.3f '%(systVals[0],systVals[1])
  else : #symmetric
      line = '%5.3f '%(systVals[0])
  print "return at line 525"   
  return line

## envelope computation, for Theory scale weights
def getFlashggLineTheoryEnvelope(proc,cat,name,details):
  
  indices=details[0:-1] # skip last entry whcuh is text specifying the treatment of uncertainty eg "replicas"
  histograms=[]
  h_nominal =None
  nBins=80
  
  for iReplica in indices:
    datasetname = "%s_%d_13TeV_%s_%s"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],cat) if "_" in proc else "%s_%d_13TeV_%s"%(proc,options.mass,cat)
    data_nominal = inWS.data(datasetname) #FIXME
    data_nominal_num = data_nominal.numEntries()
    data_new_h = r.TH1F("h_%d"%iReplica,"h_%d"%iReplica,nBins,100,180);
    data_nom_h = r.TH1F("h_nom_%d"%iReplica,"h_nom_%d"%iReplica,nBins,100,180);
    mass = inWS.var("CMS_hgg_mass")
    weight = r.RooRealVar("weight","weight",0)
    weight_new = inWS.var("%s%d"%(name,iReplica))
    theoryNormFactor=1.0
    if (options.theoryNormFactors != ""):
       values = eval("th_norm.%s_%s"%(proc,name.replace("Weight","")))
       theoryNormFactor= 1/values[iReplica]

    weight_central = inWS.var("centralObjectWeight")
    zeroWeightEvents=0.;
    for i in range(0,int(data_nominal.numEntries())):
      mass.setVal(data_nominal.get(i).getRealValue("CMS_hgg_mass"))
      mass.setBins(100)
      w_nominal =data_nominal.weight()
      w_new = theoryNormFactor*data_nominal.get(i).getRealValue("%s%d"%(name,iReplica))
      w_central = data_nominal.get(i).getRealValue("scaleWeights0")
      # w_central = data_nominal.get(i).getRealValue("centralObjectWeight")
      if (w_central==0. or w_nominal==0. or math.isnan(w_new) or w_new==0.):
        zeroWeightEvents=zeroWeightEvents+1.0
        if (zeroWeightEvents%1000==0):
          print "[WARNING] skipping one event where weight is identically 0, causing  a seg fault, occured in ",(zeroWeightEvents/data_nominal.numEntries())*100 , " percent of events"
          print " [WARNING] procs/cat  " , proc,",",cat , " entry " , i, " w_nom ", w_nominal , "  w_new " , w_new , "w_central ", w_central, "theoryNormFactor", theoryNormFactor
        #exit(1)
        continue
      elif( abs(w_central/w_new) >100 or  abs(w_central/w_new) <0.01) :
        zeroWeightEvents=zeroWeightEvents+1.0
        if (zeroWeightEvents%1000==0):
          print "[WARNING] skipping one event where weight is very large or small, causing  a seg fault, occured in ",(zeroWeightEvents/data_nominal.numEntries())*100 , " percent of events"
          print " [WARNING] procs/cat  " , proc,",",cat , " entry " , i, " w_nom ", w_nominal , "  w_new " , w_new , "w_central ", w_central
        #exit(1)
        continue
      weight_new.setVal(w_nominal*(w_new/w_central))
      data_new_h.Fill(mass.getVal(),weight_new.getVal())
      data_nom_h.Fill(mass.getVal(),w_nominal)
    histograms.append(data_new_h)
    if (h_nominal==None) : h_nominal=data_nom_h
  
  h_min = r.TH1F("h_min","h_min",nBins,100,180);
  h_max = r.TH1F("h_max","h_max",nBins,100,180);
  array ={}
  for iBin in range(0, h_min.GetNbinsX()): 
    array[iBin]=[]
    for iRep in range(0,len(indices)):
      content=histograms[iRep].GetBinContent(iRep)
      array[iBin].append(histograms[iRep].GetBinContent(iBin))
    h_min.SetBinContent(iBin,min(array[iBin]))
    h_max.SetBinContent(iBin,max(array[iBin]))

  systVals = interp1Sigma(h_nominal,h_min,h_max)
  if (systVals[0]>2. or  systVals[1]>2.):
    print " Look at these histograms because systVals[0]= ", systVals[0], " or systVals[1]= ",systVals[1]," :"
    print "h_nominal ", h_nominal.GetEntries(), " (", h_nominal.Integral(),")";
    print "h_min ", h_min.GetEntries(), " (", h_min.Integral(),")";
    print "h_max ", h_max.GetEntries(), " (", h_max.Integral(),")";
    if(h_nominal.Integral() <0. or  h_min.Integral() <0. or  h_max.Integral()<0.): 
        line = '- '
    else :
      exit(1)
    return line

  if systVals[0]==1 and systVals[1]==1:
        line = '- '
  else:
        line = '%5.3f/%5.3f '%(systVals[0],systVals[1])
  return line
###############################################################################

###############################################################################
## GENERAL ANALYSIS SYSTEMATIC SETUP  #########################################
###############################################################################
# BR uncertainty
# brSyst = [0.050,-0.049] #13TeV Values
# lumi syst
####lumiSyst = 0.026 #8TeV Values
# lumiSyst=0.025  #Correct for  13Tev!!!

##Printing Functions
def printBRSyst():
  print '[INFO] BR...'
  outFile.write('%-35s   lnN   '%('CMS_hgg_BR'))
  for c in options.cats:
    for p in options.procs:
      if '%s:%s'%(p,c) in options.toSkip: continue
      if p in bkgProcs:
        outFile.write('- ')
      else:
         outFile.write('%5.3f/%5.3f '%(1./(1.-brSyst[1]),1.+brSyst[0]))
  outFile.write('\n')
  outFile.write('\n')

# Uncorrelated 2015       0.9
# Uncorrelated 2016       0.6
# Uncorrelated 2017       2.0
# Uncorrelated 2018       1.5
# Correlated 2015,2016       1.1,0.9
# Correlated 2015,2016,2017,2018    0.6,0.6,0.9,2.0
# Correlated 2017,2018       0.6,0.2
def printLumiSyst():
  lumiSysts = {
     'lumi_16_uncorrelated' : [0.006, 0, 0],
     'lumi_17_uncorrelated' : [0, 0.020, 0],
     'lumi_18_uncorrelated' : [0 ,0, 0.015],
     'lumi_161718_correlated' : [0.006, 0.009, 0.02],
     'lumi_1718_correlated' : [0.0, 0.006, 0.002]
     # 'lumi_X_Y_factorization' : [0.009, 0.008, 0.02],
     # 'lumi_Length_Scale' : [0, 0.003, 0.003],
     # 'lumi_Beam_Beam_Deflection' : [0.004, 0.004, 0],
     # 'lumi_Dynamic_Beta' : [0.005, 0.005, 0],
     # 'lumi_Beam_Current_Calibration' : [0, 0.003, 0.002],
     # 'lumi_Ghosts_And_Satellites' : [0.004, 0.001, 0]
  }
  year = ['16', '17', '18']
  print '[INFO] Lumi...'
  for key in lumiSysts.keys():
      outFile.write('%-35s   lnN   '%(key.replace('lumi_', 'lumi_%sTeV_' %sqrts)))
      for c in options.cats:
         for p in options.procs:
           if '%s:%s'%(p,c) in options.toSkip: continue
           if p in bkgProcs:
               outFile.write('- ')
           else:
               if '_16' in c:
                  syst = lumiSysts[key][0]
               elif '_17' in c:
                  syst = lumiSysts[key][1]
               elif '_18' in c:
                  syst = lumiSysts[key][2]
                  
               if syst>0:
                  outFile.write('%5.3f '%(1.+syst))
               else:
                  outFile.write('- ')
      outFile.write('\n')
      outFile.write('\n')

def printTrigSyst():
  print '[INFO] Trig...'
  outFile.write('%-35s   lnN   '%'CMS_hgg_n_trig_eff')
  for c in options.cats:
    for p in options.procs:
      if '%s:%s'%(p,c) in options.toSkip: continue
      if p in bkgProcs:
        outFile.write('- ')
      else:
        outFile.write('%5.3f '%(1.+trigEff))
  outFile.write('\n')
  outFile.write('\n')
###############################################################################

###############################################################################
##  FLASHGG-SPECIFIC SYSTEMATIC SETUP  ########################################
###############################################################################
if not options.statonly:
   flashggSystDump = open('flashggSystDump.dat','w')
   flashggSysts={}

   # vtx eff
   vtxSyst = 0.015 

    #photon ID
   ###flashggSysts['MvaShift'] =  'phoIdMva'
   ###flashggSysts['LooseMvaSF'] =  'LooseMvaSF'
   ###flashggSysts['PreselSF']    =  'PreselSF'
   ###flashggSysts['SigmaEOverEShift'] = 'SigmaEOverEShift'
   ###flashggSysts['ElectronWeight'] = 'ElectronWeight'
   ###flashggSysts['electronVetoSF'] = 'electronVetoSF'
   ###flashggSysts['MuonWeight'] = 'MuonWeight'
   ###flashggSysts['TriggerWeight'] = 'TriggerWeight'
   ###flashggSysts['JetBTagWeight'] = 'JetBTagWeight'
   ####flashggSysts['MvaLinearSyst'] = 'MvaLinearSyst'
   ####flashggSysts[''] =  ''
   
   flashggSysts['MvaShift'] =  'phoIdMva'
   # flashggSysts['LooseMvaSF'] =  'LooseMvaSF'
   flashggSysts['PreselSF']    =  'PreselSF'
   flashggSysts['SigmaEOverEShift'] = 'SigmaEOverEShift'
   flashggSysts['electronVetoSF'] = 'electronVetoSF'
   flashggSysts['TriggerWeight'] = 'TriggerWeight'
   # flashggSysts['JEC'] = 'JEC'
   flashggSysts['JER'] = 'res_j'
   flashggSysts['PUJIDShift'] = 'PUJIDShift'


   if any("lepton" in s for s in options.procs) or any("Lepton" in s for s in options.procs) or any("NL_" in s for s in options.procs)  or '1L' in options.ext:
      flashggSysts['MuonIDWeight'] = 'eff_m_ID'
      flashggSysts['MuonIsoWeight'] = 'eff_m_MiniIso'
      flashggSysts['ElectronIDWeight'] = 'eff_e_ID'
      flashggSysts['ElectronRecoWeight'] = 'eff_e_Reco'


   if any("Bflavor" in s for s in options.procs) or any("Bjet" in s for s in options.procs) or any("BJet" in s for s in options.procs) or any("NBJ_" in s for s in options.procs) or '1B' in options.ext:
      flashggSysts['JetBTagCutWeight'] = 'eff_b'

   if "PtM" in options.ext:
      flashggSysts['metPhoUncertainty'] = 'MET_PhotonScale'
      flashggSysts['metUncUncertainty'] = 'MET_Unclustered'
      flashggSysts['metJecUncertainty'] = 'MET_JEC'
      flashggSysts['metJerUncertainty'] = 'MET_JER'

   if options.fullRunII:
      flashggSysts16 = {}
      flashggSysts17 = {}
      flashggSysts18 = {}
      
      for key in flashggSysts.keys():
         flashggSysts16['{}_16'.format(key)] = '{}_16'.format(flashggSysts[key])
         flashggSysts17['{}_17'.format(key)] = '{}_17'.format(flashggSysts[key])
         flashggSysts18['{}_18'.format(key)] = '{}_18'.format(flashggSysts[key])

      flashggSysts = dict(flashggSysts16)
      for dic in (flashggSysts17, flashggSysts18):
         flashggSysts.update(dic)

      flashggSysts['JetHEM_18'] = 'JetHEM_18'
      flashggSysts['prefireWeight_16'] = 'prefireWeight_16'
      flashggSysts['prefireWeight_17'] = 'prefireWeight_17'
      
      JECSysts = {
         'JECAbsolute' : 'Absolute',
         'JECRelativeBal' : 'RelativeBal',
         'JECBBEC1' : 'BBEC1',
         'JECEC2' : 'EC2',
         'JECFlavorQCD' : 'FlavorQCD',
         'JECHF' : 'HF'
      }

      for year in ['2016', '2017', '2018']:
         JECSysts['JECAbsolute{}'.format(year)] = 'Absolute_y_{}'.format(year)
         JECSysts['JECBBEC1{}'.format(year)] = 'BBEC1_y_{}'.format(year)
         JECSysts['JECEC2{}'.format(year)] = 'EC2_y_{}'.format(year)
         JECSysts['JECHF{}'.format(year)] = 'HF_y_{}'.format(year)
         JECSysts['JECRelativeSample{}'.format(year)] = 'RelativeSample_y_{}'.format(year)

#######   flashggSysts['JetBTagWeight'] = 'eff_b'
   #flashggSysts['MvaLinearSyst'] = 'MvaLinearSyst'
   #flashggSysts[''] =  ''



   #tth Tags
   tthSysts={}
   tthSysts['JEC'] = 'JEC_TTH'
   tthSysts['JER'] = 'JER_TTH'
   #flashggSysts['regSig'] = 'n_sigmae'
   #flashggSysts['idEff'] = 'n_id_eff'
   #flashggSysts['triggerEff'] = 'n_trig_eff'
   
   # pu jet eff = [ggEffect,qqEffect,WHeffect,ZHeffect,ttHeffect] - append for each vbf cat and for each VH hadronic cat
   puJetIdEff = []
   
   # naming is important to correlate with combination
   vbfSysts={}
   vbfSysts['JEC'] = [] 
   vbfSysts['JER'] = [] 
   vbfSysts['JetVeto'] =[]
   vbfSysts['UEPS'] =[]
   vbfSysts['RMSShift'] =[]
   for dijetCat in dijetCats: #each entry will represent a different migration
      vbfSysts['JER'].append([1.,1.,1.])  #value of 1 given gor both ggh and qqh, since vairations are taken from histograms directly
      vbfSysts['JEC'].append([1.,1.,1.]) #value of 1 given gor both ggh and qqh, since vairations are taken from histograms directly
   vbfSysts['RMSShift'].append([1.,1.]) #should only apply to ggh<->vbf
   vbfSysts['UEPS'].append([0.01,0.01]) # adhoc for ggh<->vbf ## taken from runI values.
   vbfSysts['UEPS'].append([0.02,0.01]) # adhoc for vbf0<->vbf1
   vbfSysts['JetVeto'].append([0.27,0.0]) # adhoc for ggh<->vbf ##as suggested my Martina, this onyl applies to VBF, not ggh.
   vbfSysts['JetVeto'].append([0.07,0.0]) # adhoc for vbf0<->vbf1
   
   #lepton, MET tags  ## lepton tags not considered for Dry run...
   # [VH tight, VH loose, ttH leptonic]
   eleSyst = {}
   muonSyst = {}
   metSyst = {}
   metSyst['ggH'] = [0.,0.,0.04] ##FIXME 13TeV Flashgg!!
   metSyst['qqH'] = [0.,0.,0.04]##FIXME 13TeV Flashgg!!
   metSyst['VH'] = [0.012,0.019,0.026] ##FIXME 13TeV Flashgg!!
   metSyst['ZH'] = [0.012,0.019,0.026] ##FIXME 13TeV Flashgg!!
   metSyst['WH'] = [0.012,0.019,0.026] ##FIXME 13TeV Flashgg!!
   metSyst['ttH'] = [0.011,0.012,0.040]##FIXME 13TeV Flashgg!!
   #tth tags  ## lepton tags not considered for Dry run...
   # syst for tth tags - [ttHlep,tthHad]
   ###tth tags not considered for dry run
   #btagSyst={}
   ggHforttHSysts = {}
   
   # spec for ggh in tth cats - [MC_low_stat,gluon_splitting,parton_shower]
   ggHforttHSysts['CMS_hgg_tth_mc_low_stat'] = 0.10 ##FIXME 13TeV Flashgg!!
   ggHforttHSysts['CMS_hgg_tth_gluon_splitting'] = 0.18 ##FIXME 13TeV Flashgg!!
   ggHforttHSysts['CMS_hgg_tth_parton_shower'] = 0.45 ##FIXME 13TeV Flashgg!!
   
   # rate adjustments
   tthLepRateScale = 1.0 ##FIXME 13TeV Flashgg!!
   tthHadRateScale = 1.0 ##FIXME 13TeV Flashgg!!

   print(flashggSysts)
###############################################################################

###############################################################################
##  INTERPOLATION TOOLS #######################################################
###############################################################################
def interp1Sigma(th1f_nom,th1f_down,th1f_up,factor=1.):
  nomE = th1f_nom.Integral()
  if abs(nomE)< 1.e-6:
    return [1.000,1.000]
  down_integral = th1f_down.Integral()
  if th1f_down.Integral()<0:
     down_integral=0.
  up_integral = th1f_up.Integral()
  if th1f_up.Integral()<0:
     up_integral=0.
  downE = 1+ factor*((down_integral - nomE) /nomE)
  print "downE"
  print downE
  upE = 1+ factor*((up_integral - nomE) /nomE)
  print "upE"
  print upE
  if options.quadInterpolate!=0:
    print "we do quadInterpolate" 
    downE = quadInterpolate(-1.,-1.*options.quadInterpolate,0.,1.*options.quadInterpolate,down_integral,th1f_nom.Integral(),up_integral)
    upE = quadInterpolate(1.,-1.*options.quadInterpolate,0.,1.*options.quadInterpolate,down_integral,th1f_nom.Integral(),up_integral)
    if upE != upE:
       print "upE != upE"
       upE=1.000
    if downE != downE: 
       print "downE != downE"
       downE=1.000
  return [downE,upE]

def interp1SigmaDataset(d_nom,d_down,d_up,factor=1.):
  nomE = d_nom.sumEntries()
  print "nomE ",nomE
###  if abs(nomE)< 1.e-6:
  if nomE< 1.e-6 or (d_down.sumEntries()<0.) or (d_up.sumEntries()<0.):
    print "nomE < 1.e-6, return [1.0,1.0]"
    return [1.000,1.000]
  sumEntr_down = d_down.sumEntries()
#  if d_down.sumEntries()<0.:
#     sumEntr_down = 0.
  sumEntr_up = d_up.sumEntries()
#  if d_up.sumEntries()<0.:
#     sumEntr_up = 0.
#  print "d_down.sumEntries() ",d_down.sumEntries()
  downE = 1+ factor*((sumEntr_down - nomE) /nomE)
  print "downE ",downE
#  print "d_up.sumEntries() ",d_up.sumEntries()
  upE = 1+ factor*((sumEntr_up - nomE) /nomE)
  print "upE ",upE
  if options.quadInterpolate!=0:
    print "we do quadInterpolate" 
    downE = quadInterpolate(-1.,-1.*options.quadInterpolate,0.,1.*options.quadInterpolate,sumEntr_down,d_nom.sumEntries(),s_up.sumEntries())
    upE = quadInterpolate(1.,-1.*options.quadInterpolate,0.,1.*options.quadInterpolate,sumEntr_down,d_nom.sumEntries(),sumEntr_up)
    if upE != upE:
       print "upE != upE"
       upE=1.000
    if downE != downE:
       print "downE != downE"
       downE=1.000
  return [downE,upE]
###############################################################################

###############################################################################
##  DATACARD PREAMBLE TOOLS ###################################################
###############################################################################
def printPreamble():
  print '[INFO] Making Preamble...'
  outFile.write('CMS-HGG datacard for parametric model - 2015 %dTeV \n'%sqrts)
  outFile.write('Auto-generated by flashggFinalFits/Datacard/makeParametricModelDatacardFLASHgg.py\n')
  outFile.write('Run with: combine\n')
  outFile.write('---------------------------------------------\n')
  outFile.write('imax *\n')
  outFile.write('jmax *\n')
  outFile.write('kmax *\n')
  outFile.write('---------------------------------------------\n')
  outFile.write('\n')
###############################################################################

###############################################################################
##  SHAPE SYSTEMATICS TOOLS ###################################################
###############################################################################
def printFileOptions():
  print '[INFO] File opts...'
  for typ, info in fileDetails.items():
     print "wiritng shapes into datacard for ",typ, info
     for c in options.cats:
        file = info[0]
        print file
        file = info[0].replace('$CAT','%s'%c)
        print file
        wsname = info[1]
        pdfname = info[2].replace('$CHANNEL','%s'%c)
        if typ == 'bkg_mass':
           if '16' in pdfname.split('_'):
              pdfname = pdfname.replace('_16_13TeV', '_16_16_13TeV')
           elif '17' in pdfname.split('_'):
              pdfname = pdfname.replace('_17_13TeV', '_17_17_13TeV')
           elif '18' in pdfname.split('_'):
              pdfname = pdfname.replace('_18_13TeV', '_18_18_13TeV')
           # elif 'ALL' in pdfname.split('_') and 'wUL17' in pdfname.split('_'):
           #    pdfname = pdfname.replace('_ALL_wUL17_13TeV', '_ALL_wUL17_ALL_wUL17_13TeV')
        if typ not in options.procs and typ!='data_obs': continue
      #outFile.write('shapes %-10s %-15s %-30s %-30s\n'%(typ,'%s_%dTeV'%(c,sqrts),file.replace(".root","_%s_%s.root"%(typ,c)),wsname+':'+pdfname))
        outFile.write('shapes %-10s %-15s %-30s %-30s\n'%(typ,'%s_%dTeV'%(c,sqrts),file,wsname+':'+pdfname))
  outFile.write('\n')
###############################################################################

###############################################################################
##  PROCESS/BIN LINES TOOLS ###################################################
###############################################################################
def printObsProcBinLines():
  print '[INFO] Rates...'
  outFile.write('%-15s '%'bin')
  for c in options.cats:
    outFile.write('%s_%dTeV '%(c,sqrts))
  outFile.write('\n')
  
  outFile.write('%-15s '%'observation')
  for c in options.cats:
    outFile.write('-1 ')
  outFile.write('\n')
  
  outFile.write('%-15s '%'bin')
  for c in options.cats:
    for p in options.procs:
      if '%s:%s'%(p,c) in options.toSkip: continue
      outFile.write('%s_%dTeV '%(c,sqrts))
  outFile.write('\n')
  
  outFile.write('%-15s '%'process')
  for c in options.cats:
    for p in options.procs:
      if '%s:%s'%(p,c) in options.toSkip: continue
      outFile.write('%s '%p)
  outFile.write('\n')

  outFile.write('%-15s '%'process')
  for c in options.cats:
    for p in options.procs:
      if '%s:%s'%(p,c) in options.toSkip: continue
      outFile.write('%d '%procId[p])
  outFile.write('\n')

  outFile.write('%-15s '%'rate')
  for c in options.cats:
    for p in options.procs:
      if '%s:%s'%(p,c) in options.toSkip: continue
      if p in bkgProcs:
        outFile.write('1.0 ')
      else:
        scale=1.
        if c in looseLepCat: scale *= looseLepRateScale
        if c in tightLepCat: scale *= tightLepRateScale
        if c in tthCats:
          if c in tthLepCat: scale *= tthLepRateScale
          else: scale *= tthHadRateScale
          
        if intL16 is not None and '_16'in c:
           outFile.write('%7.1f '%(intL16*scale))
        elif intL17 is not None and '_17' in c:
           outFile.write('%7.1f '%(intL17*scale))
        elif intL18 is not None and '_18' in c:
           outFile.write('%7.1f '%(intL18*scale))
        else:
           outFile.write('%7.1f '%(intL*scale))
  outFile.write('\n')
  outFile.write('\n')
###############################################################################

###############################################################################
##  NUISANCE PARAM LINES TOOLS ################################################
###############################################################################
def printNuisParam(name,typ,sqrtS=None):
  val="1.0"
  if ":" in name:
    name,val = name.split(":")
  if sqrtS:
    typ="%dTeV%s" % (sqrtS, typ)
  outFile.write('%-40s param 0.0 %s\n'%('CMS_hgg_nuisance_%s_%s'%(name,typ),val))

def printNuisParams():
    print '[INFO] Nuisances...'
    outFile.write('%-40s param 0.0 %1.4g\n'%('CMS_hgg_nuisance_deltafracright',vtxSyst))
    for phoSyst in options.photonCatScales:
      printNuisParam(phoSyst,"scale",sqrts)
    for phoSyst in options.photonCatScalesCorr:
      printNuisParam(phoSyst,"scale")
    for phoSyst in options.globalScales:
      printNuisParam(phoSyst,"scale",sqrts)      
    for phoSyst in options.globalScalesCorr:
      printNuisParam(phoSyst,"scale")
    for phoSyst in options.photonCatSmears:
      printNuisParam(phoSyst,"smear",sqrts)
    for phoSyst in options.photonCatSmearsCorr:
      printNuisParam(phoSyst,"smear")
    if options.constrainedMass is not None:
      outFile.write('MH param %.2f %1.4g\n'%(options.constrainedMass[0], options.constrainedMass[1]))
    outFile.write('\n')
###############################################################################


###############################################################################
##  LN(N) LINES TOOLS ########################################################
###############################################################################
#individual numbers for each proc/cat
def getFlashggLine(proc,cat,syst):
  asymmetric=False 
  eventweight=False 
  #print "===========> SYST", syst ," PROC ", proc , ", TAG ", cat
###  dataSYMMETRIC =  inWS.data("%s_%d_13TeV_%s_%s"%(flashggProc[proc],options.mass,cat,syst)) #Will exist if the systematic is a symmetric uncertainty not stored as event weights
###  dataDOWN =  inWS.data("%s_%d_13TeV_%s_%sDown01sigma"%(flashggProc[proc],options.mass,cat,syst)) # will exist if teh systematic is an asymetric uncertainty not strore as event weights
###  dataUP =  inWS.data("%s_%d_13TeV_%s_%sUp01sigma"%(flashggProc[proc],options.mass,cat,syst))# will exist if teh systematic is an asymetric uncertainty not strore as event weights
###  dataNOMINAL =  inWS.data("%s_%d_13TeV_%s"%(flashggProc[proc],options.mass,cat)) #Nominal RooDataSet,. May contain required weights if UP/DOWN/SYMMETRIC roodatahists do not exist (ie systematic stored as event weigths)
  print "SYMMETRIC "
  datasetname = "%s_%d_13TeV_%s_%s"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],cat) if "_" in proc else "%s_%d_13TeV_%s"%(proc,options.mass,cat)
  print "%s_%s"%(datasetname,syst)
#  dataSYMMETRIC =  inWS.data("%s_%d_13TeV_%s_%s_%s"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],cat,syst)) #Will exist if the systematic is a symmetric uncertainty not stored as event weights
  dataSYMMETRIC =  inWS.data("%s_%s"%(datasetname,syst)) #Will exist if the systematic is a symmetric uncertainty not stored as event weights
  print "DOWN"
  print "%s_%sDown01sigma"%(datasetname,syst)
#  dataDOWN =  inWS.data("%s_%d_13TeV_%s_%s_%sDown01sigma"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],cat,syst)) # will exist if teh systematic is an asymetric uncertainty not strore as event weights
  dataDOWN =  inWS.data("%s_%sDown01sigma"%(datasetname,syst)) # will exist if teh systematic is an asymetric uncertainty not strore as event weights
  print "UP"
  print "%s_%sUp01sigma"%(datasetname,syst)
#  dataUP =  inWS.data("%s_%d_13TeV_%s_%s_%sUp01sigma"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],cat,syst))# will exist if teh systematic is an asymetric uncertainty not strore as event weights
  dataUP =  inWS.data("%s_%sUp01sigma"%(datasetname,syst))# will exist if teh systematic is an asymetric uncertainty not strore as event weights
  print "NOMINAL"
  print datasetname
#  dataNOMINAL =  inWS.data("%s_%d_13TeV_%s_%s"%(proc.split("_",1)[0],options.mass,proc.split("_",1)[1],cat)) #Nominal RooDataSet,. May contain required weights if UP/DOWN/SYMMETRIC roodatahists do not exist (ie systematic stored as event weigths)
  dataNOMINAL =  inWS.data(datasetname) #Nominal RooDataSet,. May contain required weights if UP/DOWN/SYMMETRIC roodatahists do not exist (ie systematic stored as event weigths)

  dataNOMINAL.Print()
  if (dataSYMMETRIC==None):
    if( (dataUP==None) or  (dataDOWN==None)) :
      #print "[INFO] Systematic ", syst," stored as asymmetric event weights in RooDataSet"
      asymmetric=True
      eventweight=True
    else:
      #print "[INFO] Systematic ", syst," stored as asymmetric rooDataHists"
      asymmetric=True
      eventweight=False
  else:
      #print "[INFO] Systematic ", syst," stored as symmetric rooDataHist"
      asymmetric=False
      eventweight=False
  
  if (asymmetric and eventweight) :

     df = nominalDFDict[datasetname]
     # for tpeWeight in ['ElectronIDWeight', 'ElectronRecoWeight', 'MuonIsoWeight', 'MuonIDWeight', 'JetBTagCutWeight']:
     #    if tpeWeight in df.columns:
     #       df['weight'] = df.eval('weight/{}'.format(tpeWeight))
     wDown = df.eval('{}Down01sigma*(weight/centralObjectWeight)'.format(syst))
     wUp = df.eval('{}Up01sigma*(weight/centralObjectWeight)'.format(syst))
     downS = 1 + ((wDown.sum() - df['weight'].sum())/df['weight'].sum())
     upS = 1 + ((wUp.sum() - df['weight'].sum())/df['weight'].sum())
     systVals = [downS, upS]
    # data_up = dataNOMINAL.emptyClone();
    # data_down = dataNOMINAL.emptyClone();
    # data_nominal = dataNOMINAL.emptyClone();
    # mass = inWS.var("CMS_hgg_mass")
    # weight = r.RooRealVar("weight","weight",0)
    # weight_up = inWS.var("%sUp01sigma"%syst)
    # #weight_down = inWS.var("%sDown01sigma"%sys)
    # weight_down = r.RooRealVar("%sDown01sigma"%syst,"%sDown01sigma"%syst,-1.)
    # weight_central = inWS.var("centralObjectWeight")
    # zeroWeightEvents=0.
    # for i in range(0,int(dataNOMINAL.numEntries())):
    #   mass.setVal(dataNOMINAL.get(i).getRealValue("CMS_hgg_mass"))
    #   w_nominal =dataNOMINAL.weight()
    #   w_down = dataNOMINAL.get(i).getRealValue(weight_down.GetName())
    #   w_up = dataNOMINAL.get(i).getRealValue(weight_up.GetName())
    #   w_central = dataNOMINAL.get(i).getRealValue(weight_central.GetName())
    #   if (w_central==0.) :
    #     zeroWeightEvents=zeroWeightEvents+1.0
    #     if (zeroWeightEvents%1==0):
    #       print "[WARNING] skipping one event where weight is identically 0, causing  a seg fault, occured in ",(zeroWeightEvents/dataNOMINAL.numEntries())*100 , " percent of events"
    #       print "[WARNING]  syst " , syst , " w_nom ", w_nominal , "  w_up " , w_up , " w_ down " , w_down, "w_central ", w_central
    #       #exit(1)
    #     continue
    #   if (w_up==w_down):
    #     weight_down.setVal(w_nominal)
    #     weight_up.setVal(w_nominal)
    #   else :
    #     weight_down.setVal(w_nominal*(w_down/w_central))
    #     weight_up.setVal(w_nominal*(w_up/w_central))

    #   data_up.add(r.RooArgSet(mass,weight_up),weight_up.getVal())
    #   data_down.add(r.RooArgSet(mass,weight_down),weight_down.getVal())
    #   data_nominal.add(r.RooArgSet(mass,weight),w_nominal)
    # dataUP =  data_up  #repalce UP/DOwn histograms defined outside scope of this "if"
    # dataDOWN =  data_down  #repalce UP/DOwn histograms defined outside scope of this "if"
    # dataNOMINAL =  data_nominal  #repalce UP/DOwn histograms defined outside scope of this "if"
     flashggSystDump.write('proc: %s, cat: %s, %s nominal: %5.3f up: %5.3f down: %5.3f vals: [%5.3f,%5.3f] \n'%(proc,cat,syst,dataNOMINAL.sumEntries(),wUp.sum(),wDown.sum(),systVals[0],systVals[1]))
  else:
     data_nominal = dataNOMINAL.emptyClone();
     use_nominal = False
     weightNew = r.RooRealVar("weight","weight",0)
     massNew = inWS.var("CMS_hgg_mass")
     exWgtList = []
     # for tpeWeight in ['ElectronIDWeight', 'ElectronRecoWeight', 'MuonIsoWeight', 'MuonIDWeight', 'JetBTagCutWeightCentral']:
     #    if inWS.var(tpeWeight) is not None:
     #       weight_tpe = inWS.var(tpeWeight)
     #       exWgtList.append(weight_tpe)
     #       use_nominal = True
     # print "use_nominal: ", use_nominal, " exWgtList: ", exWgtList
     # if use_nominal:
     #    if dataNOMINAL.GetName() not in reweightedDatasets.keys():
     #       for i in range(0, int(dataNOMINAL.numEntries())):
     #          massNew.setVal(dataNOMINAL.get(i).getRealValue("CMS_hgg_mass"))
     #          wgtNOM = dataNOMINAL.weight()
     #          wgtSet = wgtNOM
     #          for tpeWgt in exWgtList:
     #             wgtSet /= dataNOMINAL.get(i).getRealValue(tpeWgt.GetName())
     #          weightNew.setVal(wgtSet)
     #          data_nominal.add(r.RooArgSet(massNew, weightNew), weightNew.getVal())
     #       reweightedDatasets[data_nominal.GetName()] = data_nominal
     #    systVals = interp1SigmaDataset(reweightedDatasets[dataNOMINAL.GetName()],dataDOWN,dataUP)
     # else:
     systVals = interp1SigmaDataset(dataNOMINAL,dataDOWN,dataUP)
     flashggSystDump.write('proc: %s, cat: %s, %s nominal: %5.3f up: %5.3f down: %5.3f vals: [%5.3f,%5.3f] \n'%(proc,cat,syst,dataNOMINAL.sumEntries(),dataUP.sumEntries(),dataDOWN.sumEntries(),systVals[0],systVals[1]))
  print "systvals ", systVals 
  if systVals[0]==1 and systVals[1]==1:
      line = '- '
  elif systVals[0] == systVals[1] and systVals[0] != 1.:
     line = '%5.3f '%(systVals[0])
  else:
      line = '%5.3f/%5.3f '%(systVals[0],systVals[1])
  return line

# printing whole lines 
def printFlashggSysts():
  print '[INFO] lnN lines...'
  for flashggSyst, paramSyst in flashggSysts.items():
      name='CMS_hgg_%s'%paramSyst
      print "[INFO] processing " ,name
      allSystList.append(name)
      if (not options.justThisSyst==""):
          if (not options.justThisSyst==name): continue
      outFile.write('%-35s   lnN   '%(name))
      for c in options.cats:
        for p in options.procs:
          if '%s:%s'%(p,c) in options.toSkip: continue
          if p in bkgProcs or ('pdfWeight' in flashggSyst and (p!='ggH' and p!='qqH')):
             outFile.write('- ')
          elif options.fullRunII and '_16' in flashggSyst and not c[-3:] == '_16':
             outFile.write('- ')
          elif options.fullRunII and '_17' in flashggSyst and not c[-3:] == '_17':
             outFile.write('- ')
          elif options.fullRunII and '_18' in flashggSyst and not c[-3:] == '_18':
             outFile.write('- ')
          else:
             outFile.write(getFlashggLine(p,c,flashggSyst))
      outFile.write('\n')
  outFile.write('\n')


def printJECSysts():
  print '[INFO] lnN lines...'
  for JECSyst, paramSyst in JECSysts.items():
      name='CMS_scale_j_%s'%paramSyst
      print "[INFO] processing " ,name
      allSystList.append(name)
      if (not options.justThisSyst==""):
          if (not options.justThisSyst==name): continue
      outFile.write('%-35s   lnN   '%(name))
      for c in options.cats:
        for p in options.procs:
          if '%s:%s'%(p,c) in options.toSkip: continue
          if p in bkgProcs or ('pdfWeight' in JECSyst and (p!='ggH' and p!='qqH')):
             outFile.write('- ')
          elif options.fullRunII and '2016' in JECSyst and not c[-3:] == '_16':
             outFile.write('- ')
          elif options.fullRunII and '2017' in JECSyst and not c[-3:] == '_17':
             outFile.write('- ')
          elif options.fullRunII and '2018' in JECSyst and not c[-3:] == '_18':
             outFile.write('- ')
          else:
             outFile.write(getFlashggLine(p,c,JECSyst))
      outFile.write('\n')
  outFile.write('\n')
###############################################################################

###############################################################################
##  VBF CATEGORY MIGRATION LINES TOOLS ########################################
###############################################################################
def printVbfSysts():
  # we first figure out what migrations are needed
  # e.g. for 5 inc cats and 3 vbf cats we need:
  # cat5 -> cat6, cat5+cat6 -> cat7, cat5+cat6+cat7 -> incCats
  # the other important thing is to adjust the name of the VBFtot -> incCats mig to 
  # correlate with combination for the QCDscale and UEPS
  print "[INFO] considering VBF catgeory migrations"
  # now print relevant numbers
  for vbfSystName, vbfSystValArray in vbfSysts.items():
    asymmetric=False
    adhoc=False
    affectsTTH=None
    if (len(vbfSystValArray)>(len(dijetCats))) : affectsTTH=True
    #print "vbfSystName, vbfSystValArray ", vbfSystName,", ", vbfSystValArray, " affects tth ? ", affectsTTH
    
    # work out which cats we are migrating to and from
    syst=vbfSystName
    if (len(vbfSystValArray)==0) : continue
    vbfMigrateFromCats=[]
    vbfMigrateToCats=[]
    vbfMigrateFromEvCount={}
    vbfMigrateToEvCount={}
    vbfMigrateFromEvCountUP={}
    vbfMigrateToEvCountUP={}
    vbfMigrateFromEvCountDOWN={}
    vbfMigrateToEvCountDOWN={}
    vbfMigrateFromEvCountNOMINAL={}
    vbfMigrateToEvCountNOMINAL={}
    temp = []
    for c in dijetCats:
      temp.append(c)
      vbfMigrateFromCats.append(copy.copy(temp))
      if c==options.cats[len(incCats)+len(dijetCats)-1]: # i.e. last vbf cat
        vbfMigrateToCats.append(incCats)
      else:
        index=options.cats.index(c)
        dummy=[]
        dummy.append(options.cats[index+1])
        vbfMigrateToCats.append(dummy)
    if (affectsTTH):
     vbfMigrateToCats.append(incCats)
     vbfMigrateFromCats.append(tthHadCat)
     vbfMigrateToCats.append(incCats) 
     vbfMigrateFromCats.append(tthLepCat)
    
    # reverse
    vbfMigrateToCats.reverse()
    vbfMigrateFromCats.reverse()
    #summary
    #print "--> cats To " , vbfMigrateToCats 
    #print "--> cats From " , vbfMigrateFromCats 
      
    # now get relevant event counts
    for p in options.procs:
      if p in bkgProcs: continue
      vbfMigrateToEvCount[p] = []
      vbfMigrateToEvCountNOMINAL[p] = []
      vbfMigrateToEvCountUP[p] = []
      vbfMigrateToEvCountDOWN[p] = []
      for cats in vbfMigrateToCats:
        sum=0
        sumUP=0
        sumNOMINAL=0
        sumDOWN=0
        for c in cats:
          #print "looking at c ", c , " proc ", p , " syst ", syst
###          data =  inWS.data("%s_%d_13TeV_%s_%s"%(flashggProc[p],options.mass,c,syst))                
###          dataDOWN =  inWS.data("%s_%d_13TeV_%s_%sDown01sigma"%(flashggProc[p],options.mass,c,syst)) 
###          dataNOMINAL =  inWS.data("%s_%d_13TeV_%s"%(flashggProc[p],options.mass,c))                 
          data =  inWS.data("%s_%d_13TeV_%s_%s"%(p,options.mass,c,syst))                
          dataDOWN =  inWS.data("%s_%d_13TeV_%s_%sDown01sigma"%(p,options.mass,c,syst)) 
          dataNOMINAL =  inWS.data("%s_%d_13TeV_%s"%(p,options.mass,c))                 
          #print "DEBUG got dataNOMINAL " , ("%s_%d_13TeV_%s"%(flashggProc[p],options.mass,c)), " --> dataNOMINAL" , dataNOMINAL
          mass = inWS.var("CMS_hgg_mass")
###          dataUP =  inWS.data("%s_%d_13TeV_%s_%sUp01sigma"%(flashggProc[p],options.mass,c,syst))
          dataUP =  inWS.data("%s_%d_13TeV_%s_%sUp01sigma"%(p,options.mass,c,syst))
          
          if (data==None):
            if( (dataUP==None) or  (dataDOWN==None)) :
              #print "[INFO] VBF Systematic ", syst," could not be found either as symmetric (",syst,") or asymmetric (",syst,"Down01sigma,",syst,"Up01sigma). Consider as adhoc variation..."
              adhoc=True
              asymmetric=False
            else:
              asymmetric=True
              #print "[INFO] VBF Systematic ", syst," will be treated as asymmetric"
          else:
              asymmetric=False
              #print "[INFO] VBF Systematic ", syst," wil be treated as symmetric"


          if (asymmetric) :
            sumUP += dataUP.sumEntries()
            sumDOWN += dataDOWN.sumEntries()
            sumNOMINAL += dataNOMINAL.sumEntries()
          elif (adhoc) : 
            sumNOMINAL += dataNOMINAL.sumEntries()
          else : 
            sum += data.sumEntries()
            sumNOMINAL += dataNOMINAL.sumEntries()
        vbfMigrateToEvCount[p].append(sum)
        vbfMigrateToEvCountNOMINAL[p].append(sumNOMINAL)
        vbfMigrateToEvCountUP[p].append(sumUP)
        vbfMigrateToEvCountDOWN[p].append(sumDOWN)
    for p in options.procs:
      if p in bkgProcs: continue
      vbfMigrateFromEvCount[p] = []
      vbfMigrateFromEvCountNOMINAL[p] = []
      vbfMigrateFromEvCountUP[p] = []
      vbfMigrateFromEvCountDOWN[p] = []
      for cats in vbfMigrateFromCats:
        sum=0
        sumUP=0
        sumNOMINAL=0
        sumDOWN=0
        for c in cats:
          data =  inWS.data("%s_%d_13TeV_%s_%s"%(flashggProc[p],options.mass,c,syst))
          dataDOWN =  inWS.data("%s_%d_13TeV_%s_%sDown01sigma"%(flashggProc[p],options.mass,c,syst))
          dataNOMINAL =  inWS.data("%s_%d_13TeV_%s"%(flashggProc[p],options.mass,c))
          dataUP =  inWS.data("%s_%d_13TeV_%s_%sUp01sigma"%(flashggProc[p],options.mass,c,syst))
          if (asymmetric) :
            sumUP += dataUP.sumEntries()
            sumDOWN += dataDOWN.sumEntries()
            sumNOMINAL += dataNOMINAL.sumEntries()
          elif (adhoc) :
            sumNOMINAL += dataNOMINAL.sumEntries()
          else : 
            sum += data.sumEntries()
            sumNOMINAL += dataNOMINAL.sumEntries()
        vbfMigrateFromEvCount[p].append(sum)
        vbfMigrateFromEvCountUP[p].append(sumUP)
        vbfMigrateFromEvCountNOMINAL[p].append(sumNOMINAL)
        vbfMigrateFromEvCountDOWN[p].append(sumDOWN)
    
    for migIt, vbfSystVal in (enumerate(vbfSystValArray)):
      name = "CMS_hgg_"+vbfSystName
      name += '_migration%d'%(migIt)
      allSystList.append(name)
      if (not options.justThisSyst=="") :
          if (not options.justThisSyst==name): continue
      outFile.write('%-35s   lnN   '%name)
      for c in options.cats:
        for p in options.procs:
          if '%s:%s'%(p,c) in options.toSkip: continue
          if p=='ggH': thisUncert = vbfSystVal[0]
          elif p=='qqH': thisUncert = vbfSystVal[1]
          elif (p=='ttH' and affectsTTH): thisUncert = vbfSystVal[2]
          else:
            outFile.write('- ')
            continue
          if thisUncert==0:
            outFile.write('- ')
          else:
            if c in vbfMigrateToCats[migIt]:
              if (asymmetric) : 
                UP=vbfMigrateToEvCountUP[p][migIt]/vbfMigrateToEvCountNOMINAL[p][migIt]
                DOWN=vbfMigrateToEvCountDOWN[p][migIt]/vbfMigrateToEvCountNOMINAL[p][migIt]
                outFile.write('%1.4g/%1.4g '%(DOWN,UP))
              elif (adhoc) : 
                VAR=((vbfMigrateToEvCountNOMINAL[p][migIt]-thisUncert*vbfMigrateFromEvCountNOMINAL[p][migIt])/vbfMigrateToEvCountNOMINAL[p][migIt]) 
                #print " TO categories : " , VAR
                outFile.write('%1.4g '%VAR)
              else : outFile.write('%1.4g '%(vbfMigrateToEvCount[p][migIt]/vbfMigrateToEvCountNOMINAL[p][migIt]))
            elif c in vbfMigrateFromCats[migIt]:
              if (asymmetric):
                UP=vbfMigrateFromEvCountUP[p][migIt]/vbfMigrateFromEvCountNOMINAL[p][migIt]
                DOWN=vbfMigrateFromEvCountDOWN[p][migIt]/vbfMigrateFromEvCountNOMINAL[p][migIt]
                outFile.write('%1.4g/%1.4g '%(DOWN,UP))
              elif (adhoc) :
                VAR=(1.+thisUncert)
                #print " FROM categories : " , VAR
                outFile.write('%1.4g '%VAR)
              else:
                VAR=vbfMigrateFromEvCount[p][migIt]/vbfMigrateFromEvCountNOMINAL[p][migIt]
                outFile.write('%1.4g '%(VAR))
            else:
              outFile.write('- ')
      outFile.write('\n')
    outFile.write('\n')
###############################################################################

###############################################################################
##  LEPTON SYST LINES TOOLS ###################################################
###############################################################################
def printLepSysts():
  print '[INFO] Lep...'
  # electron efficiency -- NOTE to correlate with combination change to CMS_eff_e

  # met efficiency -- NOTE to correlate with combination change to CMS_scale_met
  outFile.write('%-35s   lnN   '%('CMS_scale_met_old'))
  for c in options.cats:
    for p in options.procs:
      if '%s:%s'%(p,c) in options.toSkip: 
        outFile.write('- ')
        continue
      if p in bkgProcs or p=='ggH' or p=='qqH': 
        outFile.write('- ')
        continue
      else:
        if c in tightLepCat: thisUncert = metSyst[p][0]
        elif c in looseLepCat: thisUncert = metSyst[p][1]
        elif c in tthLepCat: thisUncert = metSyst[p][2]
        else: thisUncert = 0.
        if thisUncert==0:
          outFile.write('- ')
        else:
          outFile.write('%6.4f/%6.4f '%(1.-thisUncert,1+thisUncert))
  outFile.write('\n')
###############################################################################

###############################################################################
##  TTH SYST LINES TOOLS ######################################################
###############################################################################
def printTTHSysts():
  print '[INFO] TTH lnN lines...'
  for tthSyst, paramSyst in tthSysts.items():
      name='CMS_hgg_%s'%paramSyst
      allSystList.append(name)
      if (not options.justThisSyst=="") :
          if (not options.justThisSyst==name): continue
      outFile.write('%-35s   lnN   '%(name))
      for c in options.cats:
        for p in options.procs:
          if '%s:%s'%(p,c) in options.toSkip: continue
          if p in bkgProcs or ('pdfWeight' in tthSyst and (p!='ggH' and p!='qqH')):
            outFile.write('- ')
          elif c not in tthCats:
            outFile.write('- ')
          else:
            outFile.write(getFlashggLine(p,c,tthSyst))
      outFile.write('\n')
  outFile.write('\n')

def printSimpleTHSysts():
  for systName, systVal in ggHforttHSysts.items():
    outFile.write('%-35s   lnN   '%systName)
    for c in options.cats:
      for p in options.procs:
        if '%s:%s'%(p,c) in options.toSkip: 
          outFile.write('- ')
          continue
        if p=='ggH' and c in tthCats:
          outFile.write('%6.4f/%6.4f '%(1.-systVal,1.+systVal))
        else:
          outFile.write('- ')
          continue
    outFile.write('\n')
###############################################################################

###############################################################################
##  DISCRETE SYST LINES TOOLS #################################################
###############################################################################
def printMultiPdf():
  if options.isMultiPdf:
    for c in options.cats:
      mPdfOutputString = 'pdfindex_%s_%dTeV  discrete\n'%(c,sqrts)
      if '16' in mPdfOutputString.split('_'):
         mPdfOutputString = mPdfOutputString.replace('_16_13TeV', '_16_16_13TeV')
      elif '17' in mPdfOutputString.split('_'):
         mPdfOutputString = mPdfOutputString.replace('_17_13TeV', '_17_17_13TeV')
      elif '18' in mPdfOutputString.split('_'):
         mPdfOutputString = mPdfOutputString.replace('_18_13TeV', '_18_18_13TeV')
      # elif 'ALL' in mPdfOutputString.split('_') and 'wUL17' in mPdfOutputString.split('_'):
      #    mPdfOutputString = mPdfOutputString.replace('_ALL_wUL17_13TeV', '_ALL_wUL17_ALL_wUL17_13TeV')
      outFile.write(mPdfOutputString)

def printRateParams():
   yr = ['16', '17', '18']
   xs = {}
   for i,f in enumerate(options.rateFiles):
      if yr[i] not in f:
         raise ValueError("Wrong order of rate files")
      xs[yr[i]] = pkl.load(open(f,'rb'))

   rateParams = {}
   for year in ['16', '17']:
      rateParams[year] = xs['18'][:,1]/xs[year][:,1]
   outFile.write('\n')
   procsHere = [pr for pr in options.procs if pr != 'OutsideAcceptance' and pr != 'bkg_mass']
   for j, p in enumerate(procsHere):
      outFile.write('rate{0}_16 rateParam *_16_13TeV {0} {1}\n'.format(p, rateParams['16'][j]))
      if rateParams['16'][j] > 1.3 or rateParams['16'][j] < 0.7:
         print("WARNING: RateParam for process {0} in 2016 is far away from 1: {1}".format(p, rateParams['16'][j]))
      outFile.write('nuisance edit freeze rate{0}_16\n'.format(p))
      outFile.write('rate{0}_17 rateParam *_17_13TeV {0} {1}\n'.format(p, rateParams['17'][j]))
      if rateParams['17'][j] > 1.3 or rateParams['17'][j] < 0.7:
         print("WARNING: RateParam for process {0} in 2017 is far away from 1: {1}".format(p, rateParams['17'][j]))
      outFile.write('nuisance edit freeze rate{0}_17\n'.format(p))
   outFile.write('\n')
###############################################################################

###############################################################################
## MAIN #######################################################################
###############################################################################
# __main__ here
#preamble


print "JustThisSyst == " , options.justThisSyst
if ((options.justThisSyst== "batch_split") or options.justThisSyst==""):
  printPreamble()
  #shape systematic files
  printFileOptions()
  #obs proc/tag bins
  printObsProcBinLines()
  #nuisance param systematics
  printMultiPdf()
#  printBRSyst()
  #printTrigSyst() # now a weight in the main roodataset!
  if not options.statonly:
     # printSimpleTTHSysts()
     printNuisParams()
if not options.statonly:
   if (len(tthCats) > 0 ):  printTTHSysts()
   nominalDFDict = getNominalDFs()
   printTheorySysts()
   printLumiSyst()
   # lnN systematics
   printFlashggSysts()
   printJECSysts()
   if options.differential and options.fullRunII:
      printRateParams()
   #catgeory migrations
   #if (len(dijetCats) > 0 and len(tthCats)>0):  printVbfSysts()
   if (len(dijetCats) > 0 ):  printVbfSysts()
   #other 
   #printLepSysts() #obsolete

   print "################## all sys list #######################"
   print allSystList
###   print "procs :" , ",".join(flashggProc[p] for p in options.procs).replace("bkg_mass","")
   print "procs :" , ",".join(p for p in options.procs).replace("bkg_mass","")
   print "tags : " , ",".join(options.cats)
   print "smears ", ",".join(options.photonCatSmears)
if options.submitSelf:
  counter=0
  os.system('mkdir -p jobs ')
  os.system('rm jobs/* ')
  for syst in allSystList:
    fname='%s/sub%d.sh'%("jobs",counter)
    f = open(fname ,'w')
    os.system('chmod +x %s'%f.name)
    counter=counter+1
    f.write('\#!/bin/bash\n')
    f.write('touch %s.run\n'%os.path.abspath(f.name))
    f.write('cd %s\n'%os.getcwd())
    f.write('eval `scramv1 runtime -sh`\n')
    execLine = '$CMSSW_BASE/src/flashggFinalFit/Datacard/makeParametricModelDatacardFLASHgg.py -i %s -o %s -p %s -c %s --photonCatScales %s --photonCatSmears %s --isMultiPdf --mass %d --justThisSyst %s'%(options.infilename,"jobs/"+options.outfilename+"."+syst,",".join(flashggProc[p] for p in options.procs).replace(",bkg_mass",""),",".join(options.cats),",".join(options.photonCatScales),",".join(options.photonCatSmears),options.mass,syst  )
    f.write('if (%s) then \n'%execLine);
    f.write('\t touch %s.done\n'%os.path.abspath(f.name))
    f.write('else\n')
    f.write('\t touch %s.fail\n'%os.path.abspath(f.name))
    f.write('fi\n')
    f.write('rm -f %s.run\n'%os.path.abspath(f.name))
    print "[SUBMITTING] ",execLine
    f.close()
    os.system('rm -f %s.done'%os.path.abspath(f.name))
    os.system('rm -f %s.fail'%os.path.abspath(f.name))
    os.system('rm -f %s.log'%os.path.abspath(f.name))
    os.system('rm -f %s.err'%os.path.abspath(f.name))
    os.system('qsub -q %s -o %s.log %s'%("hepmedium.q",os.path.abspath(f.name),os.path.abspath(f.name)))
  continueLoop=1;
  while( continueLoop):
    os.system('sleep 10') 
    os.system('qstat') 
    os.system('qstat >out.txt') 
    if( os.stat('out.txt').st_size==0) :continueLoop=0;
  
  print "All done, now just do :"
  print ('cat jobs/%s* >> %s'%(options.outfilename,options.outfilename)) 
  # os.system("cat jobs/%s* >> %s"%(options.outfilename,options.outfilename)) 
  #print ('mv %s.tmp %s'%(options.outfilename,options.outfilename)) 
  #os.system('mv %s.tmp %s'%(options.outfilename,options.outfilename)) 
###############################################################################
#import time
#if options.submitSelf:
#   
#  BJOBS= len([name for name in os.listdir('.') if (os.path.isfile(name)] and (not ".run" in name) and ))
#  RUN= len([name for name in os.listdir('.') if (os.path.isfile(name)] and ".run" in name))
#  while RUN :
#  
#  RUN= len([name for name in os.listdir('.') if (os.path.isfile(name)] and ".run" in name))
#  DONE= len([name for name in os.listdir('.') if (os.path.isfile(name)] and ".done" in name))
#  FAIL= len([name for name in os.listdir('.') if (os.path.isfile(name)] and ".fail" in name))
#  time.sleep(10s)

