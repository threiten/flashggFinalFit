#!/usr/bin/env python2.7
###!/usr/bin/env python
# 
# --------------------------------------------
# Standard python import
from optparse import OptionParser, make_option
import fnmatch, glob, os, sys, json, itertools, array
import re
#sys.argv.append( '-b' )

## ------------------------------------------------------------------------------------------------------------------------------------------------------


#from templates_maker import buildRooDataSet
import ROOT
from ROOT import TH2D, TH1D, TFile, TProfile, TCanvas, TGraphAsymmErrors
from ROOT import RooWorkspace
from ROOT import RooAbsData
from ROOT import RooDataSet
from ROOT import *

gSystem.AddIncludePath("$CMSSW_BASE/src/flashggFinalFit/Signal/interface/")

gSystem.Load("/mnt/t3nfs01/data01/shome/threiten/Hgg/Differentials/CMSSW_7_4_7/src/flashggFinalFit/Signal/src/DataSetFiller_vec_cc.so")
gSystem.Load("/mnt/t3nfs01/data01/shome/threiten/Hgg/Differentials/CMSSW_7_4_7/src/flashggFinalFit/Signal/Phi12_C.so")
gSystem.Load("/mnt/t3nfs01/data01/shome/threiten/Hgg/Differentials/CMSSW_7_4_7/src/flashggFinalFit/Signal/DeltaPhi_C.so")
#gROOT.ProcessLine('#include "flashgg/Systematics/macros/DataSetFiller.h"')


from ROOT import gROOT
gROOT.ForceStyle()
gStyle.SetOptTitle(0)
gStyle.SetOptStat(0)

class Load:
    def __call__(self,option, opt_str, value, parser, *args, **kwargs):
        if option.dest == "__opts__":
            dest = parser.values
        else:
            dest = getattr(parser.values,option.dest)
        origin = getattr(parser.values,"%s_src" % option.dest,None)
        if origin:
            origin += ",%s" % value
        else:
            origin = value
            setattr(parser.values,"%s_src" % option.dest,origin)

        if type(dest) == dict:
            setter = dict.__setitem__
            getter = dict.get
        else:
            setter = setattr
            getter = getattr
        
        for cfg in value.split(","):
            cf = open(cfg)
            settings = json.loads(cf.read())
            for k,v in settings.iteritems():
                print k,v
                attr  = getter(dest,k,None)
                if attr and type(attr) == list:           
                    attr.extend(v)
                setter(dest,k,v)
            cf.close()



#"variables": [
#        ("genPt","recoPt",[0.0,100.0,200.0,300.0,400.0,500.0,600.0,700.0,10000.0],[0.0,10000.0]), 
#        ("genNjets2p5","recoNjets2p5",[-0.5,0.5,1.5,2.5,3.5,4.5,5.5,6.5,100.0],[]) ],




def getSystLabels(isMET):
    phosystlabels=[]
    jetsystlabels=[]
    metsystlabels=[]
    systlabels=[]
    for direction in ["Up","Down"]:
        phosystlabels.append("MvaShift%s01sigma" % direction)
        phosystlabels.append("SigmaEOverEShift%s01sigma" % direction)
#        phosystlabels.append("MaterialCentralBarrel%s01sigma" % direction)
#        phosystlabels.append("MaterialOuterBarrel%s01sigma" % direction)
#        phosystlabels.append("MaterialForward%s01sigma" % direction)
#        phosystlabels.append("FNUFEB%s01sigma" % direction)
#        phosystlabels.append("FNUFEE%s01sigma" % direction)
        phosystlabels.append("MCScaleGain6EB%s01sigma" % direction)
        phosystlabels.append("MCScaleGain1EB%s01sigma" % direction)
        jetsystlabels.append("JEC%s01sigma" % direction)
        jetsystlabels.append("JER%s01sigma" % direction)
        jetsystlabels.append("PUJIDShift%s01sigma" % direction)
        metsystlabels.append("metJecUncertainty%s01sigma" % direction)
        metsystlabels.append("metJerUncertainty%s01sigma" % direction)
        metsystlabels.append("metPhoUncertainty%s01sigma" % direction)
        metsystlabels.append("metUncUncertainty%s01sigma" % direction)
        for r9 in ["HighR9","LowR9"]:
            for region in ["EB","EE"]:
                phosystlabels.append("ShowerShape%s%s%s01sigma"%(r9,region,direction))
                phosystlabels.append("MCScale%s%s%s01sigma" % (r9,region,direction))
                for var in ["Rho","Phi"]:
                    phosystlabels.append("MCSmear%s%s%s%s01sigma" % (r9,region,var,direction))
    systlabels += phosystlabels
    systlabels += jetsystlabels
    if isMET:
        systlabels += metsystlabels

    return systlabels

#variables=[("leadGenPt", "subleadGenPt", [0.,100.,200.,300.,400.,500.,600.,700.,10000.])]
variables=[]
#[["genPt", "recoPt", [0.,100.,200.,300.,400.,500.,600.,700.,10000.],[0.,10000.]], ["genNjets2p5", "recoNjets2p5", [-0.5,0.5,1.5,2.5,3.5,4.5,5.5,6.5,100.],[]] ]
procs=[]
#procs=["InsideAcceptance_130_13TeV"]
cats=[]
#cats=["SigmaMpTTag_0", "SigmaMpTTag_1", "SigmaMpTTag_2"]

import numpy as np


def setAliases(tree):
    tree.SetAlias("myGenNjets2p5", "(genJet2p5Numberofdaughters0 > 5) + (genJet2p5Numberofdaughters1 > 5) + (genJet2p5Numberofdaughters2 > 5) + (genJet2p5Numberofdaughters3 > 5) + (genJet2p5Numberofdaughters4 > 5) + (genJet2p5Numberofdaughters5 > 5)")
    tree.SetAlias("recoMjjEta2p5", "(recoJet2p5Energy0!=-999 && recoJet2p5Energy1!=-999)*(sqrt((recoJet2p5Energy0 + recoJet2p5Energy1)**2 - (recoJet2p5Px0 + recoJet2p5Px1)**2 - (recoJet2p5Py0 + recoJet2p5Py1)**2 - (recoJet2p5Pz0 + recoJet2p5Pz1)**2)) + (recoJet2p5Energy0==-999 || recoJet2p5Energy1==-999)*(-999)")
    tree.SetAlias("genMjjEta2p5", "(genJet2p5Energy0!=-999 && genJet2p5Energy1!=-999)*(sqrt((genJet2p5Energy0 + genJet2p5Energy1)**2 - (genJet2p5Px0 + genJet2p5Px1)**2 - (genJet2p5Py0 + genJet2p5Py1)**2 - (genJet2p5Pz0 + genJet2p5Pz1)**2)) + (genJet2p5Energy0==-999 || genJet2p5Energy1==-999)*(-999)")
    tree.SetAlias("recoMjjEta4p7", "(recoJet4p7Energy0!=-999 && recoJet4p7Energy1!=-999)*(sqrt((recoJet4p7Energy0 + recoJet4p7Energy1)**2 - (recoJet4p7Px0 + recoJet4p7Px1)**2 - (recoJet4p7Py0 + recoJet4p7Py1)**2 - (recoJet4p7Pz0 + recoJet4p7Pz1)**2)) + (recoJet4p7Energy0==-999 || recoJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("genMjjEta4p7", "(genJet4p7Energy0!=-999 && genJet4p7Energy1!=-999)*(sqrt((genJet4p7Energy0 + genJet4p7Energy1)**2 - (genJet4p7Px0 + genJet4p7Px1)**2 - (genJet4p7Py0 + genJet4p7Py1)**2 - (genJet4p7Pz0 + genJet4p7Pz1)**2)) + (genJet4p7Energy0==-999 || genJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("recoAbsRapidity", "(recoRapidity>=0)*(recoRapidity) +  (recoRapidity<0)*(-recoRapidity)")
    tree.SetAlias("genAbsRapidity", "(genRapidity>=0)*(genRapidity) +  (genRapidity<0)*(-genRapidity)")
    tree.SetAlias("recoAbsDeltaRapidityGgJet0Eta2p5","(recoJet2p5Rapidity0 != -999)*(abs(recoRapidity - recoJet2p5Rapidity0)) + (recoJet2p5Rapidity0==-999)*(-999)")
    tree.SetAlias("genAbsDeltaRapidityGgJet0Eta2p5","(genJet2p5Rapidity0 != -999)*(abs(genRapidity - genJet2p5Rapidity0)) + (genJet2p5Rapidity0==-999)*(-999)")
    tree.SetAlias("recoAbsDeltaEtaJJEta2p5","(recoJet2p5Energy0!=-999 && recoJet2p5Energy1!=-999)*(abs(recoJet2p5Eta0 - recoJet2p5Eta1)) + (recoJet2p5Energy0==-999 || recoJet2p5Energy1==-999)*(-999)")
    tree.SetAlias("genAbsDeltaEtaJJEta2p5","(genJet2p5Energy0!=-999 && genJet2p5Energy1!=-999)*(abs(genJet2p5Eta0 - genJet2p5Eta1)) + (genJet2p5Energy0==-999 || genJet2p5Energy1==-999)*(-999)")
    tree.SetAlias("recoAbsDeltaEtaJJEta4p7","(recoJet4p7Energy0!=-999 && recoJet4p7Energy1!=-999)*(abs(recoJet4p7Eta0 - recoJet4p7Eta1)) + (recoJet4p7Energy0==-999 || recoJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("genAbsDeltaEtaJJEta4p7","(genJet4p7Energy0!=-999 && genJet4p7Energy1!=-999)*(abs(genJet4p7Eta0 - genJet4p7Eta1)) + (genJet4p7Energy0==-999 || genJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("recoAbsDeltaPhiGgJet0Eta2p5","(recoJet2p5Phi0 != -999)*(abs(DeltaPhi(recoPhi , recoJet2p5Phi0))) + (recoJet2p5Phi0==-999)*(-999)")
    tree.SetAlias("genAbsDeltaPhiGgJet0Eta2p5","(genJet2p5Phi0 != -999)*(abs(DeltaPhi(genPhi , genJet2p5Phi0))) + (genJet2p5Phi0==-999)*(-999)")
    tree.SetAlias("recoZeppenfeldEta4p7", "(recoJet4p7Energy0!=-999 && recoJet4p7Energy1!=-999)*(abs(recoEta - 0.5*(recoJet4p7Eta0+recoJet4p7Eta1))) + (recoJet4p7Energy0==-999 || recoJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("genZeppenfeldEta4p7", "(genJet4p7Energy0!=-999 && genJet4p7Energy1!=-999)*(abs(genEta - 0.5*(genJet4p7Eta0+genJet4p7Eta1))) + (genJet4p7Energy0==-999 || genJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("recoCosThetaStar", "abs( ( (recoLeadEnergy + recoLeadPz)*(recoSubleadEnergy - recoSubleadPz) - (recoLeadEnergy - recoLeadPz)*(recoSubleadEnergy + recoSubleadPz) )/( recoMass*sqrt(recoMass*recoMass + recoPt*recoPt) ) )")
    tree.SetAlias("genCosThetaStar", "abs( ( (genLeadEnergy + genLeadPz)*(genSubleadEnergy - genSubleadPz) - (genLeadEnergy - genLeadPz)*(genSubleadEnergy + genSubleadPz) )/( genMass*sqrt(genMass*genMass + genPt*genPt) ) )")
    
    tree.SetAlias("recoAbsDeltaPhiGgJjEta4p7","(recoJet4p7Energy0!=-999 && recoJet4p7Energy1!=-999)*abs( DeltaPhi( recoPhi, Phi12(recoJet4p7Px0,recoJet4p7Py0, recoJet4p7Px1,recoJet4p7Py1) ) )   + (recoJet4p7Energy0==-999 || recoJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("genAbsDeltaPhiGgJjEta4p7","(genJet4p7Energy0!=-999 && genJet4p7Energy1!=-999)*abs( DeltaPhi( genPhi, Phi12(genJet4p7Px0,genJet4p7Py0, genJet4p7Px1,genJet4p7Py1) ) )   + (genJet4p7Energy0==-999 || genJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("recoAbsDeltaPhiJjEta4p7","(recoJet4p7Energy0!=-999 && recoJet4p7Energy1!=-999)*abs( DeltaPhi( recoJet4p7Phi0, recoJet4p7Phi1 ) )   + (recoJet4p7Energy0==-999 || recoJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("genAbsDeltaPhiJjEta4p7","(genJet4p7Energy0!=-999 && genJet4p7Energy1!=-999)*abs( DeltaPhi( genJet4p7Phi0, genJet4p7Phi1 ) )   + (genJet4p7Energy0==-999 || genJet4p7Energy1==-999)*(-999)")
    tree.SetAlias("recoJet2p5AbsRapidity0", "(recoJet2p5Rapidity0!=-999)*abs(recoJet2p5Rapidity0) + (recoJet2p5Rapidity0==-999)*(-999)")
    tree.SetAlias("genJet2p5AbsRapidity0", "(genJet2p5Rapidity0!=-999)*abs(genJet2p5Rapidity0) + (genJet2p5Rapidity0==-999)*(-999)")
    
    tree.SetAlias("recoJet4p7AbsRapidity1", "(recoJet4p7Rapidity1!=-999)*abs(recoJet4p7Rapidity1) + (recoJet4p7Rapidity1==-999)*(-999)")
    tree.SetAlias("genJet4p7AbsRapidity1", "(genJet4p7Rapidity1!=-999)*abs(genJet4p7Rapidity1) + (genJet4p7Rapidity1==-999)*(-999)")
    
    tree.SetAlias("recoPtNjets2p5","13000*min(2,recoNjets2p5) + recoPt")
    tree.SetAlias("genPtNjets2p5","13000*min(2,genNjets2p5) + genPt")
    
    tree.SetAlias("recoAbsDeltaPhiJjEta4p7VBFlike","(recoJet4p7Energy0!=-999 && recoJet4p7Energy1!=-999 && recoAbsDeltaEtaJJEta4p7 >=3.5 && recoMjjEta4p7>=200)*abs( DeltaPhi( recoJet4p7Phi0, recoJet4p7Phi1 ) )   + (recoJet4p7Energy0==-999 || recoJet4p7Energy1==-999 || recoAbsDeltaEtaJJEta4p7 <3.5 || recoMjjEta4p7<200)*(-999)")
    tree.SetAlias("genAbsDeltaPhiJjEta4p7VBFlike","(genJet4p7Energy0!=-999 && genJet4p7Energy1!=-999 && genAbsDeltaEtaJJEta4p7 >=3.5 && genMjjEta4p7>=200)*abs( DeltaPhi( genJet4p7Phi0, genJet4p7Phi1 ) )   + (genJet4p7Energy0==-999 || genJet4p7Energy1==-999 || genAbsDeltaEtaJJEta4p7 <3.5 || genMjjEta4p7<200)*(-999)")
    
    tree.SetAlias("recoAbsDeltaPhiGgJjEta4p7VBFlike","(recoJet4p7Energy0!=-999 && recoJet4p7Energy1!=-999 && recoAbsDeltaEtaJJEta4p7 >=3.5 && recoMjjEta4p7>=200)*abs( DeltaPhi( recoPhi, Phi12(recoJet4p7Px0,recoJet4p7Py0, recoJet4p7Px1,recoJet4p7Py1) ) )   + (recoJet4p7Energy0==-999 || recoJet4p7Energy1==-999 || recoAbsDeltaEtaJJEta4p7 <3.5 || recoMjjEta4p7<200)*(-999)")
    tree.SetAlias("genAbsDeltaPhiGgJjEta4p7VBFlike","(genJet4p7Energy0!=-999 && genJet4p7Energy1!=-999 && genAbsDeltaEtaJJEta4p7 >=3.5 && genMjjEta4p7>=200)*abs( DeltaPhi( genPhi, Phi12(genJet4p7Px0,genJet4p7Py0, genJet4p7Px1,genJet4p7Py1) ) )   + (genJet4p7Energy0==-999 || genJet4p7Energy1==-999 || genAbsDeltaEtaJJEta4p7 <3.5 || genMjjEta4p7<200)*(-999)")
    
    tree.SetAlias("recoJet4p7Pt1VBFlike", "(recoJet4p7Pt1!=-999 && recoAbsDeltaEtaJJEta4p7 >=3.5 && recoMjjEta4p7>=200)*recoJet4p7Pt1 + (recoJet4p7Pt1==-999 || recoAbsDeltaEtaJJEta4p7 <3.5 || recoMjjEta4p7<200)*(-999)")
    tree.SetAlias("genJet4p7Pt1VBFlike", "(genJet4p7Pt1!=-999 && genAbsDeltaEtaJJEta4p7 >=3.5 && genMjjEta4p7>=200)*genJet4p7Pt1 + (genJet4p7Pt1==-999 || genAbsDeltaEtaJJEta4p7 <3.5 || genMjjEta4p7<200)*(-999)")

#    tree.SetAlias("recoNleptons", "recoNmuonsAll + (abs(recoElectronAllEta0) < 2.4) + (abs(recoElectronAllEta1) < 2.4) + (abs(recoElectronAllEta2) < 2.4) + (abs(recoElectronAllEta3) < 2.4) ")
    tree.SetAlias("recoNleptons", "(recoNmuonsAll + recoNelectronsAll) ")
#    tree.SetAlias("genNleptons", "(abs(genLeptonAllEta0) < 2.4  && genLeptonAllPt0 > 20.0) + (abs(genLeptonAllEta1) < 2.4  && genLeptonAllPt1 > 20.0) + (abs(genLeptonAllEta2) < 2.4  && genLeptonAllPt2 > 20.0) + (abs(genLeptonAllEta2) < 2.4  && genLeptonAllPt2 > 20.0) + (abs(genLeptonAllEta3) < 2.4  && genLeptonAllPt3 > 20.0)")
    tree.SetAlias("genNleptons", "(genNleptons2p4)")

    tree.SetAlias("recoMET", "recoMETAllPt")
    tree.SetAlias("genMET", "genMETall")

    tree.SetAlias("reco1LeptonHighMET", "(-999)*(!(recoNleptons==1 && recoMET > 100.0)) + (1)*(recoNleptons==1 && recoMET > 100.0)")
    tree.SetAlias("gen1LeptonHighMET", "(-999)*(!(genNleptons==1 && genMET > 100.0)) + (1)*(genNleptons==1 && genMET > 100.0)")

    tree.SetAlias("reco1LeptonLowMET", "(-999)*(!(recoNleptons==1 && recoMET < 100.0)) + (1)*(recoNleptons==1 && recoMET < 100.0)")
    tree.SetAlias("gen1LeptonLowMET", "(-999)*(!(genNleptons==1 && genMET < 100.0)) + (1)*(genNleptons==1 && genMET < 100.0)")

    tree.SetAlias("reco1Lepton1Bjet", "(-999)*(!(recoNleptons>0 && recoNjetsBflavorTight2p5>0)) + (1)*(recoNleptons>0 && recoNjetsBflavorTight2p5>0)")
    tree.SetAlias("gen1Lepton1Bjet", "(-999)*(!(genNleptons>0 && genNjetsBflavor2p5>0)) + (1)*(genNleptons>0 && genNjetsBflavor2p5>0)")

    tree.SetAlias("genNjetsBflavorTight4p7", "genNjetsBflavor4p7")
    tree.SetAlias("recoNjetsBflavorTight4p7", "recoNjetsBflavorTight4p7")

    tree.SetAlias("genNjetsBflavorTight2p5", "genNjetsBflavor2p5")
    tree.SetAlias("recoNjetsBflavorTight2p5", "recoNjetsBflavorTight2p5")

    tree.SetAlias("genInclusive", "(genPt>-1)")
    tree.SetAlias("recoInclusive", "(recoPt>-1)")

    
#    tree.SetAlias

def reduceTrees(label, proc, cat, infile, variable, ralist_nominal, ralist_syst):
##    if options.label:
##        if label == "pdfWeights":
##            systlabels=[]
##        elif label == "nominal":
##            systlabels=[""]
##        else:
##            systlabels = [label]
##    for label in systlabels:
##        for proc in procs:
##            print "proc ",proc
##            for cat in cats:
##                print "cat ",cat

    datasetsReduced={}
    tempDatasetsReduced={}
    if label == "" or label == "pdfWeights":
        if options.verbose:
            print "trying to read the following tree"
            print "tagsDumper/trees/"+str(proc)+"_"+str(cat)
        tree = infile.Get( "tagsDumper/trees/"+str(proc)+"_"+str(cat) )
        setAliases(tree)

        tree.Print()
    else:
        if options.verbose:
            print "trying to read the following tree"
            print "tagsDumper/trees/"+str(proc)+"_"+str(cat)+"_"+str(label)
        tree = infile.Get( "tagsDumper/trees/"+str(proc)+"_"+str(cat)+"_"+str(label) )
        tree.Print()
        setAliases(tree)

    boundaries = []
    boundaries.append( variable[-2] )
    boundaries.append( variable[-1] )
    if options.verbose:
        print "root hasattr before"
        print hasattr(ROOT, "vecBoundaries")
    if not hasattr(ROOT, "vecBoundaries"):
        gROOT.ProcessLine(" std::vector<std::vector<double > >* vecBoundaries = new std::vector<std::vector<double > >() ;")
    else:
        ROOT.vecBoundaries.clear()
    if options.verbose:
        print "root hasattr after"
        print hasattr(ROOT, "vecBoundaries")
    if not hasattr(ROOT, "tempvec"):
        gROOT.ProcessLine("std::vector<double >* tempvec = new std::vector<double >() ;")
    else:
        ROOT.tempvec.clear()
    for b in variable[-1]:
        ROOT.tempvec.push_back(b)
    ROOT.vecBoundaries.push_back(tempvec)
    ROOT.tempvec.clear()
    for b in variable[-2]:
        ROOT.tempvec.push_back(b)
    ROOT.vecBoundaries.push_back(tempvec)
    if label == "" or label == "pdfWeights":
        filler = DataSetFiller(str(proc), str(cat), ralist_nominal, "weight", False, True)
    else:
        filler = DataSetFiller(str(proc), str(cat)+"_"+str(label), ralist_syst, "weight", False, True)           
    wei = TCut("weight")
    if options.cut:
        wei *= TCut(str(options.cut))
    if not hasattr(ROOT, "obsnames"):
        gROOT.ProcessLine("std::vector<std::string >* obsnames = new std::vector<std::string >() ;")
    else:
        ROOT.obsnames.clear()
    ROOT.obsnames.push_back( str( variable[1] ) ) #first reco
    ROOT.obsnames.push_back( str( variable[0] ) ) #then gen
    if options.verbose:
        print obsnames
    filler.setGrid(ROOT.obsnames, vecBoundaries )
    filler.fillFromTree(tree,wei.GetTitle())
    if not hasattr(ROOT, "datasets"):
        gROOT.ProcessLine("std::vector<RooDataSet* >* datasets = new std::vector<RooDataSet* >() ;")
    else:
        ROOT.datasets.clear()
    ROOT.datasets = filler.get()
    for ids in range( ROOT.datasets.size() ):
        if label=="":
            datasetsReduced[ ROOT.datasets[ids].GetName() ] = ( ROOT.datasets[ids] )
        elif label=="pdfWeights":
            print "pushing back for pdfw in tempDatasets"
            tempDatasetsReduced[ ROOT.datasets[ids].GetName() ] = ( ROOT.datasets[ids] )
        else:
            datasetsReduced[ ROOT.datasets[ids].GetName() ] = ( ROOT.datasets[ids] ).binnedClone(str(  ROOT.datasets[ids].GetName()  ))
            datasetsReduced[ ROOT.datasets[ids].GetName() ].SetTitle( ROOT.datasets[ids].GetName() )


##        for proc in procs:
##            print "proc ",proc
##            for cat in cats:
##                print "cat ",cat

    print tempDatasetsReduced.keys()
    if label == "pdfWeights":
        rrv_pdfWeights={}
        for iw in range(60):
            rrv_pdfw = RooRealVar("pdfWeight_"+str(iw), "weight*pdfWeights["+str(iw)+"]/pdfWeights[0]", -float("inf"), float("inf"))
            rrv_pdfWeights[rrv_pdfw.GetName()] = (rrv_pdfw)
        rrv_cms_mass = RooRealVar("CMS_hgg_mass", "CMS_hgg_mass", 100, 180)
        rrv_cms_mass.setBins(160)
        rrv_weight = RooRealVar("weight", "weight", -float("inf"), float("inf"))
        rooarglist_pdfw=RooArgList(rrv_cms_mass,rrv_weight)
        for irrv in rrv_pdfWeights.values():
            rooarglist_pdfw.add(irrv)

        tree = infile.Get( "tagsDumper/trees/"+str(proc)+"_"+str(cat) )
        boundaries = []
        boundaries.append( variable[-2] )
        boundaries.append( variable[-1] )
        if options.verbose:
            print "pdfweights root hasattr before"
            print hasattr(ROOT, "vecBoundaries")
        if not hasattr(ROOT, "vecBoundaries"):
            gROOT.ProcessLine(" std::vector<std::vector<double > >* vecBoundaries = new std::vector<std::vector<double > >() ;")
        else:
            ROOT.vecBoundaries.clear()
        if options.verbose:
            print "pdfweights root hasattr after"
            print hasattr(ROOT, "vecBoundaries")
        if not hasattr(ROOT, "tempvec"):
            gROOT.ProcessLine("std::vector<double >* tempvec = new std::vector<double >() ;")
        else:
            ROOT.tempvec.clear()
        for b in variable[-1]:
            ROOT.tempvec.push_back(b)
        ROOT.vecBoundaries.push_back(tempvec)
        ROOT.tempvec.clear()
        for b in variable[-2]:
            ROOT.tempvec.push_back(b)
        ROOT.vecBoundaries.push_back(tempvec)
        
        filler = DataSetFiller(str(proc), str(cat)+"_pdfWeights", rooarglist_pdfw, "weight", False)
        wei = TCut("1")
        if options.cut:
            wei *= TCut(str(options.cut))
        if not hasattr(ROOT, "obsnames"):
            gROOT.ProcessLine("std::vector<std::string >* obsnames = new std::vector<std::string >() ;")
        else:
            ROOT.obsnames.clear()
        ROOT.obsnames.push_back( str( variable[1] ) )
        ROOT.obsnames.push_back( str( variable[0] ) )
        if options.verbose:
            print obsnames
        filler.setGrid(ROOT.obsnames, vecBoundaries )
        filler.fillFromTree(tree,wei.GetTitle())
        if not hasattr(ROOT, "datasets"):
            gROOT.ProcessLine("std::vector<RooDataSet* >* datasets = new std::vector<RooDataSet* >() ;")
        else:
            ROOT.datasets.clear()
        ROOT.datasets = filler.get()


        rrv_sumentries = RooRealVar("sumW","sumW", 0)                            
        for ids in range( ROOT.datasets.size() ):
            ds = ( ROOT.datasets[ids] )
            totpdfweights={}
            ##initialise keys and values of pdfweights dict (so we don't have to check it at each iteration)

            iset=ds.get()
            if options.verbose:
                iset.Print()
                print iset
            iteriset = iset.createIterator()
            iwvar = iteriset.Next()
            while iwvar:
                totpdfweights[iwvar.GetName()] = 0.
                iwvar = iteriset.Next()

                #now loop through the entries and sum    
            for ientry in range(ds.numEntries()):
                iset=ds.get(ientry)
                iteriset = iset.createIterator()
                iwvar = iteriset.Next()
                while iwvar:
                    totpdfweights[iwvar.GetName()] = totpdfweights[iwvar.GetName()] + iset.getRealValue(iwvar.GetName())
                    iwvar = iteriset.Next()
            for ikey in totpdfweights.keys():
                if ikey=='CMS_hgg_mass':
                    if options.verbose:
                        print "we found ",ikey
                        print "so we continue"
                    continue
                if tempDatasetsReduced[str( ds.GetName() ).replace("_pdfWeights","")].sumEntries() != 0:
                    totpdfweights[ikey] = totpdfweights[ikey]/tempDatasetsReduced[str( ds.GetName() ).replace("_pdfWeights","")].sumEntries()
                else:
                    totpdfweights[ikey]=0.
                rrv_pdfWeights[ikey].setVal(totpdfweights[ikey])
#                        ds.var(str(ikey)).setVal(totpdfweights[ikey])
            rooarglist_pdfw_final=RooArgSet()
            for irrv in rrv_pdfWeights.values():
                rooarglist_pdfw_final.add(irrv)
            rooarglist_pdfw_final.add(rrv_sumentries)

            rrv_sumentries.setVal(tempDatasetsReduced[str( ds.GetName() ).replace("_pdfWeights","")].sumEntries())
            ds_final = RooDataSet(str( ds.GetName() ), str( ds.GetName() ), rooarglist_pdfw_final, rrv_sumentries.GetName())
            ds_final.add( rooarglist_pdfw_final, rrv_sumentries.getVal() )
            if options.verbose:
                ds.Print("V")
                ds_final.Print("V")
            datasetsReduced[ds_final.GetName()]=ds_final
    return datasetsReduced





savefmts=['.png','.root','.pdf','.jpg']
# Main routine
def main(o,args):
    variables = options.variables
    procs = options.procs
    cats = options.cats
    print "****variables***"
    print variables
    #from configfile import variables, procs, cats
    if 0 and options.files.startswith("/store"):
        filepath = "root://eoscms/"+str(options.files)
    else:
        filepath = options.files
    print 'filepath is '+str(filepath)
    infile = TFile.Open(filepath)
    infile.Print()
    wspace =  RooWorkspace("cms_hgg_13TeV")
###    tree = infile.Get("tagsDumper/cms_hgg_13TeV")
    wspace.Print()

    variables_0=[]    
    variables_1=[]    
    isData=False
    isMET=False
    for ivarset in range(len(variables)):
        isData = any("data" in bla.lower() for bla in procs)
        isMET = any("MET" in bla for bla in procs)
        varset = variables[ivarset]
        print 'we look at the variable ...'
        print variables[ivarset]
        if not variables[ivarset][-2] and (variables[ivarset][0] and variables[ivarset][1] ):
            variables[ivarset] = (variables[ivarset][0], variables[ivarset][1], variables[ivarset][-1], variables[ivarset][-1])
#        if not variables[ivarset][1]:
#            variables[ivarset] = (variables[ivarset][0], variables[ivarset][0], variables[ivarset][2], [1,1])
#            isData=True
        variables_0.append(variables[ivarset][0])
        variables_1.append(variables[ivarset][1])

        proc = procs[0]
        cat = cats[0]
        print "trying to read.."
        print "tagsDumper/trees/"+str(proc)+"_"+str(cat)
        tree = infile.Get( "tagsDumper/trees/"+str(proc)+"_"+str(cat) )

        setAliases(tree)

        varsInTree = []
        branchNames = tree.GetListOfBranches().Clone()
        for i in range( branchNames.GetEntries() ):
            varsInTree.append(branchNames.At(i).GetName())
        print "varsInTree"
        print varsInTree
        #do not dump into ws vars if they are needed only for splitting, or if they are "other" vars not used at the moment (not even for splitting)
        varsToDump = [var for var in varsInTree if (var not in options.othervars and var not in variables_0 and var not in variables_1 and var != "CMS_hgg_mass") ]
#        print "varsToDump"
#        print varsToDump
        #eliminate duplicates from list
        varsToDump = list(set(varsToDump))
        roorealvars=[]
        rooarglist=RooArgList()
        limitedvars=[]
        rrv0=RooRealVar(varsToDump[0],varsToDump[0],-10.,10.)
        rrv1=RooRealVar(varsToDump[1],varsToDump[1],-10.,10.)
        ras = RooArgSet(rrv0,rrv1)
#        print varsToDump[20]
#        print varsToDump[23]
#        del varsToDump[23]
        
        print "varsToDump before for"
        print varsToDump
##        varsToDump = ["weight","dZ", "lumi", "processIndex"]
        varsToDump = ["weight", "lumi", "NNLOPSweight"]
        
        for var in varsToDump:
#            print "print them one by one"
#            print var
#            val = tree.GetBranch(var).GetEntry(0)
#            print type(val)
##            if type(val) is int:
##                print "skipping var ",var
##                continue
            if var in limitedvars:
                rrv = RooRealVar(str(var),limitedvars[var]["min"],limitedvars[var]["max"])
                roorealvars.append(rrv)
                rooarglist.add(rrv)
            else:
                rrv = RooRealVar(str(var), str(var), -float("inf"), float("inf"))
#                rrv = wspace.factory(str(var)+"[0.]")
#                rrv.Print()
#                rrv.SetName(str(var))
#                rrv.SetTitle(str(var))
                roorealvars.append(rrv)
                rooarglist.add(rrv, True)
        datasetsReduced={}
#        print roorealvars
        rooarglist.Print()
        
        systlabels = getSystLabels(isMET)
        rrv_cms_mass = RooRealVar("CMS_hgg_mass", "CMS_hgg_mass", 100, 180)
        rrv_cms_mass.setBins(160)
        ###add mass also to the standard set
        rooarglist.add(rrv_cms_mass, True)
        
        if not isData:
            rrv_dZ = RooRealVar("dZ", "dZ", -25, 27)
            rrv_dZ.setBins(2)
            rooarglist.add(rrv_dZ, True)

            rrv_processIndex = RooRealVar("processIndex", "processIndex", 10, 15)
            rrv_processIndex.setBins(5)
            rooarglist.add(rrv_processIndex, True)



##        rrv_cms_mass.setBins(160)
        rrv_weight = RooRealVar("weight", "weight", -float("inf"), float("inf"))
        rooarglist_syst=RooArgList(rrv_cms_mass,rrv_weight)
        systlabels.append("")

        if options.label == "nominal" and "125" in procs[0]:
#            systweights = ["LooseMvaSF", "PreselSF", "ElectronWeight", "electronVetoSF", "MuonWeight", "TriggerWeight"]
            systweights = ["UnmatchedPUWeight", "MvaLinearSyst", "LooseMvaSF", "PreselSF", "electronVetoSF", "TriggerWeight", "FracRVWeight", "FracRVNvtxWeight", "ElectronWeight", "MuonWeight", "MuonMiniIsoWeight", "JetBTagCutWeight"] #, "JetBTagReshapeWeight"]

            directions = ["Up01sigma", "Down01sigma"]
            for syst in systweights:
                for ddir in directions:
                    rrv = RooRealVar(str(syst)+str(ddir), str(syst)+str(ddir), -1000, 1000)
                    rrv.Print()
                    rooarglist.add(rrv, True)    
                    roorealvars.append(rrv)
            rrv = RooRealVar("centralObjectWeight", "centralObjectWeight", -1000, 1000)
            rooarglist.add(rrv, True)    
            roorealvars.append(rrv)
        rooarglist.Print()
                
        if options.label:
            if options.label == "pdfWeights":
                systlabels=["pdfWeights"]
            elif options.label == "nominal":
                systlabels=[""]
            else:
                systlabels = [options.label]

        for label in systlabels:
            for proc in procs:
                print "proc ",proc
                for cat in cats:
                    print "cat ",cat
		    datasetsReduced.update( reduceTrees(label, proc, cat, infile, variables[ivarset], rooarglist, rooarglist_syst) )
                    
        if options.label == "nominal" and "125" in procs[0]:
            processNames=[]
            categoryNames=[]
            dnames = datasetsReduced.keys()
            for dname in dnames:
                print dname
                gr = re.compile('(_m*\d+p\d*_m*\d+p\d*)').split(dname)
                if len(gr) != 5:
                    print "ERROR! Could not extract proc, cat names from datasets' list"
                    break
                processNames.append( gr[0].replace("125_13TeV_","")+gr[1])
                print "processNames at this iteration"
                print processNames[-1]
                categoryNames.append( gr[2].lstrip("_")+gr[3]+gr[4] )
                print "categoryNames at this iteration"
                print categoryNames[-1]
            procNames_set = set(processNames)    
            catNames_set = set(categoryNames)
            fnames = open("proc_cat_names_"+str(variables[ivarset][1])+".txt", "w")
            fnames.write(",".join(procNames_set)+"\n")
            fnames.write(",".join(catNames_set)+"\n")

        new_wspace = RooWorkspace("cms_hgg_13TeV")
        outfilename = options.outfile.split('.')[0]+'_'+str(variables[ivarset][0])
        if options.label:
            outfilename = outfilename+'_'+options.label
        outfilename = outfilename+'.root'
        outfile = TFile(outfilename, 'RECREATE')
        stepsize=int(len(datasetsReduced.keys())/10)
        if stepsize == 0:
            stepsize=len(datasetsReduced.keys())
        print "stepsize ",stepsize
        iteration = 0
        while(len(datasetsReduced.keys())>0):
            print 'iteration '+str(iteration)
            print "length of datasets is "+str(len(datasetsReduced.keys()))
            iteration=iteration+1
            try: outfile
            except NameError: outfile = TFile(outfilename, 'UPDATE')

            try: new_wspace
            except NameError: 
                outfile.ls()
                new_wspace = outfile.Get("cms_hgg_13TeV")
                print "cms_hgg_13TeV;1"
                gDirectory.Delete("cms_hgg_13TeV;1")
                outfile.ls()
            written=[]
            if stepsize > len(datasetsReduced.keys()):
                stepsize = len(datasetsReduced.keys())
            for ikey in range(stepsize):
                getattr(new_wspace, 'import')(datasetsReduced[  datasetsReduced.keys()[ikey] ], ROOT.RooFit.Rename(  datasetsReduced[  datasetsReduced.keys()[ikey] ].GetName()  )  ) ##since root6, overloaded import methods are ambigous, need a second argument (rename the ds to the name it already has...) to disambiguate
###                datasetsReduced[ datasetsReduced.keys()[ikey]  ].Write()
                written.append( datasetsReduced.keys()[ikey]  )
            print 'Number of data in ws '
            print len(new_wspace.allData())
            new_wspace.Print()
            new_wspace.Write()
            outfile.Write()
            outfile.Close()
            print "written at this round"
            print written
            print "still in the list are "+str(len(datasetsReduced))
            print datasetsReduced
            for wkey in written:
                print wkey
                del datasetsReduced[wkey]
            del new_wspace
            del outfile

    

## ------------------------------------------------------------------------------------------------------------------------------------------------------    
if __name__ == "__main__":
    parser = OptionParser(option_list=[
            make_option("-i", "--indir",
                        action="store", type="string", dest="indir",
                        default="./",
                        help="input directory", metavar="DIR"
                        ),
            make_option("-f", "--files",
                        action="store", type="string", dest="files",
                        default="allSig125IA.root",
                        help="pattern of files to be read", metavar="PATTERN"
                        ), 
            make_option("-t", "--treeName",
                        action="store", type="string", dest="treename",
                        default="TestTree",
                        help="TTree name", metavar="TREENAME"
                        ),
            make_option("-o", "--outfile",
                        action="store", type="string", dest="outfile",
                        default="reduced.root",
                        help="outputfile", metavar="FILE"
                        ),
            make_option("-l", "--label",
                        action="store", type="string", dest="label",
                        default="",
                        help="label", metavar="LABEL"
                        ),
            make_option("-w", "--weightsdir",
                        action="store", type="string", dest="weightsdir",
                        default="weights",
                        help="tmva weights dir", metavar="DIR"
                        ),
#            make_option("-V", "--variables",
#                        action="store", dest="variables", type="string",
#                        default="",
#                        help="list of variables"
#                        ),
            make_option("-T", "--tmvaSettings",
                        action="store", dest="tmvaSettings", type="string",
                        default="dipho.json",
                        help="settings for the TMVA training"
                        ),
            make_option("-v", "--verbose",
                        action="store_true", dest="verbose",
                        default=False,
                        ),
            make_option("-O", "--optimize",
                        action="store_true", dest="optimize",
                        default=False,
                        ),
            make_option("-D", "--outputdir",
                        action="store_true", dest="outdir",
                        default="plots",
                        ),
            make_option("-L", "--logz",
                        action="store_true", dest="logz",
                        default=False,
                        ),
            make_option("-N", "--maxEntries",
                        action="store", type="int", dest="maxEntries",
                        default=-1,
                        ),
            make_option("--load",  # special option to load whole configuaration from JSON
                        action="callback",callback=Load(),dest="__opts__",
                        type="string",
                        help="load JSON file with configuration",metavar="reduce_cfg.json"
                        )
            ])

    (options, args) = parser.parse_args()

    sys.argv.append("-b")
    main(options, args)






