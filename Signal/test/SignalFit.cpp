#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <typeinfo>

#include "TROOT.h"
#include <TStyle.h>
#include "TFile.h"
#include "TMath.h"
#include "TStopwatch.h"
#include "RooWorkspace.h"
#include "RooDataSet.h"
#include "RooDataHist.h"
#include "TKey.h"
#include "TMacro.h"
#include "TClass.h"
#include "TIterator.h"
#include "TRandom3.h"
#include "HiggsAnalysis/CombinedLimit/interface/RooSpline1D.h"

#include "../interface/InitialFit.h"
#include "../interface/LinearInterp.h"
#include "../interface/FinalModelConstruction.h"
#include "../interface/Packager.h"
#include "../interface/WSTFileWrapper.h"

#include "boost/program_options.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/predicate.hpp"

using namespace std;
using namespace RooFit;
using namespace boost;
namespace po = boost::program_options;

typedef map<int,map<string,RooRealVar*> > parlist_t;
typedef map<pair<string,string>, std::pair<parlist_t,parlist_t> > parmap_t;
typedef map<pair<string,string>,map<string,RooSpline1D*> > clonemap_t;

string filenameStr_;
vector<string> filename_;

string outfilename_;
string mergefilename_;
string datfilename_;
string systfilename_;
string plotDir_;
bool skipPlots_=false;
int mhLow_=110;
int mhHigh_=140;
int nCats_;
float constraintValue_;
int constraintValueMass_;
bool spin_=false;
vector<string> procs_;
string procStr_;
bool isCutBased_=false;
bool is2011_=false;
bool is2012_=false;
string massesToSkip_;
vector<int> skipMasses_;
bool splitRVWV_=true;
bool doSecondaryModels_=true;
bool doQuadraticSigmaSum_=false;
bool runInitialFitsOnly_=false;
bool cloneFits_=false;
bool replace_=false;
pair<string,string> replaceWith_rv_;
pair<string,string> replaceWith_wv_;
string cloneFitFile_;
bool recursive_=true;
string highR9cats_;
string lowR9cats_;
int verbose_=0;
int ncpu_=1;
int sqrts_=13;
int pdfWeights_=26;
vector<int> cats_;
string catsStr_;
bool isFlashgg_;
bool binnedFit_;
int  nBins_;
string flashggCatsStr_;
vector<string> flashggCats_;
bool checkYields_;
bool useMerged_;
vector<string>  split_;
string  splitStr_;
float newIntLumi_;
float originalIntLumi_;

string normalisationCut_;

float mcBeamSpotWidth_=5.14; //cm
//float dataBeamSpotWidth_=4.24; //cm
float dataBeamSpotWidth_=3.5; //cm


//string referenceProc_   = "ggh";
//string referenceProcWV_ = "ggh";

string referenceProc_			= "InsideAcceptance_genPt_45.0to85.0";	      
string referenceProcDifferential_	= "InsideAcceptance_genPt_45.0to85.0";	      
string referenceProcWV_			= "InsideAcceptance_genPt_45.0to85.0";

//string referenceProc_			= "InsideAcceptance_genPt_125.0to200.0";	      
//string referenceProcDifferential_	= "InsideAcceptance_genPt_125.0to200.0";	      
//string referenceProcWV_			= "InsideAcceptance_genPt_125.0to200.0";

//string referenceProc_			= "OutsideAcceptance";
//string referenceProcDifferential_	= "OutsideAcceptance";
//string referenceProcWV_			= "OutsideAcceptance";

string referenceProcGGH_= "ggh"; //make these configurables
string referenceProcVBF_= "vbf";
string referenceProcWH_ = "wh";
string referenceProcZH_ = "zh";
string referenceProcTTH_= "tth";


string referenceTagWV_			= "SigmaMpTTag_1_recoPt_45.0to85.0";
string referenceTagDifferential_	= "SigmaMpTTag_1_recoPt_45.0to85.0";	    
string referenceTagRV_			= "SigmaMpTTag_1_recoPt_45.0to85.0";

//string referenceTagWV_			= "SigmaMpTTag_2_recoPt_0.0to15.0";
////string referenceTagDifferential_	= "SigmaMpTTag_1_recoPt_125.0to200.0";	    
//string referenceTagDifferential_	= "SigmaMpTTag_2_recoPt_45.0to85.0";
//string referenceTagRV_			= "SigmaMpTTag_2_recoPt_45.0to85.0";


//string referenceProc_="InsideAcceptance";
//string referenceProcWV_="InsideAcceptance";
//string referenceProcTTH_="InsideAcceptance";
//string referenceTagWV_="SigmaMpTTag_1";
//string referenceTagRV_="UntaggedTag_2";

vector<string> map_proc_;
vector<string> map_cat_;
vector<string> map_replacement_proc_rv_;
vector<string> map_replacement_cat_rv_;
vector<string> map_replacement_proc_wv_;
vector<string> map_replacement_cat_wv_;
vector<int> map_nG_rv_;
vector<int> map_nG_wv_;
RooRealVar *mass_;
RooRealVar *dZ_;
RooRealVar *procIndex_;
RooRealVar *intLumi_;
bool beamSpotReweigh_ = false;
bool shiftOffDiag_ = false;
bool noSkip_ = false;
float MHref_ = 0.;

string optReferenceProc_ = "";
string optReferenceProcDifferential_ = "";
string optReferenceTagDifferential_ = "";
string optReferenceTagWV_ = "";
string optReferenceProcWV_ = "";

// set range to be the same as FTest
// want quite a large range otherwise don't
// see crazy bins on the sides
float rangeLow = 110;   // make this configurable and in Ftest too
float rangeHigh = 140;


void OptionParser(int argc, char *argv[]){
  po::options_description desc1("Allowed options");
  desc1.add_options()
    ("help,h",                                                                                			"Show help")
    ("infilename,i", po::value<string>(&filenameStr_),                                           			"Input file name")
    ("outfilename,o", po::value<string>(&outfilename_)->default_value("CMS-HGG_sigfit.root"), 			"Output file name")
    ("merge,m", po::value<string>(&mergefilename_)->default_value(""),                               	        "Merge the output with the given workspace")
    ("datfilename,d", po::value<string>(&datfilename_)->default_value("dat/newConfig.dat"),      			"Configuration file")
    ("systfilename,s", po::value<string>(&systfilename_)->default_value(""),		"Systematic model numbers")
    ("plotDir,p", po::value<string>(&plotDir_)->default_value("plots"),						"Put plots in this directory")
    ("skipPlots", 																																									"Do not make any plots")
    ("mhLow,L", po::value<int>(&mhLow_)->default_value(110),                                  			"Low mass point")
    ("mcBeamSpotWidth", po::value<float>(&mcBeamSpotWidth_)->default_value(5.14),                                  "Default width of MC beamspot")
    ("dataBeamSpotWidth", po::value<float>(&dataBeamSpotWidth_)->default_value(3.50),                                  "Default width of data beamspot")
    ("nThreads,t", po::value<int>(&ncpu_)->default_value(ncpu_),                               			"Number of threads to be used for the fits")
    ("mhHigh,H", po::value<int>(&mhHigh_)->default_value(140),                                			"High mass point")
    // ("nCats,n", po::value<int>(&nCats_)->default_value(9),                                    			"Number of total categories")
    ("constraintValue,C", po::value<float>(&constraintValue_)->default_value(0.1),            			"Constraint value")
    ("constraintValueMass,M", po::value<int>(&constraintValueMass_)->default_value(125),                        "Constraint value mass")
    ("pdfWeights", po::value<int>(&pdfWeights_)->default_value(0),                        "If pdf systematics should be considered, say how many (default 0 = off)")
    ("skipSecondaryModels",                                                                   			"Turn off creation of all additional models")
    ("doQuadraticSigmaSum",  										        "Add sigma systematic terms in quadrature")
    ("procs", po::value<string>(&procStr_)->default_value("ggh,vbf,wh,zh,tth"),					"Processes (comma sep)")
    ("skipMasses", po::value<string>(&massesToSkip_)->default_value(""),					"Skip these mass points - used eg for the 7TeV where there's no mc at 145")
    ("runInitialFitsOnly",                                                                                      "Just fit gaussians - no interpolation, no systematics - useful for testing nGaussians")
    ("cloneFits", po::value<string>(&cloneFitFile_),															"Do not redo the fits but load the fit parameters from this workspace. Pass as fileName:wsName.")
    ("nonRecursive",                                                                             		"Do not recursively calculate gaussian fractions")
    ("verbose,v", po::value<int>(&verbose_)->default_value(0),                                			"Verbosity level: 0 (lowest) - 3 (highest)")
    ("isFlashgg",	po::value<bool>(&isFlashgg_)->default_value(true),														"Use flashgg format")
    ("binnedFit",	po::value<bool>(&binnedFit_)->default_value(true),														"Binned Signal fit")
    ("nBins",	po::value<int>(&nBins_)->default_value(80),														"If using binned signal for fit, how many bins in 100-180?")
    ("checkYields",	po::value<bool>(&checkYields_)->default_value(false),														"Use flashgg format (default false)")
    ("beamSpotReweigh",po::value<bool>(&beamSpotReweigh_)->default_value(false),"Reweight events to  discrepancy in width of beamspot between data and MC")
    ("shiftOffDiag",po::value<bool>(&shiftOffDiag_)->default_value(false),"Shift mean to correct for scale bias in 'off-diagonal' elements of differential analysis ")
    ("split", po::value<string>(&splitStr_)->default_value(""), "do just one tag,proc ")
    ("changeIntLumi",	po::value<float>(&newIntLumi_)->default_value(0),														"If you want to specify an intLumi other than the one in the file. The event weights and rooRealVar IntLumi are both changed accordingly. (Specify new intlumi in fb^{-1})")
    ("flashggCats,f", po::value<string>(&flashggCatsStr_)->default_value("UntaggedTag_0,UntaggedTag_1,UntaggedTag_2,UntaggedTag_3,UntaggedTag_4,VBFTag_0,VBFTag_1,VBFTag_2,TTHHadronicTag,TTHLeptonicTag,VHHadronicTag,VHTightTag,VHLooseTag,VHEtTag"),       "Flashgg categories if used")
    ("MHref", po::value<float>(&MHref_)->default_value(0.),            			"reference MH for xsec")
    ("refProc", po::value<		string>(&optReferenceProc_)->default_value(""),            			"reference proc for replacement")
    ("refProcDiff", po::value<	string>(&optReferenceProcDifferential_)->default_value(""),            			"reference proc for replacement for differential analysis")
    ("refTagDiff", po::value<	string>(&optReferenceTagDifferential_)->default_value(""),            			"reference tag for replacement for differential analysis (RV)")
    ("refTagWV", po::value<			string>(&optReferenceTagWV_)->default_value(""),            			"reference tag for replacement WV")
    ("refProcWV", po::value<			string>(&optReferenceProcWV_)->default_value(""),            			"reference proc for replacement WV")
    ("normalisationCut", po::value<string>(&normalisationCut_)->default_value("1"),            			"specify cut to apply on the final normalisation, e.g. select process with processIndex == xx")
    ("noSkip",po::value<bool>(&noSkip_)->default_value(false)," Do not skip datasets with conditions below minimum")
    ;                                                                                             		
  
  po::options_description desc("Allowed options");
  desc.add(desc1);
  
  po::variables_map vm;
  po::store(po::parse_command_line(argc,argv,desc),vm);
  po::notify(vm);
  if (vm.count("help")){ cout << desc << endl; exit(1);}
  if (vm.count("skipPlots"))								skipPlots_=true;
  if (vm.count("spin"))                     spin_=true;
  if (vm.count("isCutBased"))               isCutBased_=true;
  if (vm.count("is2011"))               		is2011_=true;
  if (vm.count("is2011"))               		sqrts_=7;
  if (vm.count("is2012"))               		is2012_=true;
  if (vm.count("is2012"))               		sqrts_=8;
  if (vm.count("runInitialFitsOnly"))       runInitialFitsOnly_=true;
  if (vm.count("cloneFits"))								cloneFits_=true;
  if (vm.count("nosplitRVWV"))              splitRVWV_=false;
  if (vm.count("doQuadraticSigmaSum"))			doQuadraticSigmaSum_=true;
  if (vm.count("skipSecondaryModels"))      doSecondaryModels_=false;
  if (vm.count("recursive"))                recursive_=false;
  if (vm.count("skipMasses")) {
    cout << "[INFO] Masses to skip... " << endl;
    vector<string> els;
    
    // if you want to skip masses for some reason...
    split(els,massesToSkip_,boost::is_any_of(","));
    if (els.size()>0 && massesToSkip_!="") {
      for (vector<string>::iterator it=els.begin(); it!=els.end(); it++) {
	skipMasses_.push_back(boost::lexical_cast<int>(*it));
      }
    }
    cout << "\t";
    for (vector<int>::iterator it=skipMasses_.begin(); it!=skipMasses_.end(); it++) cout << *it << " ";
    cout << endl;
  }
  
  // split options which are fiven as lists
  split(procs_,procStr_,boost::is_any_of(","));
  split(flashggCats_,flashggCatsStr_,boost::is_any_of(","));
  split(filename_,filenameStr_,boost::is_any_of(","));
  split(split_,splitStr_,boost::is_any_of(",")); // proc,cat
  
}

// used to get index of the reference dataset in the list of requried guassians.
unsigned int getIndexOfReferenceDataset(string proc, string cat){
  int iLine =-1;
  std::cout<<"process sought "<<proc<<std::endl;
  std::cout<<"cat sought "<<cat<<std::endl;
  for(unsigned int i =0 ; i < map_proc_.size() ; i++){
    string this_process = map_proc_[i];
    // std::cout<<"this process "<<this_process<<std::endl;
    string this_cat = map_cat_[i];
    // std::cout<<"this cat "<<this_cat<<std::endl;
    if (this_process.compare(proc) ==0 ){
      if ( this_cat.compare(cat)==0 ){ 
        iLine=i;
        break;
      }
    }
  }
  
  if (iLine==-1 ) {
    std::cout << "ERROR could not find the index of the category you wished to look up. Exit!" << std::endl;
    exit(1);
  }
  return iLine;
}

// is this still used ? -LC 
void transferMacros(TFile *inFile, TDirectory *outFile){
  
  TIter next(inFile->GetListOfKeys());
  TKey *key;
  while ((key = (TKey*)next())){
    if (string(key->ReadObj()->ClassName())=="TMacro") {
      //cout << key->ReadObj()->ClassName() << " : " << key->GetName() << endl;
      TMacro *macro = (TMacro*)inFile->Get(key->GetName());
      outFile->cd();
      macro->Write();
    }
  }
}


void addToCloneMap(clonemap_t &cloneMap, string proc, string cat, string name, RooSpline1D *spline){
  
  pair<string,string> mapKey = make_pair(proc,cat);
  if (cloneMap.find(mapKey)==cloneMap.end()){
    map<string,RooSpline1D*> tempMap;
    tempMap.insert(make_pair(name,spline));
    cloneMap.insert(make_pair(mapKey,tempMap));
  }
  else {
    cloneMap[mapKey].insert(make_pair(name,spline));
  }
}


void makeCloneConfig(clonemap_t mapRV, clonemap_t mapWV, string newdatfilename){
  
  system("mkdir -p tmp");
  ofstream datfile;
  datfile.open(newdatfilename.c_str());
  if (datfile.fail()) {
    std::cerr << "Could not open " << newdatfilename << std::endl;
    exit(1);
  }
  for (clonemap_t::iterator it=mapRV.begin(); it!=mapRV.end(); it++){
    string proc = it->first.first;
    string cat = it->first.second;
    map<string,RooSpline1D*> paramsRV = it->second;
    map<string,RooSpline1D*> paramsWV = mapWV[make_pair(proc,cat)];
    int countRV=0;
    int countWV=0;
    for (map<string,RooSpline1D*>::iterator pIt=paramsRV.begin(); pIt!=paramsRV.end(); pIt++){
      if (pIt->first.find("dm_g")!=string::npos && pIt->first.find("_SM")==string::npos && pIt->first.find("_2")==string::npos && pIt->first.find("_NW")==string::npos){
	countRV++;
      }
    }
    for (map<string,RooSpline1D*>::iterator pIt=paramsWV.begin(); pIt!=paramsWV.end(); pIt++){
      if (pIt->first.find("dm_g")!=string::npos && pIt->first.find("_SM")==string::npos && pIt->first.find("_2")==string::npos && pIt->first.find("_NW")==string::npos){
	countWV++;
      }
    }
    if (verbose_) cout << "[INFO] "<< proc << " " << cat << " " << countRV << " " << countWV << endl;
    datfile << proc << " " << cat << " " << countRV << " " << countWV << endl;
  }
  datfile.close();
  
}

RooDataSet * reduceDataset(RooDataSet *data0){
  
  RooDataSet *data = (RooDataSet*) data0->emptyClone()->reduce(RooArgSet(*mass_, *dZ_, *procIndex_));
  RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
  std::cout<<"reduceDataset: dataset "<<data0->GetName()<<std::endl;
  for (unsigned int i=0 ; i < data0->numEntries() ; i++){
    if (data0->get(i)->getRealValue("CMS_hgg_mass") > rangeLow && data0->get(i)->getRealValue("CMS_hgg_mass") <rangeHigh ){
      mass_->setVal(data0->get(i)->getRealValue("CMS_hgg_mass"));
      //      std::cout<<"reduceDataset: entry "<<i<<", CMS_hgg_mass = "<<data0->get(i)->getRealValue("CMS_hgg_mass")<<std::endl;
      weight0->setVal(data0->weight() ); // <--- is this correct?
      dZ_->setVal(data0->get(i)->getRealValue("dZ"));
      procIndex_->setVal(data0->get(i)->getRealValue("processIndex"));
      data->add( RooArgList(*mass_, *dZ_, *procIndex_, *weight0), weight0->getVal() );
    }
  }
  return data;
}


void plotBeamSpotDZdist(RooDataSet *data0, string suffix=""){
  gStyle->SetOptFit(1111);
  RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
  TH1F *histSmallDZ = new TH1F ("h1sdz","h1sdz",20,-0.1,0.1);
  TH1F *histLargeDZ = new TH1F ("h1ldz","h1ldz",20,-25,25);
  
  for (unsigned int i=0 ; i < data0->numEntries() ; i++){
    mass_->setVal(data0->get(i)->getRealValue("CMS_hgg_mass"));
    weight0->setVal(data0->weight() ); // <--- is this correct?
    dZ_->setVal(data0->get(i)->getRealValue("dZ"));
    if (fabs(dZ_->getVal()) <0.1){
      histSmallDZ->Fill( dZ_->getVal(),data0->weight());
    } else {
      histLargeDZ->Fill( dZ_->getVal(),data0->weight());
    }
  }
  TCanvas *c = new TCanvas("c","c",500,500);
  string extra="";
  if (beamSpotReweigh_){
    extra="BS_reweigh";
  }
  histSmallDZ->Draw();
  histSmallDZ->Fit("gaus");
  std::cout << "LC DEBUG sum entries smallDz " << histSmallDZ->Integral() <<std::endl;
  c->SaveAs(Form("testLC-%s_smallDz_%s_%s.pdf",data0->GetName(),extra.c_str(),suffix.c_str()));
  histLargeDZ->Draw();
  histLargeDZ->Fit("gaus");
  std::cout << "LC DEBUG sum entries largeDz " << histSmallDZ->Integral() <<std::endl;
  c->SaveAs(Form("testLC-%s_largeDz_%s_%s.pdf",data0->GetName(),extra.c_str(),suffix.c_str()));
  //delete c;
  delete histSmallDZ;
  delete histLargeDZ;
  gStyle->SetOptFit();
}

RooDataSet * rvwvDataset(RooDataSet *data0, string rvwv){
  
  RooDataSet *dataRV = (RooDataSet*) data0->emptyClone()->reduce(RooArgSet(*mass_, *dZ_, *procIndex_));
  RooDataSet *dataWV = (RooDataSet*) data0->emptyClone()->reduce(RooArgSet(*mass_, *dZ_, *procIndex_));
  RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
  std::cout<<"rvwvDataset: dataset "<<data0->GetName()<<std::endl;
  for (unsigned int i=0 ; i < data0->numEntries() ; i++){
    if (data0->get(i)->getRealValue("CMS_hgg_mass") > rangeLow && data0->get(i)->getRealValue("CMS_hgg_mass") < rangeHigh ){
      mass_->setVal(data0->get(i)->getRealValue("CMS_hgg_mass"));
      //      std::cout<<"rvwvDataset: entry "<<i<<", CMS_hgg_mass = "<<data0->get(i)->getRealValue("CMS_hgg_mass")<<std::endl;
      weight0->setVal(data0->weight() ); // <--- is this correct?
      dZ_->setVal(fabs(data0->get(i)->getRealValue("dZ")));
      procIndex_->setVal(data0->get(i)->getRealValue("processIndex"));
      if (dZ_->getVal() <1.){
	dataRV->add( RooArgList(*mass_, *dZ_, *procIndex_, *weight0), weight0->getVal() );
      } else{
	dataWV->add( RooArgList(*mass_, *dZ_, *procIndex_, *weight0), weight0->getVal() );
      }
    }
  }
  if (rvwv.compare("RV") ==0){
    return dataRV;
  } else if (rvwv.compare("WV") ==0){
    return dataWV;
  } else {
    std::cout << "[ERROR] (rvwvDataset) please specific second argument as 'RV' or 'WV'. Exit (1); " << std::endl;
    exit (1);
  }
}

RooDataSet * beamSpotReweigh(RooDataSet *data0 /*original dataset*/){
  std::cout << " LC DEBUG REWEIGHITNG BEAMSPOT !!!"<< std::endl;
  RooDataSet *data = (RooDataSet*) data0->emptyClone();
  RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
  data0->Print();
  plotBeamSpotDZdist(data0,"before");
  data0->Print();
  std::cout<<"beamSpotReweigh: dataset "<<data0->GetName()<<std::endl;
  for (int i = 0; i < data0->numEntries(); i++) {
    mass_->setVal(data0->get(i)->getRealValue("CMS_hgg_mass"));
    //    std::cout<<"beamSpotReweigh: entry "<<i<<", CMS_hgg_mass = "<<data0->get(i)->getRealValue("CMS_hgg_mass")<<std::endl;
    dZ_->setVal(data0->get(i)->getRealValue("dZ"));
    procIndex_->setVal(data0->get(i)->getRealValue("processIndex"));
    double factor =1.0;
    
    if (fabs(dZ_->getVal()) < 0.1 ){
      factor =1;
    } else {
      double mcBeamSpot=TMath::Gaus(dZ_->getVal(),0,TMath::Sqrt(2)*mcBeamSpotWidth_,true); 
      double dataBeamSpot=TMath::Gaus(dZ_->getVal(),0,TMath::Sqrt(2)*dataBeamSpotWidth_,true); 
      factor = dataBeamSpot/mcBeamSpot; 
    }
    //std::cout << " LC DEBUG entry "<< i << " dZ " << dZ_->getVal() << " factor "<< factor  << std::endl;
    
    weight0->setVal(factor * data0->weight() ); // <--- is this correct?
    data->add( RooArgList(*mass_, *dZ_, *procIndex_, *weight0), weight0->getVal() );
  }
  data->Print();
  plotBeamSpotDZdist(data,"after");
  std::cout<<"data after BS reweighting"<<std::endl;
  data->Print();  
  
  if (verbose_) std::cout << "[INFO] Old dataset (before beamSpot  reweight): " << *data0 << std::endl;
  if (verbose_) std::cout << "[INFO] New dataset (after beamSpot reweight):  " << *data << std::endl;
  
  return data;
}

RooDataSet * intLumiReweigh(RooDataSet *data0 /*original dataset*/){
  
  double factor = newIntLumi_/originalIntLumi_; // newIntLumi expressed in 1/fb
  
  if (verbose_) std::cout << "[INFO] Was able to access IntLumi directlly from WS. IntLumi " << intLumi_->getVal() << "pb^{-1}" << std::endl;
  
  if (verbose_) std::cout << "[INFO] Old int lumi " << originalIntLumi_  <<", new int lumi " << newIntLumi_<< std::endl;
  if (verbose_) std::cout << "[INFO] Changing weights of dataset by a factor " << factor << " as per newIntLumi option" << std::endl;
  
  RooDataSet *data = (RooDataSet*) data0->emptyClone();
  RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
  std::cout<<"intLumiReweigh: dataset "<<data0->GetName()<<std::endl;
  for (int i = 0; i < data0->numEntries(); i++) {
    if (data0->get(i)->getRealValue("CMS_hgg_mass") > rangeLow && data0->get(i)->getRealValue("CMS_hgg_mass") < rangeHigh ){      
      mass_->setVal(data0->get(i)->getRealValue("CMS_hgg_mass"));
      //      std::cout<<"intLumiReweigh: entry "<<i<<", CMS_hgg_mass = "<<data0->get(i)->getRealValue("CMS_hgg_mass")<<std::endl;
      dZ_->setVal(data0->get(i)->getRealValue("dZ"));
      procIndex_->setVal(data0->get(i)->getRealValue("processIndex"));
      weight0->setVal(factor * data0->weight() ); // <--- is this correct?
      data->add( RooArgList(*mass_, *dZ_, *procIndex_, *weight0), weight0->getVal() );
    }
  }
  if (verbose_) std::cout << "[INFO] Old dataset (before intLumi change): " << *data0 << std::endl;
  if (verbose_) std::cout << "[INFO] New dataset (intLumi change x"<< factor <<"): " << *data << std::endl;
  
  return data;
}

bool skipMass(int mh){
  for (vector<int>::iterator it=skipMasses_.begin(); it!=skipMasses_.end(); it++) {
    if (*it==mh) return true;
  }
  return false;
}

int main(int argc, char *argv[]){
  
  int   minNevts    = 500; // if below minNevts #gauss = -1  //deflaut is ggh UntaggedTag3
  
  double  minConditions = 60.0; //(1 - suwNegW/datasetW)*numEntries : if below, the dataset is too unstable (due to neg weights) to be fitted, replace with empty one
  
  // Bins for fitting
  int nBinsFit_ = 160; //  //deflaut is ggh UntaggedTag3MDDB make it a parameter
  
  
  gROOT->SetBatch();
  
  OptionParser(argc,argv);
  
  TStopwatch sw;
  sw.Start();
  
  // reference details for low stats cats
  // need to make this configurable ?! -LC
  referenceProc_="ggh";
  if( !(optReferenceProc_.empty()) ){
    referenceProc_ = optReferenceProc_;
  }
  std::cout<<"referenceProc_ "<<referenceProc_<<std::endl;
  //  referenceProcTTH_="tth"; // MDDB
  //  referenceTagWV_="UntaggedTag_2"; // histest stats WV is ggh Untagged 3. 
  
  referenceProcDifferential_	= "InsideAcceptance_genPt_45p0_85p0";
  if( !(optReferenceProcDifferential_.empty()) ){
    referenceProcDifferential_ = optReferenceProcDifferential_;
  }
  
  ////  referenceTagWV_="SigmaMpTTag_1_recoPt_45.0to85.0"; // histest stats WV is ggh Untagged 3. 
  //  referenceTagWV_="SigmaMpTTag_1_recoPt_125.0to200.0"; // histest stats WV is ggh Untagged 3. 
  referenceTagWV_="recoPt_45p0_85p0_SigmaMpTTag_1"; 
  if ( !(optReferenceTagWV_.empty()) ){
    referenceTagWV_ = optReferenceTagWV_;
  }

  referenceProcWV_= "InsideAcceptance_genPt_45p0_85p0";
  if ( !(optReferenceProcWV_.empty()) ){
    referenceProcWV_ = optReferenceProcWV_;
  }
  
  //  referenceTagWV_="SigmaMpTTag_1"; // histest stats WV is ggh Untagged 3. 
  //  referenceTagRV_="UntaggedTag_2"; // fairly low resolution tag even for ggh, more approprioate as te default than re-using the original tag.
  
  
  ////  referenceTagRV_="SigmaMpTTag_1_recoPt_45.0to85.0"; // fairly low resolution tag even for ggh, more approprioate as te default than re-using the original tag.
  //  referenceTagRV_="SigmaMpTTag_1_recoPt_125.0to200.0"; // fairly low resolution tag even for ggh, more approprioate as te default than re-using the original tag.
  referenceTagRV_="recoPt_45p0_85p0_SigmaMpTTag_1";
  
  referenceTagDifferential_	= "recoPt_45p0_85p0_SigmaMpTTag_1";
  if( !(optReferenceTagDifferential_.empty()) ){
    referenceTagDifferential_ = optReferenceTagDifferential_;
  }
  
  // are WV which needs to borrow should be taken from here
  
  // isFlashgg should now be the only option.
  if (isFlashgg_){ 
    nCats_= flashggCats_.size();
  } else {
    std::cout << "[ERROR] script is onyl compatible with flashgg! exit(1)." << std::endl;
    exit(1);
  }
  
  // open sig file
  //TFile *inFile = TFile::Open(filename_[0].c_str());
  
  // print out nEvents per proc/tag etc...
  if (checkYields_){	  
    //    WSTFileWrapper * inWS0 = new WSTFileWrapper(filenameStr_,"tagsDumper/cms_hgg_13TeV");
    WSTFileWrapper * inWS0 = new WSTFileWrapper(filenameStr_,"cms_hgg_13TeV");
    std::list<RooAbsData*> data =  (inWS0->allData()) ;
    for (std::list<RooAbsData*>::const_iterator iterator = data.begin(), end = data.end();
	 iterator != end;
	 ++iterator) {
      RooDataSet *dataset = dynamic_cast<RooDataSet *>( *iterator );
      if (dataset) {
	std::cout << "dataset / numEntries / sumEntries " <<  dataset->GetName() << " , " << dataset->sumEntries() << " , " << dataset->numEntries() << std::endl;
	
      }
    }
    return 1;
  }
  
  //time to open the signal file for the main script!
  WSTFileWrapper *inWS;
  if (isFlashgg_){
    //    inWS = new WSTFileWrapper(filenameStr_,"tagsDumper/cms_hgg_13TeV");
    inWS = new WSTFileWrapper(filenameStr_,"cms_hgg_13TeV");
    std::list<RooAbsData*> test =  (inWS->allData()) ;
    if (verbose_) {
      std::cout << " [INFO] WS contains " << std::endl;
      for (std::list<RooAbsData*>::const_iterator iterator = test.begin(), end = test.end(); iterator != end; ++iterator) {
	//		std::cout << **iterator << std::endl;
      }
    }
  } else {
    std::cout << "[ERROR] script is only compatible with flashgg! exit(1)." << std::endl;
    exit(1);
  }
  
  if (inWS) { 
    if (verbose_)  std::cout << "[INFO] Workspace opened correctly" << std::endl;
  } else { 
    std::cout << "[EXIT] Workspace is null pointer. exit" << std::endl; 
    exit(1);
  }
  
  // get the required variables from the WS
  mass_ = (RooRealVar*)inWS->var("CMS_hgg_mass");
  mass_->SetTitle("m_{#gamma#gamma}");
  mass_->setUnit("GeV");
  dZ_ = (RooRealVar*)inWS->var("dZ");
  dZ_->setMin(-25.0);
  dZ_->setMax(25.0);
  dZ_->setBins(100);
  procIndex_ = (RooRealVar*)inWS->var("processIndex");
  procIndex_->setMin(10);
  procIndex_->setMax(15);
  procIndex_->setBins(5);
  //intLumi_ = (RooRealVar*)inWS->var("IntLumi"); // **** #### fix IntLumi in ws! ### ****
  intLumi_ = new RooRealVar("IntLumi","IntLumi", 1000.0);
  intLumi_->setConstant(); 
  
  originalIntLumi_ = (intLumi_->getVal());// specify in 1/pb 
  newIntLumi_ = newIntLumi_*1000; // specify in 1/pb instead of 1/fb.
  intLumi_->setVal(newIntLumi_); //fix IntLumi in ws!
  
  //RooRealVar *weight = (RooRealVar*)inWS->var("weight:weight");
  if( mass_ && dZ_ && intLumi_){
    if (verbose_) std::cout << "[INFO] RooRealVars mass, intL and dZ, found ? " << mass_ << ", " << intLumi_  << ", "<< dZ_<<  std::endl; 
  } else {
    std::cout << "[ERROR] could not find some of these RooRealVars in WS: mass_ " << mass_ << " dZ " << dZ_ << " intLumi " << intLumi_<< ".  exit(1) "<< std::endl;
    exit(1);
  }
  
  if (verbose_) std::cout << "[INFO] setting RoorealVars used for fitting."<< std::endl;
  RooRealVar *MH = new RooRealVar("MH","m_{H}",mhLow_,mhHigh_);
  MH->setUnit("GeV");
  MH->setConstant(true);
  RooRealVar *MH_SM = new RooRealVar("MH_SM","m_{H} (SM)",mhLow_,mhHigh_);
  MH_SM->setConstant(true);
  RooRealVar *DeltaM = new RooRealVar("DeltaM","#Delta m_{H}",0.,-10.,10.);
  DeltaM->setUnit("GeV");
  DeltaM->setConstant(true);
  RooAddition *MH_2 = new RooAddition("MH_2","m_{H} (2)",RooArgList(*MH,*DeltaM));
  RooRealVar *higgsDecayWidth = new RooRealVar("HiggsDecayWidth","#Gamma m_{H}",0.,0.,10.);
  higgsDecayWidth->setConstant(true);
  
  //prepare teh output file!
  if (verbose_) std::cout << "[INFO] preparing outfile "<< outfilename_ << std::endl;
  TFile *outFile = new TFile(outfilename_.c_str(),"RECREATE");
  RooWorkspace *outWS;
  
  if (isFlashgg_) outWS = new RooWorkspace("wsig_13TeV");
  
  RooWorkspace *mergeWS = 0;
  TFile *mergeFile = 0;
  
  // maybe this is no longer needed ? -LC
  if(!mergefilename_.empty()) {
    mergeFile = TFile::Open(mergefilename_.c_str());
    if (is2011_) mergeWS = (RooWorkspace*)mergeFile->Get("wsig_7TeV");
    else  mergeWS = (RooWorkspace*)mergeFile->Get("wsig_8TeV");
  }
  
  // i'm gonna comemnt this otu and see if anythign breaks... -LC
  //transferMacros(inFile,outFile);
  
  //splines for RV/WV
  clonemap_t cloneSplinesMapRV;
  clonemap_t cloneSplinesMapWV;
  
  // make the output dir
  system(Form("mkdir -p %s/initialFits",plotDir_.c_str()));
  system("mkdir -p dat/in");
  parmap_t allParameters;
  
  // Prepare the list of <proc> <tag> <nRV> <nWV> entries
  ifstream datfile;
  datfile.open(datfilename_.c_str());
  if (datfile.fail()) {
    std::cerr << "[ERROR] Could not open " << datfilename_ <<std::endl;
    exit(1);
  }
  
  if (verbose_) std::cout << "[INFO] opening dat file "<< datfilename_ << std::endl;
  //loop over it 
  while (datfile.good()){
    string line;
    getline(datfile,line);
    if (line=="\n" || line.substr(0,1)=="#" || line==" " || line.empty()) continue;
    vector<string> els;
    split(els,line,boost::is_any_of(" "));
    if( els.size()!=4 && els.size()!=6 && els.size()!=8 ) {
      cerr << "Malformed line " << line << " " << els.size() <<endl;
      assert(0);
    }
    
    // the defaukt info need: proc, tag, nRv, nrWV
    string proc = els[0];
    string cat = els[1]; // used to be an int, string directly now...
    int nGaussiansRV = boost::lexical_cast<int>(els[2]);
    int nGaussiansWV = boost::lexical_cast<int>(els[3]);
    
    replace_ = false; // old method of replacing from Matt and Nick
    // have a different appraoch now but could re-use machinery.
    
    cout << " MDDB initial string "  ;
    for (int i = 0 ;i <  els.size() ; ++i) cout << els[i] << " " ;
    cout << endl;
    
    if( els.size()==6 ) { // in this case you have specified a replacement tag!
      cout << " MDDB REPLACEMENT TAG ! : proc = " << proc << " cat =  " << cat  << endl;
      replaceWith_rv_ = make_pair(els[4],els[5]); // proc, cat
      replaceWith_wv_ = make_pair(els[4],els[5]); // proc, cat
      cout << " MDDB REPLACE WITH : proc = " << els[4] << " cat =  " << els[5]  << endl;
      replace_ = true;
      map_replacement_proc_rv_.push_back(els[4]);
      map_replacement_cat_rv_.push_back(els[5]);
      map_replacement_proc_wv_.push_back(els[4]);
      map_replacement_cat_wv_.push_back(els[5]);
    } else if (els.size()==8){
      cout << " MDDB REPLACEMENT TAG ! : proc = " << proc << " cat =  " << cat  << endl;
      replaceWith_rv_ = make_pair(els[4],els[5]); // proc, cat
      replaceWith_wv_ = make_pair(els[6],els[7]); // proc, cat
      cout << " MDDB REPLACE WITH : proc = " << els[4] << " cat =  " << els[5]  << endl;
      replace_ = true;
      map_replacement_proc_rv_.push_back(els[4]);
      map_replacement_cat_rv_.push_back(els[5]);
      map_replacement_proc_wv_.push_back(els[6]);
      map_replacement_cat_wv_.push_back(els[7]);
    } else {
      // if no replacement is speficied, use defaults
      if (cat.compare(0,3,"TTH") ==0){
	// if the cat starts with TTH, use TTH reference process.
	// howwver this is over-riden later if the WV needs to be replaced
	// as even teh TTH tags in WV has limited stats
	map_replacement_proc_rv_.push_back(referenceProcTTH_);
	map_replacement_cat_rv_.push_back(cat);
	map_replacement_proc_wv_.push_back(referenceProcTTH_);
	map_replacement_cat_wv_.push_back(cat);
	cout << " MDDB USE TTH REPLACEMENTS: proc = " << referenceProcTTH_ << " cat =  " << cat  << endl;      
      } else if (cat.compare(0,3,"VBF") ==0){
	map_replacement_proc_rv_.push_back(referenceProcVBF_);
	map_replacement_cat_rv_.push_back(cat);
	map_replacement_proc_wv_.push_back(referenceProcVBF_);
	map_replacement_cat_wv_.push_back(cat);
	cout << " MDDB USE VBF REPLACEMENTS: proc = " << referenceProcVBF_ << " cat =  " << cat  << endl;      
      } else if (cat.compare(0,2,"ZH") ==0){
	// not implemented yet
      } else if (cat.compare(0,2,"WH") ==0){
	// not implemented yet
	////      } else if (cat.compare(0,6,"SigmaM") ==0){
      } else if (cat.find("SigmaM")){
	
	//	replaceWith_ = make_pair(referenceProcDifferential_, referenceTagDifferential_ ); // proc, cat  //VRT, debug replacements
	//	replace_ = true;                                                                                //VRT, debug replacements
	
	map_replacement_proc_rv_.push_back(referenceProcDifferential_);
	map_replacement_cat_rv_.push_back(referenceTagDifferential_);
	if( optReferenceProcWV_ != ""){
	  map_replacement_proc_wv_.push_back(referenceProcWV_);
	}
	else{
	  map_replacement_proc_wv_.push_back(referenceProcDifferential_);
	}
	if( optReferenceTagWV_ != ""){
	  map_replacement_cat_wv_.push_back(referenceTagWV_);
	}
	else{
	  map_replacement_cat_wv_.push_back(referenceTagDifferential_);
	}
	cout << " MDDB USE DIFFERENTIAL REPLACEMENTS: proc = " << referenceProcDifferential_ << " cat =  " << referenceTagDifferential_  << endl;      
      }
      else{
	// else use the ggh
	map_replacement_proc_rv_.push_back(referenceProcGGH_);
	map_replacement_cat_rv_.push_back(cat);
	map_replacement_proc_wv_.push_back(referenceProcGGH_);
	map_replacement_cat_wv_.push_back(cat);
	cout << " MDDB USE STD GGH REPLACEMENTS: proc = " << referenceProcGGH_ << " cat =  " << cat  << endl;      
      }
      
      //MDDB       // if no replacement is speficied, use defaults
      //MDDB       if (cat.compare(0,3,"TTH") ==0){
      //MDDB 	// if the cat starts with TTH, use TTH reference process.
      //MDDB 	// howwver this is over-riden later if the WV needs to be replaced
      //MDDB 	// as even teh TTH tags in WV has limited stats
      //MDDB 	cout << " MDDB it's TTH " << endl;	
      //MDDB 	map_replacement_proc_.push_back(referenceProcTTH_);
      //MDDB 	cout << " MDDB pushed reference proc " << referenceProcTTH_  << endl;
      //MDDB 	map_replacement_cat_.push_back(cat);
      //MDDB 	cout << " MDDB pushed reference cat " << cat << endl;
      //MDDB       } else {
      //MDDB 	// else use the ggh
      //MDDB 	cout << " MDDB else use ggh "  << endl;
      //MDDB 	map_replacement_proc_.push_back(referenceProc_);
      //MDDB 	cout << " MDDB pushed reference proc " << referenceProc_  << endl;
      //MDDB 	map_replacement_cat_.push_back(referenceTagRV_); //deflaut is ggh UntaggedTag3
      //MDDB 	cout << " MDDB pushed reference cat " << referenceTagRV_  << endl;
      //MDDB       }
    }
    if (verbose_) std::cout << "[INFO] dat file listing: "<< proc << " " << cat << " " << nGaussiansRV << " " << nGaussiansWV <<  " " << std::endl;
    if (verbose_) std::cout << "[INFO] dat file listing: ----> selected replacements if needed for RV " <<  map_replacement_proc_rv_[map_replacement_proc_rv_.size() -1] << " " <<  map_replacement_cat_rv_[map_replacement_cat_rv_.size() -1] << std::endl;
    if (verbose_) std::cout << "[INFO] dat file listing: ----> selected replacements if needed for WV " <<  map_replacement_proc_wv_[map_replacement_proc_wv_.size() -1] << " " <<  map_replacement_cat_wv_[map_replacement_cat_wv_.size() -1] << std::endl;
    
    map_proc_.push_back(proc);
    map_cat_.push_back(cat);
    map_nG_rv_.push_back(nGaussiansRV);
    map_nG_wv_.push_back(nGaussiansWV);
  }
  
  
  // MDDB 
  cout << "MDDB see what's inside " <<  endl;
  cout << "map_proc" << endl;
  for (int i = 0 ; i< map_proc_.size() ; ++i){
    cout << map_proc_[i]  << " " ;
    cout << map_cat_[i]   <<  " " ;
    cout << map_nG_rv_[i] << " " ;
    cout << map_nG_wv_[i] << " \t" ;    
    cout << "RV: ";
    cout << map_replacement_proc_rv_[i] << " " ;
    cout << map_replacement_cat_rv_[i] ;
    cout << "WV: ";
    cout << map_replacement_proc_wv_[i] << " " ;
    cout << map_replacement_cat_wv_[i] << endl;
  }
  
  bool offDiagonal=0;
  // now start the proper loop, so loop over teh maps we filled above.
  for (unsigned int iLine = 0 ; iLine < map_proc_.size() ; iLine++){
    string proc = map_proc_[iLine] ;
    string cat = map_cat_[iLine];
    int nGaussiansRV = map_nG_rv_[iLine];
    int nGaussiansWV = map_nG_wv_[iLine];
    
    // continueFlag use din job splitting to allow you to just look at once proc/tag at a time
    bool continueFlag =0;
    if (split_.size()==2){
      continueFlag=1;
      string splitProc = split_[0];
      string  splitCat = split_[1];
      std::cout<<"splitProc "<<splitProc<<std::endl;
      std::cout<<"splitCat "<<splitCat<<std::endl;
      std::vector<std::string > splitBinFromProc;
      split(splitBinFromProc, splitProc, boost::is_any_of("_"));
      std::string binFromProc = "";
      if(splitBinFromProc.size()>3){ //typical structure process_genvar_binL_binH
	binFromProc = splitBinFromProc[2]+"_"+splitBinFromProc[3];
      }
      std::cout << "bin name from proc " << splitBinFromProc.back() << std::endl;
      std::cout << "bin name from proc " << binFromProc << std::endl;
      std::vector<std::string > splitBinFromCat;
      split(splitBinFromCat, splitCat, boost::is_any_of("_"));
      std::string binFromCat = "";
      if(splitBinFromCat.size()>2){ //typical structure recovar_binL_binH_sigmaM_catN
	binFromCat = splitBinFromCat[1]+"_"+splitBinFromCat[2];
      }
      std::cout << "bin name from cat " << splitBinFromCat.back() << std::endl;
      std::cout << "bin name from cat " << binFromCat << std::endl;
      //      if(splitBinFromProc.size()!=0 && splitBinFromCat.size()!=0){
      if(splitBinFromProc.size()>1 && splitBinFromCat.size()>1){ //at least two elements, the split has to be happened (OutsideAcceptance --> 1 el., InsideAcceptance_blabla --> 2 el.)
	//	offDiagonal =  !(splitBinFromProc.back().compare(splitBinFromCat.back())==0);
	offDiagonal =  !(binFromProc.compare(binFromCat)==0);
	if(offDiagonal){
	  std::cout << "bin from proc and name do not match, so this is an off-diagonal process!" << std::endl;
	}
      }
      
      //do not shift OutsideAcceptance data, they are not off-diagonal
      if(proc.find("OutsideAcceptance") != std::string::npos ){
	shiftOffDiag_ = false;
      }
      
      if (verbose_) std::cout << " [INFO] check if this proc " << proc << " matches splitProc " << splitProc << ": "<< (proc.compare(splitProc)==0) << std::endl; 
      if ( proc.compare(splitProc) == 0 ) {
        if (verbose_) std::cout << " [INFO] --> proc matches! Check if this cat " << cat  << " matches splitCat " << splitCat<< ": " << (cat.compare(splitCat)==0) <<  std::endl; 
        if ( cat.compare(splitCat) == 0 ) {
	  if (verbose_) std::cout << " [INFO]     --> cat matches too ! so we process it "<<  std::endl; 
          continueFlag =0;
        }
      }
    }
    
    //if no match found, then skip this cat/proc
    if(continueFlag) {
      if(verbose_) std::cout << "[INFO] skipping "<< Form(" fits for proc:%s - cat:%s with nGausRV:%d nGausWV:%d",proc.c_str(),cat.c_str(),nGaussiansRV,nGaussiansWV) << endl;
      continue;
    }
    bool userSkipRV = (nGaussiansRV==-1);
    bool userSkipWV = (nGaussiansWV==-1);
    
    cout << "-----------------------------------------------------------------" << endl;
    cout << Form("[INFO] Running fits for proc:%s - cat:%s with nGausRV:%d nGausWV:%d",proc.c_str(),cat.c_str(),nGaussiansRV,nGaussiansWV) << endl;
    //if( replace_ ) { cout << Form("Will replace parameters using  proc:%s - cat:%d",replaceWith_.first.c_str(),replaceWith_.second) << endl; }
    
    cout << "-----------------------------------------------------------------" << endl;
    // get datasets for each MH here
    map<int,RooDataSet*> datasetsRV;
    map<int,RooDataSet*> datasetsWV;
    map<int,RooDataSet*> FITdatasetsRV;// if catgeory has low stats, may use a different category dataset to make the fits
    map<int,RooDataSet*> FITdatasetsWV;// if catgeory has low stats, may use a different category dataset to make the fits
    
    map<int,RooDataSet*> datasets; // not used ?
    
    bool isProblemCategory =false;
    bool isToSkip =false;

    //these flags make sure the replacement happens for all mass points if only one falls below threshold
    bool tooFewEntriesRV=false;
    bool tooFewEntriesWV=false;
    bool negSumEntriesRV=false;
    bool negSumEntriesWV=false;
    
    bool belowMinConditionsRV=false;
    bool belowMinConditionsWV=false;
    
    double WVoverRVat125 = 0.;
    
    for (int mh=mhLow_; mh<=mhHigh_; mh+=5){
      std::cout << "First, check if at least at one of the mass points the number of entries is too low"<<std::endl;
      if (skipMass(mh)) continue;
      RooDataSet *dataRV; 
      RooDataSet *dataWV; 
      RooDataSet *data;  

      //VRT 16.05.17: attempt to adapt to differentials naming convenction
      string proctemp = proc;
      string procWmassAndE;
      if(proctemp.find_first_of("_") != string::npos){
	procWmassAndE = proctemp.insert(proctemp.find_first_of("_"), Form("_%d_13TeV", mh) );
      }
      else{
	procWmassAndE = proctemp.append( Form("_%d_13TeV", mh) );
      }
      
      
      //      if (verbose_)std::cout << "[INFO] Opening dataset called "<< Form("%s_%d_13TeV_%s",proc.c_str(),mh,cat.c_str()) << " in in WS " << inWS << std::endl;
      //      RooDataSet *data0   = reduceDataset((RooDataSet*)inWS->data(Form("%s_%d_13TeV_%s",proc.c_str(),mh,cat.c_str())));

      if (verbose_)std::cout << "[INFO] Opening dataset called "<< Form("%s_%s",procWmassAndE.c_str(),cat.c_str()) << " in in WS " << inWS << std::endl;
      RooDataSet *data0   = reduceDataset((RooDataSet*)inWS->data(Form("%s_%s",procWmassAndE.c_str(),cat.c_str())));

      if (beamSpotReweigh_){
	data = beamSpotReweigh(intLumiReweigh(data0));
      } else {
	data = intLumiReweigh(data0);
      }
      if (verbose_) std::cout << "[INFO] Old dataset (before intLumi change): " << *data0 << std::endl;
      
      dataRV = rvwvDataset(data,"RV"); 
      dataWV = rvwvDataset(data,"WV"); 
      
      if (verbose_) std::cout << "[INFO] Datasets ? " << *data << std::endl;
      if (verbose_) std::cout << "[INFO] Datasets (right vertex) ? " << *dataRV << std::endl;
      if (verbose_) std::cout << "[INFO] Datasets (wrong vertex) ? " << *dataWV << std::endl;
      
      float nEntriesRV =dataRV->numEntries();
      std::cout << "RV numEntries " << nEntriesRV << std::endl;
      float sEntriesRV= dataRV->sumEntries();
      std::cout << "RV sumEntries " << sEntriesRV << std::endl;
      float nEntriesWV =dataWV->numEntries();
      std::cout << "WV numEntries " << nEntriesWV << std::endl;
      float sEntriesWV= dataWV->sumEntries(); // count the number of entries and total weight on the RV/WV datasets
      std::cout << "WV sumEntries " << sEntriesWV << std::endl;
      
      //find problematic categories
      if(mh==125){
	if(sEntriesWV !=0){
	  WVoverRVat125=sEntriesWV/sEntriesRV;
	}
	else{
	  WVoverRVat125=0.;
	}
	std::cout<<"mh is "<<mh<<std::endl;
	std::cout<<"fitlering categories to be skipped"<<std::endl;
	double sumWpos=0.;
	double sumWneg=0.;
	for(int ientry = 0; ientry < nEntriesRV; ientry++){
	  dataRV->get(ientry);
	  if(dataRV->weight()>0.){
	    sumWpos += dataRV->weight();
	  }
	  else{
	    sumWneg += dataRV->weight();
	  }
	}
	double fracNeg=0.;
	if(sumWpos - sumWneg != 0.){
	  fracNeg = sumWneg/(sumWpos - sumWneg);
	}
	if( (1-abs(fracNeg))*nEntriesRV < minConditions ){
	  belowMinConditionsRV=true;
	}
	std::cout<<"conditions for fit : "<< (1-abs(fracNeg))*nEntriesRV <<", it falls below min?  "<<belowMinConditionsRV<<std::endl;
	
	
	sumWpos=0.;
	sumWneg=0.;
	for(int ientry = 0; ientry < nEntriesWV; ientry++){
	  dataWV->get(ientry);
	  if(dataWV->weight()>0.){
	    sumWpos += dataWV->weight();
	  }
	  else{
	    sumWneg += dataWV->weight();
	  }
	}
	fracNeg=0.;
	if(sumWpos - sumWneg != 0.){
	  fracNeg = sumWneg/(sumWpos - sumWneg);
	}
	if( (1-abs(fracNeg))*sEntriesWV < minConditions ) belowMinConditionsWV=true;
      }
      
      if(nEntriesRV < minNevts) tooFewEntriesRV=true;
      if(sEntriesRV < 0) negSumEntriesRV=true;
      if(nEntriesWV < minNevts) tooFewEntriesWV=true;
      if(sEntriesWV < 0) negSumEntriesWV=true;
      std::cout<<"Looking at mh = "<<mh<<", entries flags are:"<<std::endl;
      std::cout<<"tooFewEntriesRV "<<tooFewEntriesRV<<std::endl;
      std::cout<<"negSumEntriesRV "<<negSumEntriesRV<<std::endl;
      std::cout<<"tooFewEntriesWV "<<tooFewEntriesWV<<std::endl;
      std::cout<<"negSumEntriesWV "<<negSumEntriesWV<<std::endl;
    }
    
    
    
    
    
    for (int mh=mhLow_; mh<=mhHigh_; mh+=5){
      if (skipMass(mh)) continue;
      RooDataSet *dataRV; 
      RooDataSet *dataWV; 
      RooDataSet *dataRVRef; 
      RooDataSet *dataWVRef; 
      RooDataSet *dataRef;  // MDDB not used
      RooDataSet *data0Ref;  
      RooDataSet *data;  
      RooDataHist *dataH;   // MDDB not used

      //VRT 16.05.17: attempt to adapt to differentials naming convenction
      string proctemp = proc;
      string procWmassAndE;
      if(proctemp.find_first_of("_") != string::npos){
	procWmassAndE = proctemp.insert(proctemp.find_first_of("_"), Form("_%d_13TeV", mh) );
      }
      else{
	procWmassAndE = proctemp.append( Form("_%d_13TeV", mh) );
      }
      
      if (verbose_)std::cout << "[INFO] Opening dataset called "<< Form("%s_%s",procWmassAndE.c_str(),cat.c_str()) << " in in WS " << inWS << std::endl;
      RooDataSet *data0   = reduceDataset((RooDataSet*)inWS->data(Form("%s_%s",procWmassAndE.c_str(),cat.c_str())));

      //      if (verbose_)std::cout << "[INFO] Opening dataset called "<< Form("%s_%d_13TeV_%s",proc.c_str(),mh,cat.c_str()) << " in in WS " << inWS << std::endl;
      //      RooDataSet *data0   = reduceDataset((RooDataSet*)inWS->data(Form("%s_%d_13TeV_%s",proc.c_str(),mh,cat.c_str())));

      if (beamSpotReweigh_){
	data = beamSpotReweigh(intLumiReweigh(data0));
      } else {
	data = intLumiReweigh(data0);
      }
      if (verbose_) std::cout << "[INFO] Old dataset (before intLumi change): " << *data0 << std::endl;
      
      dataRV = rvwvDataset(data,"RV"); 
      dataWV = rvwvDataset(data,"WV"); 
      
      if(negSumEntriesWV){
	dataWV = (RooDataSet*) dataWV->emptyClone()->reduce(RooArgSet(*mass_, *dZ_));
      }
      
      // // BASIC CHECK
      // if (mh == 125) {
      // 	for (unsigned int i=0 ; i < dataRV->numEntries() ; i++){	
      // 	  if (dataRV->get(i)->getRealValue("CMS_hgg_mass") > rangeLow && dataRV->get(i)->getRealValue("CMS_hgg_mass") <rangeHigh ){
      // 	    mass_->setVal(dataRV->get(i)->getRealValue("CMS_hgg_mass"));
      // 	    RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
      // 	    weight0->setVal(dataRV->weight() ); 
      // 	    dZ_->setVal(dataRV->get(i)->getRealValue("dZ"));
      // 	    cout << "BASIC " << mass_->getVal() << " " << dZ_->getVal() << " " << weight0->getVal() << endl;
      // 	  }
      // 	}
      // }
      if (verbose_) std::cout << "[INFO] Datasets ? " << *data << std::endl;
      if (verbose_) std::cout << "[INFO] Datasets (right vertex) ? " << *dataRV << std::endl;
      if (verbose_) std::cout << "[INFO] Datasets (wrong vertex) ? " << *dataWV << std::endl;
      
      float nEntriesRV =dataRV->numEntries();
      std::cout << "RV numEntries " << nEntriesRV << std::endl;
      float sEntriesRV= dataRV->sumEntries();
      std::cout << "RV sumEntries " << sEntriesRV << std::endl;
      float nEntriesWV =dataWV->numEntries();
      std::cout << "WV numEntries " << nEntriesWV << std::endl;
      float sEntriesWV= dataWV->sumEntries(); // count the number of entries and total weight on the RV/WV datasets
      std::cout << "WV sumEntries " << sEntriesWV << std::endl;
      
      // if there are few atcual entries or if there is an  overall negative sum of weights...
      // or if it was specified that one should use the replacement dataset, then need to replace!
      //      if (nEntriesRV <   minNevts  || sEntriesRV < 0 || ( userSkipRV) || offDiagonal ){
      //      if (tooFewEntriesRV  || negSumEntriesRV || ( userSkipRV) || offDiagonal ){ //the flags for entries make sure the replacement happens for all mass points if only one falls below threshold
      if (tooFewEntriesRV  || negSumEntriesRV || belowMinConditionsRV  || ( userSkipRV) ){ //the flags for entries make sure the replacement happens for all mass points if only one falls below threshold
	std::cout << "[INFO] too few entries to use for fits in RV! nEntries " << nEntriesRV << " sumEntries " << sEntriesRV << " userSkipRV " << userSkipRV<< std::endl;
	isProblemCategory=true;        
	if( belowMinConditionsRV && !noSkip_ ) isToSkip=true;
	std::cout<<"proc, cat: "<<proc<<", "<<cat<<std::endl;
	int thisProcCatIndex = getIndexOfReferenceDataset(proc,cat);
        
	string replancementProc = map_replacement_proc_rv_[thisProcCatIndex];
	string replancementCat = map_replacement_cat_rv_[thisProcCatIndex];
	std::cout<<"repl proc, repl cat: "<<replancementProc<<", "<<replancementCat<<std::endl;
	int replacementIndex = getIndexOfReferenceDataset(replancementProc,replancementCat);
	nGaussiansRV= map_nG_rv_[replacementIndex]; // if ==-1, want it to stay that way!
	std::cout << "[INFO] try to use  dataset for " << replancementProc << ", " << replancementCat << " instead."<< std::endl;
	//pick the dataset for the replacement proc and cat, reduce it (ie remove pdfWeights etc) ,
	//reweight for lumi, and then get the RV events only.

	string replproctemp = replancementProc;
	string replprocWmassAndE;
	if(replproctemp.find_first_of("_") != string::npos){
	  replprocWmassAndE = replproctemp.insert(replproctemp.find_first_of("_"), Form("_%d_13TeV", mh) );
	}
	else{
	  replprocWmassAndE = replproctemp.append( Form("_%d_13TeV", mh) );
	}

	if(beamSpotReweigh_){

	  data0Ref   = beamSpotReweigh(
				       rvwvDataset(
						   intLumiReweigh(
								  reduceDataset(
										//										(RooDataSet*)inWS->data(Form("%s_%d_13TeV_%s",replancementProc.c_str(),mh,replancementCat.c_str()))
										(RooDataSet*)inWS->data(Form("%s_%s",replprocWmassAndE.c_str(),replancementCat.c_str()))
										)
								  ), "RV"
						   )
				       );
	  
	} else {
	  data0Ref   = rvwvDataset(
				   intLumiReweigh(
						  reduceDataset(
								//		(RooDataSet*)inWS->data(Form("%s_%d_13TeV_%s",replancementProc.c_str(),mh,replancementCat.c_str()))
		(RooDataSet*)inWS->data(Form("%s_%s",replprocWmassAndE.c_str(),replancementCat.c_str()))
								)
						  ), "RV"
				   );
	}
	if (data0Ref) {
	  std::cout << "[INFO] Found replacement dataset for RV:" << *data0Ref<< std::endl;
	} else {
	  std::cout << "[ERROR] could not find replacement dataset for RV... " <<  std::endl;
	  exit(1);
	}
	
	dataRVRef=(RooDataSet*) data0Ref->Clone();
	std::cout << "[INFO] RV: replacing dataset for FITTING with new one ("<< *dataRVRef <<"), but keeping name of "<< *data0 << std::endl;
	dataRVRef->SetName(data0->GetName()); // MDDB why this was commented out ?
      } else { // if the dataset was fine to begin with, make the reference dataset the original
	dataRVRef=(RooDataSet*) dataRV->Clone();
      }
      
      
      // if there are few atcual entries or if there is an  overall negative sum of weights...
      // or if it was specified that one should use the replacement dataset, then need to replace!
      //      if (nEntriesWV < minNevts || sEntriesWV < 0 || (userSkipWV)){
      if (tooFewEntriesWV || negSumEntriesWV || (userSkipWV)){ //the flags for entries make sure the replacement happens for all mass points if only one falls below threshold \\ || offDiagonal
	std::cout << "[INFO] too few entries to use for fits in WV! nEntries " << nEntriesWV << " sumEntries " << sEntriesWV << "userSkipWV " << userSkipWV << std::endl;
        
	//things are simpler this time, since almost all WV are bad aside from ggh-UntaggedTag3
	//and anyway the shape of mgg in the WV shoudl be IDENTICAL across all Tags.
	//	int replacementIndex = getIndexOfReferenceDataset(referenceProcWV_,referenceTagWV_);
	
	////VRT, 3.6.2016: for differentials, it's probably better to have a WV reference for each group of sigmaM/M cats, thus we use ad hoc replacementcat/proc instead of references
	int thisProcCatIndex = getIndexOfReferenceDataset(proc,cat);
	//
	std::cout<<"found proccat index "<<thisProcCatIndex<<std::endl;
	std::cout<<"size of repl proc map wv "<<map_replacement_proc_wv_.size()<<std::endl;
	std::cout<<"size of repl cat map wv "<<map_replacement_cat_wv_.size()<<std::endl;
	string replancementProc = map_replacement_proc_wv_[thisProcCatIndex];
	string replancementCat = map_replacement_cat_wv_[thisProcCatIndex];
	int replacementIndex = getIndexOfReferenceDataset(replancementProc,replancementCat);
	
        nGaussiansWV= map_nG_wv_[replacementIndex]; 
        
	//pick the dataset for the replacement proc and cat, reduce it (ie remove pdfWeights etc) ,
	//reweight for lumi and then get the WV events only.

	string replproctemp = replancementProc;
	string replprocWmassAndE;
	if(replproctemp.find_first_of("_") != string::npos){
	  replprocWmassAndE = replproctemp.insert(replproctemp.find_first_of("_"), Form("_%d_13TeV", mh) );
	}
	else{
	  replprocWmassAndE = replproctemp.append( Form("_%d_13TeV", mh) );
	}
	
	if (beamSpotReweigh_){

	  data0Ref   = beamSpotReweigh( 
				       rvwvDataset(
						   intLumiReweigh(
								  reduceDataset(
										//	(RooDataSet*)inWS->data(Form("%s_%d_13TeV_%s",referenceProcWV_.c_str(),mh,referenceTagWV_.c_str()))
										//										(RooDataSet*)inWS->data(Form("%s_%d_13TeV_%s",replancementProc.c_str(),mh,replancementCat.c_str()))
										(RooDataSet*)inWS->data(Form("%s_%s",replprocWmassAndE.c_str(),replancementCat.c_str()))
										)
								  ), "WV"
						   )
					);
	} else {
	  data0Ref   = rvwvDataset(
				   intLumiReweigh(
						  reduceDataset(
								//(RooDataSet*)inWS->data(Form("%s_%d_13TeV_%s",referenceProcWV_.c_str(),mh,referenceTagWV_.c_str()))
								//(RooDataSet*)inWS->data(Form("%s_%d_13TeV_%s",replancementProc.c_str(),mh,replancementCat.c_str()))
(RooDataSet*)inWS->data(Form("%s_%s",replprocWmassAndE.c_str(),replancementCat.c_str()))
								)
						  ), "WV"
				   );
	}
	if (data0Ref) {
	  std::cout << "[INFO] Found replacement dataset for WV:" << *data0Ref<< std::endl;
	} else { // if the dataset was fine to begin with, make the reference dataset the original
	  std::cout << "[ERROR] could not find replacement dataset for WV... " <<  std::endl;
	  exit(1);
	}
	
	dataWVRef = (RooDataSet*) data0Ref->Clone();
	std::cout << "[INFO] WV: replacing dataset for FITTING with new one ("<< *dataWVRef <<"), but keeping name of "<< *data0 << std::endl;
	dataWVRef->SetName(data0->GetName()); // MDDB why this was commented out ?
      } else {
	dataWVRef=(RooDataSet*) dataWV->Clone();
      }
      
      
      if (verbose_) std::cout << "[INFO] inserting regular RV dataset " << *dataRV<< std::endl;
      datasetsRV.insert(pair<int,RooDataSet*>(mh,dataRV));
      if (verbose_) std::cout << "[INFO] inserting regular WV datasets " << *dataWV<< std::endl;
      datasetsWV.insert(pair<int,RooDataSet*>(mh,dataWV));
      if (verbose_) std::cout << "[INFO] inserting FIT RVdatasets " << *dataRVRef << std::endl;
      FITdatasetsRV.insert(pair<int,RooDataSet*>(mh,dataRVRef));
      if (verbose_) std::cout << "[INFO] inserting FIT WVdatasets" << *dataWVRef << std::endl;
      FITdatasetsWV.insert(pair<int,RooDataSet*>(mh,dataWVRef));
      if (verbose_)std::cout << "[INFO] inserting refular RV+WV " << *data << std::endl;
      datasets.insert(pair<int,RooDataSet*>(mh,data));
      if (verbose_) std::cout << "[INFO] Original Dataset: "<< *data << std::endl;
    }//closes loop on masses
    
    //check consistency of the three datasets!!
    TString check="";
    for (std::map<int,RooDataSet*>::iterator it=FITdatasetsRV.begin(); it!=FITdatasetsRV.end(); ++it){
      if (check=="") {
	TString name=it->second->GetName();
	std::cout<<"print fitdatasetsrv "<<std::endl;
	it->second->Print();
	cout << "MDDB check RV name " << name << endl; 
	
	//	check = name.ReplaceAll(TString(Form("%d",it->first)),TString(""));
	//	b.Replace(b.Index("125",3),3,"",0)
	check = name.Replace(name.Index( Form("%d_",it->first), 5 ), 3, "", 0  );
	cout << "MDDB check RV == null " << check << endl; 
      } else {
	TString name=it->second->GetName();
	TString bareName = name.Replace(name.Index( Form("%d_",it->first), 5 ), 3, "", 0  );
	cout << "MDDB check RV else check = " << check << "   name = " << name << "    replaceAll = " << bareName // << "    replaceAll2 = " << name.Replace(name.Index( Form("%d_",it->first), 5 ), 3, "", 0  )
	     << endl; 
	assert (check == bareName );
      }
    }
    check="";
    for (std::map<int,RooDataSet*>::iterator it=FITdatasetsWV.begin(); it!=FITdatasetsWV.end(); ++it){
      if (check=="") {
	TString name=it->second->GetName();
	cout << "MDDB check WV name " << name << endl; 
        check = name.Replace(name.Index( Form("%d_",it->first), 5 ), 3, "", 0  );
	cout << "MDDB check WV == null " << check << endl; 
      } else {
	TString name=it->second->GetName();
	TString bareName = name.Replace(name.Index( Form("%d_",it->first), 5 ), 3, "", 0  );
	cout << "MDDB check WV else check = " << check << "   name = " << name << "    replaceAll = " << bareName // << "    replaceAll2 = " << name.Replace(name.Index( Form("%d_",it->first), 5 ), 3, "", 0  )
	     << endl; 
	assert (check == bareName );
      }
    }
    
    // these guys do the fitting
    // right vertex
    if (verbose_) std::cout << "[INFO] preapraing initialfit RV" << std::endl;
    mass_->Print();
    MH->Print() ;
    std::cout<<"data125 mean in signalfit.cpp "<<FITdatasetsRV[125]->mean(*mass_)<<std::endl;
    std::cout<<"binned data125 mean in signalfit.cpp "<<FITdatasetsRV[125]->binnedClone(Form("%s_binned",FITdatasetsRV[125]->GetName()) )->mean(*mass_)<<std::endl;
    InitialFit initFitRV(mass_,MH,mhLow_,mhHigh_,skipMasses_,binnedFit_,nBinsFit_); //     InitialFit initFitRV(mass_,MH,mhLow_,mhHigh_,skipMasses_,binnedFit_,nBins_);
    initFitRV.setVerbosity(verbose_);
    if (!cloneFits_) {
      if (verbose_) std::cout << "[INFO] RV building sum of gaussians with nGaussiansRV " << nGaussiansRV << std::endl;
      if (verbose_) cout << " INITFIT " << Form("%s_%s",proc.c_str(),cat.c_str()) << " " << nGaussiansRV <<  " " << recursive_ << endl;
      initFitRV.buildSumOfGaussians(Form("%s_%s",proc.c_str(),cat.c_str()),nGaussiansRV,recursive_);
      initFitRV.setDatasets(FITdatasetsRV);
      initFitRV.setDatasetsSTD(datasetsRV);
      if (verbose_) cout << "[INFO] fit setup: mhLow = " << mhLow_ << " ; mhHigh = " << mhHigh_ << " ; binned = " << binnedFit_ << " ; nBinsFit = " << nBinsFit_ << endl;      
      if (verbose_) std::cout << "[INFO] RV running fits" << std::endl;
      if (verbose_) std::cout << "[INFO] Initial fits only? " << runInitialFitsOnly_ << std::endl;
      if (verbose_) std::cout << "[INFO] Replace? " << replace_ << std::endl;
      initFitRV.runFits(ncpu_);
      //      if (!runInitialFitsOnly_ && !replace_) { //are we sure replace_ is just not obsolete?
      
      if (!runInitialFitsOnly_ ) { 
	if(verbose_) std::cout<<"[INFO] Writing to file dat/in_"<<proc.c_str()<<"_"<<cat.c_str()<<"_rv.dat initial parameters"<<std::endl;
        initFitRV.saveParamsToFileAtMH(Form("dat/in_%s_%s_rv.dat",proc.c_str(),cat.c_str()),constraintValueMass_);
	if(verbose_) std::cout<<"[INFO] Writing to file dat/in_"<<proc.c_str()<<"_"<<cat.c_str()<<"_rv.dat initial parameters"<<std::endl;
        initFitRV.loadPriorConstraints(Form("dat/in_%s_%s_rv.dat",proc.c_str(),cat.c_str()),constraintValue_);
	if(!userSkipRV){
	  initFitRV.runFits(ncpu_);
	}
	if(( userSkipRV || offDiagonal) && shiftOffDiag_){
	  std::cout << "userSkipRV is " << userSkipRV <<" and offDiagonal is " << offDiagonal <<", so after fitting we shift dm by the scale bias" << std::endl;
	  initFitRV.shiftScale();	
	}
	
      }
      
      if( replace_ ) {
        initFitRV.setFitParams(allParameters[replaceWith_rv_].first); 
      }
      if (!skipPlots_) initFitRV.plotFits(Form("%s/initialFits/%s_%s_rv",plotDir_.c_str(),proc.c_str(),cat.c_str()),"RV");
    }
    //MDDB save params to check that the gaussian parameters are the same as the ones found in SignalFit
    system(Form("mkdir -p %s/../SigFit_params",plotDir_.c_str()));   // MDDB a bit silly path...
    initFitRV.saveParamsToFileAtMH(Form("%s/../SigFit_params/SigFit_params_RV_proc_%s_cat_%s_g%d.txt", plotDir_.c_str(), proc.c_str(), cat.c_str(), nGaussiansRV),125);
    
    parlist_t fitParamsRV = initFitRV.getFitParams();
    
    // wrong vertex
    if (verbose_) std::cout << "[INFO] preparing initialfit WV" << std::endl;
    mass_->Print();
    MH->Print() ;
    
    InitialFit initFitWV(mass_,MH,mhLow_,mhHigh_,skipMasses_,binnedFit_,nBinsFit_); //     InitialFit initFitWV(mass_,MH,mhLow_,mhHigh_,skipMasses_,binnedFit_,nBins_);
    
    initFitWV.setVerbosity(verbose_);
    if (!cloneFits_) {
      if (verbose_) std::cout << "[INFO] WV building sum of gaussians wth nGaussiansWV "<< nGaussiansWV << std::endl;
      initFitWV.buildSumOfGaussians(Form("%s_cat%s",proc.c_str(),cat.c_str()),nGaussiansWV,recursive_);
      if (verbose_) std::cout << "[INFO] WV setting datasets in initial FIT " << std::endl;
      initFitWV.setDatasets(FITdatasetsWV);
      initFitWV.setDatasetsSTD(datasetsWV);
      
      if (verbose_) cout << "[INFO] fit setup: mhLow = " << mhLow_ << " ; mhHigh = " << mhHigh_ << " ; binned = " << binnedFit_ << " ; nBinsFit = " << nBinsFit_ << endl;      
      initFitWV.runFits(ncpu_);
      
      //      if (!runInitialFitsOnly_ && !replace_) {
      if (!runInitialFitsOnly_) {
	if(verbose_) std::cout<<"[INFO] Writing to file dat/in_"<<proc.c_str()<<"_"<<cat.c_str()<<"_wv.dat initial parameters"<<std::endl;
        initFitWV.saveParamsToFileAtMH(Form("dat/in/%s_%s_wv.dat",proc.c_str(),cat.c_str()),constraintValueMass_);
	if(verbose_) std::cout<<"[INFO] Writing to file dat/in_"<<proc.c_str()<<"_"<<cat.c_str()<<"_wv.dat initial parameters"<<std::endl;
        initFitWV.loadPriorConstraints(Form("dat/in/%s_%s_wv.dat",proc.c_str(),cat.c_str()),constraintValue_);
	if(!userSkipWV){
	  initFitWV.runFits(ncpu_);
	}
	if(( userSkipWV || offDiagonal) && shiftOffDiag_ && WVoverRVat125 >0.07){
	  std::cout << "WV/RV ratio at 125 "<< WVoverRVat125 << std::endl;
	  std::cout << "userSkipWV is " << userSkipWV <<" and offDiagonal is " << offDiagonal <<", so after fitting we shift dm by the scale bias" << std::endl;
	  initFitWV.shiftScale();	
	}
      }
      if( replace_ ) {
        initFitWV.setFitParams(allParameters[replaceWith_wv_].second); 
      }
      if (!skipPlots_) initFitWV.plotFits(Form("%s/initialFits/%s_%s_wv",plotDir_.c_str(),proc.c_str(),cat.c_str()),"WV");
    }
    //MDDB save params to check that the gaussian parameters are the same as the ones found in SignalFit
    cout << "MDDB save parameters WV "  << endl; 
    initFitWV.saveParamsToFileAtMH(Form("%s/../SigFit_params/SigFit_params_WV_proc_%s_cat_%s_g%d.txt",plotDir_.c_str() , proc.c_str(), cat.c_str(), nGaussiansWV),125);
    
    parlist_t fitParamsWV = initFitWV.getFitParams();
    
    allParameters[ make_pair(proc,cat) ] = make_pair(fitParamsRV,fitParamsWV);
    
    //Ok, now that we have made the fit parameters eitehr with the regular dataset or the replacement one.
    // Now we should be using the ORIGINAL dataset
    if (!runInitialFitsOnly_) {
      //these guys do the interpolation
      map<string,RooSpline1D*> splinesRV;
      map<string,RooSpline1D*> splinesWV;
      
      if (!cloneFits_){
        // right vertex
        LinearInterp linInterpRV(MH,mhLow_,mhHigh_,fitParamsRV,doSecondaryModels_,skipMasses_);
        linInterpRV.setVerbosity(verbose_);
        linInterpRV.setSecondaryModelVars(MH_SM,DeltaM,MH_2,higgsDecayWidth);
        linInterpRV.interpolate(nGaussiansRV);
        splinesRV = linInterpRV.getSplines();
	
        // wrong vertex
        LinearInterp linInterpWV(MH,mhLow_,mhHigh_,fitParamsWV,doSecondaryModels_,skipMasses_);
        linInterpWV.setVerbosity(verbose_);
        linInterpWV.setSecondaryModelVars(MH_SM,DeltaM,MH_2,higgsDecayWidth);
        linInterpWV.interpolate(nGaussiansWV);
        splinesWV = linInterpWV.getSplines();
      }
      else {
        splinesRV = cloneSplinesMapRV[make_pair(proc,cat)];
        splinesWV = cloneSplinesMapWV[make_pair(proc,cat)];
      }
      // this guy constructs the final model with systematics, eff*acc etc.
      if (isFlashgg_){
	
        outWS->import(*intLumi_);
	RooRealVar* MHref = MH;
	if(MHref_>0.){
	  MHref = (RooRealVar*)MH->Clone("MHref");
	  MHref->setVal(MHref_);
	  MHref->setConstant(true);
	}
	std::cout << "Syst file name in SignalFit.cpp "<<systfilename_<<std::endl; 
        FinalModelConstruction finalModel(mass_,MH,MHref,intLumi_,mhLow_,mhHigh_,proc,cat,doSecondaryModels_,systfilename_,skipMasses_,verbose_,procs_, flashggCats_,plotDir_, isProblemCategory, isToSkip, isCutBased_,sqrts_,doQuadraticSigmaSum_);
	
        finalModel.setSecondaryModelVars(MH_SM,DeltaM,MH_2,higgsDecayWidth);
        finalModel.setRVsplines(splinesRV);
        finalModel.setWVsplines(splinesWV);
        finalModel.setRVdatasets(datasetsRV);
        finalModel.setWVdatasets(datasetsWV);
        finalModel.setFITRVdatasets(FITdatasetsRV);
        finalModel.setFITWVdatasets(FITdatasetsWV);
        //finalModel.setSTDdatasets(datasets);
        finalModel.makeSTDdatasets();
        finalModel.makeFITdatasets();
        if( isFlashgg_){
          finalModel.buildRvWvPdf("hggpdfsmrel_13TeV",nGaussiansRV,nGaussiansWV,recursive_);
        }
        finalModel.getNormalization(normalisationCut_);
        if (!skipPlots_) finalModel.plotPdf(plotDir_);
        finalModel.save(outWS);
      }
    }
  }
  datfile.close();
  
  sw.Stop();
  cout << "[INFO] Whole fitting process took..." << endl;
  cout << "\t";
  sw.Print();
  
  if (!runInitialFitsOnly_) { 
    sw.Start();
    cout << "[INFO] Starting to combine fits..." << endl;
    // this guy packages everything up
    WSTFileWrapper *outWSWrapper = new WSTFileWrapper(outFile, outWS);
    string skipProc; 
    if (map_proc_.size() > 2){
      skipProc = map_proc_[2];
    } else {
      skipProc = map_proc_[0];
    }
    Packager packager(outWSWrapper, outWS,procs_,nCats_,mhLow_,mhHigh_,skipMasses_,sqrts_,skipPlots_,plotDir_,mergeWS,cats_,flashggCats_,skipProc);
    
    // if we are doing jobs for each proc/tag, want to do the split.
    bool split = 0;
    std::cout << "Split size " << split_.size() << std::endl;
    std::cout << "split_ " << split_[0] << std::endl;
    if (split_.size() == 2){
      split=1;
      packager.packageOutput(/*split*/split, /*proc*/split_[0], /*tag*/ split_[1] );
    } else {
      packager.packageOutput(/*split*/split, /*proc*/procs_[0], /*tag*/ flashggCats_[0] );
    }
    sw.Stop();
    cout << "[INFO] Combination complete." << endl;
    cout << "[INFO] Whole process took..." << endl;
    cout << "\t";
    sw.Print();
  }
  
  cout << "[INFO] Writing to file..." << endl;
  outFile->cd();
  outWS->Write();
  outFile->Close();
  //	inFile->Close();
  inWS->Close();
  cout << "[INFO] Done." << endl;
  
  
  return 0;
}
