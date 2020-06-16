#include <iostream>
#include <string>
#include <map>
#include <vector>

#include "TFile.h"
#include "TMath.h"
#include "TCanvas.h"
#include "TH1F.h"
#include "TLegend.h"
#include "TF1.h"

#include "TStopwatch.h"
#include "RooWorkspace.h"
#include "RooDataSet.h"
#include "RooDataHist.h"
#include "RooAddPdf.h"
#include "RooGaussian.h"
#include "RooRealVar.h"
#include "RooFormulaVar.h"
#include "RooFitResult.h"
#include "RooPlot.h"
#include "RooMsgService.h"
#include "RooMinuit.h"
#include "RooMoment.h"
#include "RooHist.h"
#include "TFitResult.h"

#include "boost/program_options.hpp"
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/predicate.hpp"

#include "../interface/WSTFileWrapper.h"
#include "../interface/InitialFit.h"

#include "../../tdrStyle/tdrstyle.C"
#include "../../tdrStyle/CMS_lumi.C"

using namespace std;
using namespace RooFit;
using namespace boost;
namespace po = boost::program_options;

string filename_;
string datfilename_;
string json_dict_;
string outdir_;
int mass_;
string procString_;
int ncats_;
bool recursive_=false;
string flashggCatsStr_;
vector<string> flashggCats_;
string considerOnlyStr_;
vector<string> considerOnly_;
bool forceFracUnity_=false;
bool isFlashgg_;
bool verbose_;
bool unbinnedFit_;
RooRealVar *mass ;
RooRealVar *dZ; 

// set range to be the same as SigfitPlots
// want quite a large range otherwise don't
// see crazy bins on the sides
float rangeLow = 115;   // make this configurable and in Ftest too
float rangeHigh = 135;

void OptionParser(int argc, char *argv[]){
  po::options_description desc1("Allowed options");
  desc1.add_options()
    ("help,h",                                                                                "Show help")
    ("infilename,i", po::value<string>(&filename_),                                           "Input file name")
    ("datfilename,d", po::value<string>(&datfilename_)->default_value("dat/config.dat"),      "Output configuration file")
    ("outdir,o", po::value<string>(&outdir_)->default_value("plots"),      "Output configuration file")
    ("json_dict,j", po::value<string>(&json_dict_)->default_value(""),      "Output configuration file")
    ("mass,m", po::value<int>(&mass_)->default_value(125),                                    "Mass to run at")
    ("procs,p", po::value<string>(&procString_)->default_value("ggh,vbf,wh,zh,tth"),          "Processes")
    ("recursive",																																							"Recursive fraction")
    ("forceFracUnity",																																				"Force fraction unity")
    ("isFlashgg",	po::value<bool>(&isFlashgg_)->default_value(true),													"Use flashgg format")
    ("verbose",     po::bool_switch(&verbose_), "verbose")
    ("unbinnedFit",     po::bool_switch(&unbinnedFit_), "Binned/Unbinned Fit")
    ("flashggCats,f", po::value<string>(&flashggCatsStr_)->default_value("UntaggedTag_0,UntaggedTag_1,UntaggedTag_2,UntaggedTag_3,UntaggedTag_4,VBFTag_0,VBFTag_1,VBFTag_2,TTHHadronicTag,TTHLeptonicTag,VHHadronicTag,VHTightTag,VHLooseTag,VHEtTag"),       "Flashgg category names to consider")
    ("considerOnly", po::value<string>(&considerOnlyStr_)->default_value("All"), 
     "If you wish to only consider a subset cat in the list, list them as separated by commas. ")
    ;

  po::options_description desc("Allowed options");
  desc.add(desc1);

  po::variables_map vm;
  po::store(po::parse_command_line(argc,argv,desc),vm);
  po::notify(vm);
  if (vm.count("help")){ cout << desc << endl; exit(1);}
  if (vm.count("recursive")) recursive_=true;
  if (vm.count("forceFracUnity")) forceFracUnity_=true;
}

void plot(string outPath, int mh, RooRealVar *var, RooAbsData *data, RooAbsPdf *pdf){

  TCanvas *canv = new TCanvas("c","c",1000,1000);
  RooPlot *plot = var->frame(Range(mh-10,mh+10));
  data->plotOn(plot);
  pdf->plotOn(plot);
  plot->Draw();
  canv->Print(Form("%s.pdf",outPath.c_str()));
}

double getMyNLL(RooRealVar *var, RooAbsPdf *pdf, RooDataHist *data){
  RooPlot *plot = var->frame();
  data->plotOn(plot);
  pdf->plotOn(plot);
  RooCurve *pdfCurve = (RooCurve*)plot->getObject(plot->numItems()-1);
  double sum=0.;
  for (int i=0; i<data->numEntries(); i++){
    double binCenter = data->get(i)->getRealValue("CMS_hgg_mass");
    double weight = data->weight();
    sum+=TMath::Log(TMath::Poisson(100.*weight,100.*pdfCurve->Eval(binCenter)));
  }
  return -1.*sum;
}


RooDataSet * reduceDataset(RooDataSet *data0){

  RooDataSet *data = (RooDataSet*) data0->emptyClone()->reduce(RooArgSet(*mass, *dZ));
  RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
  for (unsigned int i=0 ; i < data0->numEntries() ; i++){
    if (data0->get(i)->getRealValue("CMS_hgg_mass") > rangeLow && data0->get(i)->getRealValue("CMS_hgg_mass") <rangeHigh ){
      mass->setVal(data0->get(i)->getRealValue("CMS_hgg_mass"));
      weight0->setVal(data0->weight() ); // <--- is this correct?
      dZ->setVal(data0->get(i)->getRealValue("dZ"));
      data->add( RooArgList(*mass, *dZ, *weight0), weight0->getVal() );
    }
  }
return data;
}


RooDataSet * rvwvDataset(RooDataSet *data0, string rvwv){

  RooDataSet *dataRV = (RooDataSet*) data0->emptyClone()->reduce(RooArgSet(*mass, *dZ));
  RooDataSet *dataWV = (RooDataSet*) data0->emptyClone()->reduce(RooArgSet(*mass, *dZ));
  RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
  for (unsigned int i=0 ; i < data0->numEntries() ; i++){
    if (data0->get(i)->getRealValue("CMS_hgg_mass") > rangeLow && data0->get(i)->getRealValue("CMS_hgg_mass") < rangeHigh ){
      mass->setVal(data0->get(i)->getRealValue("CMS_hgg_mass"));
      weight0->setVal(data0->weight() ); // <--- is this correct?
      dZ->setVal(fabs(data0->get(i)->getRealValue("dZ")));
      if (dZ->getVal() <1.){
	dataRV->add( RooArgList(*mass, *dZ, *weight0), weight0->getVal() );
      } else{
	dataWV->add( RooArgList(*mass, *dZ, *weight0), weight0->getVal() );
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

// double NConc(RooAddPdf *pdf, RooRealVar* x){

//   RooAbsReal* dpdfdx2 = pdf->derivative(x, 2);
  
// }

int main(int argc, char *argv[]){

  // Criteria to choose #gauss
  int   minNevts    = 500; // if below minNevts #gauss = -1  
  //  float myThresholdDist = 0.8; // how much better n+1 has to be wrt n
  float myThresholdDist = 0.8; // how much better n+1 has to be wrt n
  //  float myThresholdChi2 = 0.5; // how much better n+1 has to be wrt n
  float myThresholdChi2 = 0.8; // how much better n+1 has to be wrt n
  float itsOK       = 0.1; // if  myDistance < itsOK suggest the #gauss that fulfill this condition

  OptionParser(argc,argv);
  verbose_=true;
  if (verbose_) {
    std::cout << "[INFO] datfilename_	" << datfilename_ << std::endl;
  }
  if (verbose_) {
    std::cout << "[INFO] filename_	" << filename_ << std::endl;
  }

  TStopwatch sw;
  sw.Start();
 

  //make nice plots in ~correct style.
  setTDRStyle();
  writeExtraText = true;       // if extra text
  extraText  = "";  // default extra text is "Preliminary"
  lumi_8TeV  = "19.1 fb^{-1}"; // default is "19.7 fb^{-1}"
  lumi_7TeV  = "4.9 fb^{-1}";  // default is "5.1 fb^{-1}"
  lumi_sqrtS = "13 TeV";       // used with iPeriod = 0
                               //, e.g. for simulation-only plots
                               //(default is an empty string)

  
  //want binning of \0.5GeV for plots
  int   nBins    = 2* int(rangeHigh -rangeLow);  

  // Bins for fitting
  int nBinsFit = 160; // MDDB make it a parameter
	
  vector<int> skipMasses;	
  bool binnedFit = false;
  if (unbinnedFit_) binnedFit=false;
  
  // silence roofit
  RooMsgService::instance().setGlobalKillBelow(RooFit::ERROR);
  RooMsgService::instance().setSilentMode(true);

  // make output dir
  //std::cout<< "Making outdir " << outdir<<endl;
  system(Form("mkdir -p %s/fTest",outdir_.c_str()));
  std::cout<< "Outdir existing" << outdir_<<endl;
  // split procs, cats
  vector<string> procs;
  split(procs,procString_,boost::is_any_of(","));
  split(flashggCats_,flashggCatsStr_,boost::is_any_of(","));
  split(considerOnly_,considerOnlyStr_,boost::is_any_of(","));
  
  // automatically determine nCats from flashggCats input
  if (isFlashgg_){
    ncats_ =flashggCats_.size();
    // Ensure that the loop over the categories does not go out of scope. 
  } else {
    std::cout 
      << "[ERROR] Sorry, script not compatible with non-flashgg input! Exit." 
      << std::endl;
    exit (1);
  }

  // job splitting: can get this script to only consider specified tags
  for (unsigned int j =0; j <considerOnly_.size() ; j++){
    std::cout << " [INFO] considering only " << considerOnly_[j]<<std::endl;
  }

  // Open input files using WS wrapper.
  WSTFileWrapper *inWS 
    = new WSTFileWrapper(filename_,"cms_hgg_13TeV");
  if(verbose_) std::cout << "[INFO] Opened files OK!" << std::endl;

  mass = (RooRealVar*)inWS->var("CMS_hgg_mass");
  if(verbose_) std::cout << "[INFO] Got mass variable " << mass << std::endl;
  mass->setBins(nBins);
  mass->setRange(rangeLow,rangeHigh);
  dZ = (RooRealVar*)inWS->var("dZ");
 

  // duplicate MH variable ? need to remove this ?? LC
  RooRealVar *MH = new RooRealVar("MH","MH",mass_);
  MH->setVal(mass_);
  MH->setBins(nBins);
  MH->setRange(rangeLow,rangeHigh);
  MH->setConstant(true);
  
  // record chosen nGaussians... 
  // This is really a bit useless: we are still picking the nGaussians by eye.
  map<string,pair<int,int> > choices;
  map<string,vector<RooPlot*> > plots;
  map<string,vector<RooPlot*> > plotsRV;
  map<string,vector<RooPlot*> > plotsWV;
  
  // declare temporary Plots/Frames, one for each proc and cat to consider.
  if(verbose_) std::cout << "[INFO] start looping through nProcs " << procs.size() << " to book rooPlots " <<  std::endl;
  for (unsigned int p=0; p<procs.size(); p++){
    vector<RooPlot*> temp;
    vector<RooPlot*> tempRV;
    vector<RooPlot*> tempWV;
    if(verbose_) std::cout << "[INFO] on proc " << procs[p] <<  " start looping through nCats " << ncats_ << " to book RooPlots " <<  std::endl;
    for (int cat=0; cat<ncats_; cat++){
      //RooPlot *plotRV = mass->frame(Range(mass_-10,mass_+10));
      RooPlot *plotRV = mass->frame(Range(rangeLow,rangeHigh));
      plotRV->SetTitle(
		       Form("%s_%s_RV",procs[p].c_str(),flashggCats_[cat].c_str()));
      tempRV.push_back(plotRV);
      //RooPlot *plotWV = mass->frame(Range(mass_-10,mass_+10));
      RooPlot *plotWV = mass->frame(Range(rangeLow,rangeHigh));
      plotWV->SetTitle(
		       Form("%s_%s_WV",procs[p].c_str(),flashggCats_[cat].c_str()));
      tempWV.push_back(plotWV);

      //combine dataset
      RooPlot *plot = mass->frame(Range(rangeLow,rangeHigh));
      plot->SetTitle(
		     Form("%s_%s",procs[p].c_str(),flashggCats_[cat].c_str()));
      temp.push_back(plot);
    }
    plots.insert(pair<string,vector<RooPlot*> >(procs[p],temp));
    plotsRV.insert(pair<string,vector<RooPlot*> >(procs[p],tempRV));
    plotsWV.insert(pair<string,vector<RooPlot*> >(procs[p],tempWV));
  }


  for(map<string, vector<RooPlot*> >::const_iterator it = plotsRV.begin();it != plotsRV.end(); ++it)
    {
      for(int ip=0; ip < it->second.size(); ip++){
	std::cout<<"plotsRV"<<std::endl;
	std::cout << it->first << " " ;
	((RooPlot*)it->second[ip])->Print()  ;
	std::cout << std::endl;
      }
    }

  
  // decide what color to make the fits in output plots...
  vector<int> colors;
  colors.push_back(kBlue);
  colors.push_back(kRed);
  colors.push_back(kGreen+2);
  colors.push_back(kMagenta+1);

  // continue flag is used to tell the script to ignore some
  // cases if we are using a considerOnly option.
  bool continueFlag =0;
  if(verbose_) std::cout << "[INFO] start looping through nCats " << ncats_ << " to get datasets " <<std::endl;
  for (int cat=0; cat<ncats_; cat++){
    
    if ( (considerOnly_[0]).compare("All") != 0 ){
      for (unsigned int j =0; j <considerOnly_.size() ; j++){
	if ( (considerOnly_[j]).compare(flashggCats_[cat]) != 0) { 
	  std::cout << " [INFO] skipping " <<  flashggCats_[cat] << std::endl;
	  continueFlag=1;
	}
      }
    }
    
    if (continueFlag){ continueFlag=0; continue;}
    

	       
    if(verbose_) std::cout << "[INFO] on cat " << flashggCats_[cat] <<  " start looping through procs  " << procs.size() << " to get datasets " <<std::endl;
    // now main loop through processes...

    cout << "[TEX] \\documentclass[11pt]{article}" << endl;
    cout << "[TEX] \\usepackage{graphicx}" << endl;
    cout << "[TEX] \\usepackage{xcolor}" << endl;
    cout << "[TEX] \\begin{document}" << endl;
    cout << "[TEX] " << endl;
    cout << "[TEX] \\title{}" << endl;
    cout << "[TEX] \\author{}" << endl;
    cout << "[TEX] \\date{}" << endl;
    cout << "[TEX] \\maketitle" << endl;
    cout << "[TEX] " << endl;
    

    std::string catForTex;
    for (unsigned int p=0; p<procs.size(); p++){
      
      // get desired proc
      string proc = procs[p];

      //declare the datasets to use
      RooDataSet *data;  
      RooDataSet *dataRV;
      RooDataSet *dataWV; 
      
      // We want to reduce our datasets, so just get the most important vars.
      //      RooRealVar *mass = (RooRealVar*)inWS->var("CMS_hgg_mass");
      mass = (RooRealVar*)inWS->var("CMS_hgg_mass");
      //RooRealVar *dZ = (RooRealVar*)inWS->var("dZ");

      // RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
      // RooRealVar *dZ = new RooRealVar("dZ","dZ",-100000,1000000);
      // if (verbose_) std::cout << "[INFO] got roorealvars from ws ? mass " << mass << " dz " << dZ << std::endl;
      
      // access dataset and immediately reduce it!
      if (isFlashgg_){
	//VRT 16.05.17: attempt to adapt to differentials naming convenction
	string proctemp = proc;
	string procWmassAndE;
	if(proctemp.find_first_of("_") != string::npos){
	  procWmassAndE = proctemp.insert(proctemp.find_first_of("_"), Form("_%d_13TeV", mass_) );
	}
	else{
	  procWmassAndE = proctemp.append( Form("_%d_13TeV", mass_) );
	}
	if(verbose_){
	  std::cout<<"Looking for dataset "<<Form("%s_%s",procWmassAndE.c_str(),flashggCats_[cat].c_str())<<std::endl;
	  //std::cout<<"Looking for dataset "<<Form("%s_%d_13TeV_%s",proc.c_str(),mass_,flashggCats_[cat].c_str())<<std::endl;
	}
	RooDataSet *data0   = (RooDataSet*)inWS->data(
						      //						      Form("%s_%d_13TeV_%s",proc.c_str(),mass_,flashggCats_[cat].c_str()));
						      Form("%s_%s",procWmassAndE.c_str(),flashggCats_[cat].c_str()));
        if(verbose_) {
          std::cout << "[INFO] got dataset data0 ? " << data0 << "now make empty clones " << std::endl;
          if (data0) {
            std::cout << "[INFO] and it looks like this : " << *data0 << std::endl;
          } else {
            std::cout << "[INFO] but it is a null pointer! extit " << std::endl;
            exit (1);
          }
        }
        
	// this is to make it uniform with SignalFit


	//	data   = reduceDataset((RooDataSet*)inWS->data(Form("%s_%d_13TeV_%s",proc.c_str(),mass_,flashggCats_[cat].c_str())));
	data   = reduceDataset((RooDataSet*)inWS->data(Form("%s_%s",procWmassAndE.c_str(),flashggCats_[cat].c_str()) ) );

	dataRV = rvwvDataset(data,"RV"); 
	dataWV = rvwvDataset(data,"WV"); 
	
	// // BASIC CHECK
	// for (unsigned int i=0 ; i < dataRV->numEntries() ; i++){
	//   if (dataRV->get(i)->getRealValue("CMS_hgg_mass") > rangeLow && dataRV->get(i)->getRealValue("CMS_hgg_mass") <rangeHigh ){
	//     mass->setVal(dataRV->get(i)->getRealValue("CMS_hgg_mass"));
	//     RooRealVar *weight0 = new RooRealVar("weight","weight",-100000,1000000);
	//     weight0->setVal(dataRV->weight() ); 
	//     dZ->setVal(dataRV->get(i)->getRealValue("dZ"));
	//     cout << "BASIC " << mass->getVal() << " " << dZ->getVal() << " " << weight0->getVal() << endl;
	//   }
	// }	
      
        // data   = (RooDataSet*) data0->emptyClone()->reduce(RooArgSet(*mass, *dZ));
        // dataRV = (RooDataSet*) data0->emptyClone()->reduce(RooArgSet(*mass, *dZ));
        // dataWV = (RooDataSet*) data0->emptyClone()->reduce(RooArgSet(*mass, *dZ));

        // for (int i=0 ; i < data0->numEntries() ; i++){
	//   if (data0->get(i)->getRealValue("CMS_hgg_mass") > rangeLow && data0->get(i)->getRealValue("CMS_hgg_mass") < rangeHigh ){
        //     mass->setVal(data0->get(i)->getRealValue("CMS_hgg_mass"));
        //     weight0->setVal(data0->weight() ); // <--- is this correct?
        //     dZ->setVal(data0->get(i)->getRealValue("dZ"));
        //     data->add( RooArgList(*mass, *dZ, *weight0), weight0->getVal() );
        //     if (dZ->getVal() <1.){
	//       dataRV->add( RooArgList(*mass, *dZ, *weight0), weight0->getVal() );
        //     } else{
	//       dataWV->add( RooArgList(*mass, *dZ, *weight0), weight0->getVal() );
        //     }
	//   }
	// }

        //print out contents, if you want... 
	if (verbose_) {
	  std::cout << "[INFO] Workspace contains : " << std::endl;
	  std::list<RooAbsData*> data =  (inWS->allData()) ;
	  for (std::list<RooAbsData*>::const_iterator 
		 iterator = data.begin(), end = data.end();
	       iterator != end;
	       ++iterator) {
	    std::cout << **iterator << std::endl;
	    RooDataSet *dataset = dynamic_cast<RooDataSet *>( *iterator );
	    if (dataset) {
	      std::cout << "dataset / numEntries / sumEntries " <<  dataset->GetName() << ", " << dataset->numEntries() << ", " << dataset->sumEntries() << std::endl;
	    }
	  }
	}
        
        //more verbosity
	if (verbose_) {
          std::cout 
            << "[INFO] Retrieved combined RV/WV data " 
	    //            << Form("%s_%d_13TeV_%s",proc.c_str(),mass_,flashggCats_[cat].c_str()) 
	    << Form("%s_%s",procWmassAndE.c_str(),flashggCats_[cat].c_str())
            << "? "<< data<<std::endl;
	  std::cout<<"[DEBUGVRT] mean is "<<data->mean(*mass)<<std::endl;
        }
        
        // now split RV/WV scenarios
        // this method of reducing the dataset is not safe, I think it ignores
        // events with negative weights entirely!
        // See workaround above
	//dataRV = new RooDataSet("dataRV","dataRV",&*data,*(data->get()),"dZ<1");
	//dataWV = new RooDataSet("dataWV","dataWV",&*data,*(data->get()),"dZ>=1");
	if (verbose_) std::cout << "[INFO] Retrieved unreduced data"<< *data0  << std::endl;
	if (verbose_) std::cout<<"[DEBUGVRT] unreduced mean is "<<data0->mean(*mass)<<std::endl;
	if (verbose_) std::cout << "[INFO] Retrieved reduced   data"<< *data    << std::endl;
	if (verbose_) std::cout<<"[DEBUGVRT] reduced mean is "<<data->mean(*mass)<<std::endl;
	if (verbose_) std::cout << "[INFO] Retrieved reducedRV data"<< *dataRV  << std::endl;
	if (verbose_) std::cout<<"[DEBUGVRT] reducedRV mean is "<<dataRV->mean(*mass)<<std::endl;
	if (verbose_) std::cout << "[INFO] Retrieved reducedWV data"<< *dataWV  << std::endl;
	if (verbose_) std::cout<<"[DEBUGVRT] reducedWV mean is "<<dataWV->mean(*mass)<<std::endl;

      } else { // not flash!
      
        std::cout << "[ERROR] Sorry, only use flashgg input with this script."
		  << " Exit." << std::endl;
        exit(1);
      }

      if (dataRV->numEntries() > minNevts){
	  catForTex = boost::replace_all_copy(flashggCats_[cat], "_", "\\_");
	  auto procForTex = boost::replace_all_copy(proc, "_", "\\_");
	  cout << catForTex << endl;
	  cout <<  Form("[TEX] \\section{RV proc %s cat %s}", procForTex.c_str(), catForTex.c_str()) << endl;
	  cout << "[TEX] \\begin{tabular}{ l l l l l l l }" << endl;
	  cout << "[TEX] \\hline" << endl;
	  cout << "[TEX]   Order & Mean & Sigma & Dist & chi2 & entries & dist/entrie\\\\" << endl;
	}
      
      // which nGaussians do we want to choose?
      int rvChoice=0;
      int wvChoice=0;

      // right vertex
      int order=1;
      int prev_order=0;
      //      int cache_order=0;
      double thisNll=0.;
      //double prevNll=1.e6;
      double prevNll=0;
      double chi2=0.;
      double prob=0.;
      std::vector<pair<int,float> > rv_results;
      std::vector<pair<int,float> > rv_chi2;
      std::vector<pair<int,RooArgSet*> > rv_args;

      float rv_prob_limit =999;

      std::cout<<"before ploton"<<std::endl;
      std::cout<<"proc "<<proc<<"cat "<<cat<<std::endl;
      std::cout<<plotsRV[proc][cat]<<std::endl;
      dataRV->plotOn(plotsRV[proc][cat]);

      TCanvas *ccResRV  = new TCanvas("ccResRV","Residuals",600,600);
      TCanvas *ccPullRV = new TCanvas("ccPullRV","Pulls",600,600);
      TCanvas *ccpPullRV = new TCanvas("ccpPullRV","PullsProjections",600,600);
	
      RooDataHist * dataRVbinned = new RooDataHist("dataRVbinned","dataRVbinned",*mass,*dataRV); 
      if (verbose_) std::cout<<"[DEBUGVRT] dataRVbinned mean is "<<dataRVbinned->mean(*mass)<<std::endl;
      
      while (prob<rv_prob_limit && order <5){ // MDDB make it configurable
			  
	// Use the  InitialFit class that will be used later for the final signal fits
	int mhLow(125), mhHigh(125);
	if (verbose_) cout << "[INFO] Mass " << endl;
	mass->Print();
	MH->Print() ;

	InitialFit initFitRV(mass,MH,mhLow,mhHigh,skipMasses,binnedFit,nBinsFit);
	initFitRV.setVerbosity(5);
	bool recursive = true; 
	initFitRV.buildSumOfGaussians(Form("cat%d_g%d",cat,order),order,recursive);

	map<int,RooDataSet*> dataSets;
	int mh = 125; // just because you can have multiple mass points in InitialFit
	dataSets.insert(pair<int,RooDataSet*>(mh,dataRV));
	initFitRV.setDatasets(dataSets);    // one of these two is useless here
	initFitRV.setDatasetsSTD(dataSets); // one of these two is useless here

	if (verbose_) cout << "[INFO] fit setup: mh = " << mh << " ; mhLow = " << mhLow << " ; mhHigh = " << mhHigh << " ; binned = " << binnedFit << " ; nBinsFit = " << nBinsFit << endl;
	int ncpu = 1;
	initFitRV.runFits(ncpu);       
	// MDDB	mass->setBins(nBins); // InitialFit reset the binning to 160

	//MDDB save params to check that the gaussian parameters are the same as the ones found in SignalFit
	system(Form("mkdir -p %s/fTest_params",outdir_.c_str()));   // MDDB a bit silly path...
        initFitRV.saveParamsToFileAtMH(Form("%s/fTest_params/fTest_params_RV_proc_%s_cat_%s_g%d.txt",outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str(), order),125);
	
	RooFitResult *fitRes = 0;
	std::map<int,RooFitResult*> map_fitRes = initFitRV.getFitResults();
	std::map<int,RooFitResult*>::iterator it_fitres;
	it_fitres = map_fitRes.find(125);
        if (it_fitres != map_fitRes.end() ) fitRes = it_fitres->second;
	
	RooAddPdf *pdf = 0;
	std::map<int,RooAddPdf*> map_pdf = initFitRV.getSumOfGaussians();
	std::map<int,RooAddPdf*>::iterator it_pdf;
	it_pdf = map_pdf.find(125);
	if (it_pdf != map_pdf.end()) pdf = it_pdf->second;

	// MDDB mass->setBins(nBins);
	cout << " RV PDF arguments" << endl;
	RooArgSet *args = pdf->getComponents();
	args->Print("s");
	rv_args.push_back(std::make_pair(order,args)); 

	ccResRV->cd();
	RooPlot* frame2 = mass->frame() ;
      	dataRVbinned->plotOn(frame2);
	pdf->plotOn(frame2);
	RooHist *hres = frame2->residHist() ;
	hres->SetMarkerStyle(24+order);
	hres->SetMarkerColor(colors[order-1]);
	hres->SetLineColor(colors[order-1]);
	if (order == 1) hres->Draw("");
	hres->Draw("same");
	gPad->Update();
	
	ccPullRV->cd();
	RooHist *hpull = frame2->pullHist() ;
 	hpull->SetMarkerStyle(24+order);
	hpull->SetMarkerColor(colors[order-1]);
	hpull->SetLineColor(colors[order-1]);
	if (order == 1) hpull->Draw("");
	hpull->Draw("same");
	gPad->Update();

	ccpPullRV->cd();
	TH1F *hpp = new TH1F("hpp","hpp",20,-10,10);
	for (int ib = 1 ; ib <hpull->GetN()+1; ++ib){
	  double x = -999;
	  double y = -999;
	  int tmp = hpull->GetPoint(ib,x,y);
	  if (tmp != -1) hpp->Fill(y);
	}
 	hpp->SetMarkerStyle(24+order);
	hpp->SetMarkerColor(colors[order-1]);
	hpp->SetLineColor(colors[order-1]);	
	TF1 *f = new TF1("f","gaus(0)",-10,10);
	f->SetParameters(1,0,1);
	f->SetLineColor(colors[order-1]);	
	TFitResultPtr r = hpp->Fit("f","SL","same");
	float_t fitchi2   = r->Chi2(); 
	if (order == 1) {
	  hpp->SetMaximum(15);
	  hpp->Draw("");
	}
	hpp->Draw("same");
	gPad->Update();

	float myDistance = sqrt(f->GetParameter(1)*f->GetParameter(1) + (f->GetParameter(2)-1)*(f->GetParameter(2)-1));

	cout << "proc RV: " << proc << " cat " << flashggCats_[cat] << " order " << order << " --> Mean = " << f->GetParameter(1) << " Sigma = " << f->GetParameter(2) << " myDist = " << myDistance << " chi2 = " << fitchi2 <<  endl;
	if (dataRV->numEntries() > minNevts){
	  cout << "[TEX]  " << order << " &" << f->GetParameter(1) << " & " << f->GetParameter(2) << " & " << myDistance << " & " << fitchi2 <<  " & " << dataRVbinned->numEntries() <<  " & " << myDistance/dataRVbinned->numEntries() << "\\\\" << endl;
	}



        //get NLL
        double myNll=0.;
	thisNll = fitRes->minNll();
        // maybe better way to do it ?
	//double myNll = getMyNLL(mass,pdf,dataRV);
	//thisNll = getMyNLL(mass,pdf,dataRV);
	//RooAbsReal *nll = pdf->createNLL(*dataRV);
	//RooMinuit m(*nll);
	//m.migrad();
	//thisNll = nll->getVal();
	//plot(Form("plots/fTest/%s_cat%d_g%d_rv",proc.c_str(),cat,order),
        //  mass_,mass,dataRV,pdf);
	chi2 = 2.*(prevNll-thisNll);
	
        // alternative, simpler chi2... but assumed high stats?
        float chi2_bis= (plotsRV[proc][cat])->chiSquare();
        
        // plot this order
	pdf->plotOn(plotsRV[proc][cat],LineColor(colors[order-1]));
	
	if (chi2<0. && order>1) chi2=0.;
	int diffInDof = (2*order+(order-1))-(2*prev_order+(prev_order-1));
	//int diffInDof = (order- prev_order);
	//prob = TMath::Prob(chi2,diffInDof);
	float prob_old = TMath::Prob(chi2,diffInDof);
	prob = TMath::Prob(chi2_bis,2*order+(order-1));

	
	//Wilk's theorem
	cout << "[INFO] \t RV: proc " << proc << " cat " 
	     << flashggCats_[cat] << " order " << order << " diffinDof " 
	     << diffInDof << " prevNll " << prevNll << " this Nll " << thisNll 
	     << " myNll " << myNll << " chi2 " << chi2 << " chi2_bis " 
	     << chi2_bis<<  " prob_old " << prob_old << ", prob_new " 
	     <<  prob << endl;

	// rv_results.push_back(std::make_pair(order,prob));
	rv_results.push_back(std::make_pair(order,myDistance)); // MDDB
	rv_chi2.push_back(std::make_pair(order,fitchi2));       // MDDB

	prevNll=thisNll;
	//	cache_order=order;
	prev_order=order;
	order++;
      }

      delete dataRVbinned;
      ccResRV->Print(Form("%s/fTest/res_%s_cat_%s_rv.png", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      ccResRV->Print(Form("%s/fTest/res_%s_cat_%s_rv.pdf", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      delete ccResRV;
      ccPullRV->Print(Form("%s/fTest/pull_%s_cat_%s_rv.png", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      ccPullRV->Print(Form("%s/fTest/pull_%s_cat_%s_rv.pdf", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      delete ccPullRV;
      ccpPullRV->Print(Form("%s/fTest/ppull_%s_cat_%s_rv.png", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      ccpPullRV->Print(Form("%s/fTest/ppull_%s_cat_%s_rv.pdf", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      delete ccpPullRV;
      
      if (dataRV->numEntries() > minNevts){
	cout << "[TEX] \\end{tabular}" << endl;
	cout << "[TEX] "<< endl;
	cout << "[TEX] \\begin{figure}[htbp]" << endl;
	cout << "[TEX] \\begin{center}" << endl;
	cout << Form("[TEX] \\includegraphics[width=12.0cm]{{%s/sigfTest/rv_%s_%s}.pdf}"  , outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()) << endl;
	cout << "[TEX] \\end{center}" << endl;
	cout << "[TEX] \\end{figure}" << endl;
	cout << "[TEX] \\begin{figure}[htbp]" << endl;
	cout << "[TEX] \\begin{center}" << endl;
	cout << Form("[TEX] \\includegraphics[width=5.0cm]{{%s/sigfTest/res_%s_cat_%s_rv}.pdf}\\\\"  , outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()) << endl;
	cout << Form("[TEX] \\includegraphics[width=5.0cm]{{%s/sigfTest/pull_%s_cat_%s_rv}.pdf}" , outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()) << endl;
	cout << Form("[TEX] \\includegraphics[width=5.0cm]{{%s/sigfTest/ppull_%s_cat_%s_rv}.pdf}", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()) << endl;
	cout << "[TEX] \\end{center}" << endl;
	cout << "[TEX] \\end{figure}" << endl;
      }

      // original snippet of code for the choice
      // if (prob <rv_prob_limit){
      // 	float maxprob=-1;
      // 	for(unsigned int i =0; i<rv_results.size(); i++){
      // 	  if (rv_results[i].second > maxprob){
      // 	    maxprob=rv_results[i].second;
      // 	    rvChoice=rv_results[i].first;
      // 	  }
      // 	}
      // }else {
      // 	rvChoice=cache_order;
      // }

      // Choose the #gaussians RV
      float minDist = 9999999;
      float minchi2 = 9999999;
      unsigned int indChoice = 0;
      rvChoice = 1;
      if (dataRV->numEntries() > minNevts){
      	for(unsigned int i =0; i<rv_results.size(); i++){
	  // if distance is small enough just use this #gauss, where enough is set by itsOK
	  if (rv_results[i].second < itsOK){
	    rvChoice = rv_results[i].first;
	    indChoice = i;
	    break;
	  }
	  // look for the "significantly" smaller distance, where significantly is set by myThresholdDist 
      	  if (rv_results[i].second < minDist*myThresholdDist){
      	    minDist  = rv_results[i].second;
	    minchi2  = rv_chi2[i].second;
      	    rvChoice = rv_results[i].first;
	    indChoice = i;
      	  } 
	  else // look for the "significantly" smaller chi2, where significantly is set by myThresholdChi2
	    if (rv_chi2[i].second < minchi2*myThresholdChi2){
	    minDist  = rv_results[i].second;
	    minchi2  = rv_chi2[i].second;
      	    rvChoice = rv_results[i].first;
	    indChoice = i;
	  }
      	}
      } else {
      	rvChoice = -1;
      }

      if (dataRV->numEntries() > minNevts){
	if (rvChoice > 2){
	  cout <<  "[TEX] {\\Large\\color{red}\\textbf{Suggested number of gaussians: " << rvChoice << "}}" << endl;
	}
	else{
	  cout <<  "[TEX] Suggested number of gaussians: " << rvChoice << endl;
	}
	if ((rvChoice > -1) && (rv_results[indChoice].second > 0.5 || rv_chi2[indChoice].second > 25.)){
	  cout << "[TEX] \\newline {\\Huge\\color{red}\\textbf{DANGER!!!}}" << endl;
	}
      }
      cout <<  "MDDB RV parameters of the " << rvChoice << " gaussians " << Form("proc_cat=%s_%s", proc.c_str(), flashggCats_[cat].c_str()) << endl;
      if (rvChoice != -1) rv_args[rvChoice-1].second->Print("s");
      if (dataWV->numEntries() > minNevts){
	cout << "[TEX] \\newpage" << endl;
      }

    
      // wrong vertex
      order=1;
      prev_order=0;
      //      cache_order=0;
      thisNll=0.;
      //prevNll=1.e6;
      prevNll=0;
      chi2=0.;
      prob=0.;
      std::vector<pair<int,float> > wv_results;
      std::vector<pair<int,float> > wv_chi2;
      std::vector<pair<int,RooArgSet*> > wv_args;
      float wv_prob_limit = 999;

      dataWV->plotOn(plotsWV[proc][cat]);

      if (dataWV->numEntries() > minNevts){
	catForTex = boost::replace_all_copy(flashggCats_[cat], "_", "\\_");
	auto procForTex = boost::replace_all_copy(proc, "_", "\\_");
	cout << catForTex << endl;
	cout <<  Form("[TEX] \\section{WV proc %s cat %s}", procForTex.c_str(), catForTex.c_str()) << endl;
	cout << "[TEX] \\begin{tabular}{ l l l l l l l }" << endl;
	cout << "[TEX] \\hline" << endl;
	cout << "[TEX]   Order & Mean & Sigma & Dist & chi2 & entries & dist/entries\\\\" << endl;
      }
      
      TCanvas *ccResWV  = new TCanvas("ccResWV","Residuals",600,600);
      TCanvas *ccPullWV = new TCanvas("ccPullWV","Pulls",600,600);
      TCanvas *ccpPullWV = new TCanvas("ccpPullWV","PullsProjections",600,600);

      //see comments in the RV section above.
      RooDataHist * dataWVbinned = new RooDataHist("dataWVbinned","dataWVbinned",*mass,*dataWV); 
      while (prob<wv_prob_limit && order < 5){ // MDDB make it configurable
	        
	// Use the  InitialFit class that will be used later for the final signal fits
	int mhLow(125), mhHigh(125);
	if (verbose_) cout << "[INFO] Mass " << endl;
	mass->Print();
	MH->Print() ;

	InitialFit initFitWV(mass,MH,mhLow,mhHigh,skipMasses,binnedFit,nBinsFit);
	initFitWV.setVerbosity(5);
	bool recursive = false;
	initFitWV.buildSumOfGaussians(Form("cat%d_g%d",cat,order),order,recursive);

	map<int,RooDataSet*> dataSets;
	int mh = 125; // just because you can have multiple mass points in InitialFit
	dataSets.insert(pair<int,RooDataSet*>(mh,dataWV));
	initFitWV.setDatasets(dataSets);    // one of these two is useless here
	initFitWV.setDatasetsSTD(dataSets); // one of these two is useless here

	if (verbose_) cout << "[INFO] fit setup: mh = " << mh << " ; mhLow = " << mhLow << " ; mhHigh = " << mhHigh << " ; binned = " << binnedFit << " ; nBinsFit = " << nBinsFit << endl;	
	int ncpu = 1;
	initFitWV.runFits(ncpu);
	//MDDB fixed in InitialFit mass->setBins(nBins); // InitialFit reset the binning to 160

	//MDDB save params to check that the gaussian parameters are the same as the ones found in SignalFit
	system(Form("mkdir -p %s/fTest_params",outdir_.c_str())); //MDDB 
        initFitWV.saveParamsToFileAtMH(Form("%s/fTest_params/fTest_params_WV_proc_%s_cat_%s_g%d.txt",outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str(), order),125);

	RooFitResult *fitRes = 0;
	std::map<int,RooFitResult*> map_fitRes = initFitWV.getFitResults();
	std::map<int,RooFitResult*>::iterator it_fitres;
	it_fitres = map_fitRes.find(125);
        if (it_fitres != map_fitRes.end() ) fitRes = it_fitres->second;
	
	RooAddPdf *pdf = 0;
	std::map<int,RooAddPdf*> map_pdf = initFitWV.getSumOfGaussians();
	std::map<int,RooAddPdf*>::iterator it_pdf;
	it_pdf = map_pdf.find(125);
	if (it_pdf != map_pdf.end()) pdf = it_pdf->second;


	cout << " WV PDF arguments" << endl;
	RooArgSet *args = pdf->getComponents();
	args->Print("s");
	wv_args.push_back(std::make_pair(order,args)); 

	//MDDB not necessary	mass->setBins(nBins);
	ccResWV->cd();
	RooPlot* frame2 = mass->frame() ;
	dataWVbinned->plotOn(frame2);
	pdf->plotOn(frame2);
	RooHist *hres = frame2->residHist() ;
	hres->SetMarkerStyle(24+order);
	hres->SetMarkerColor(colors[order-1]);
	hres->SetLineColor(colors[order-1]);
	if (order == 1) hres->Draw("");
	hres->Draw("same");
	gPad->Update();
	
	ccPullWV->cd();
	RooHist *hpull = frame2->pullHist() ;
 	hpull->SetMarkerStyle(24+order);
	hpull->SetMarkerColor(colors[order-1]);
	hpull->SetLineColor(colors[order-1]);
	if (order == 1) hpull->Draw("");
	hpull->Draw("same");
	gPad->Update();

	ccpPullWV->cd();
	TH1F *hpp = new TH1F("hpp","hpp",20,-10,10);
	for (int ib = 1 ; ib <hpull->GetN()+1; ++ib){
	  double x = -999;
	  double y = -999;
	  int tmp = hpull->GetPoint(ib,x,y);
	  if (tmp != -1) hpp->Fill(y);
	}
 	hpp->SetMarkerStyle(24+order);
	hpp->SetMarkerColor(colors[order-1]);
	hpp->SetLineColor(colors[order-1]);	
	TF1 *f = new TF1("f","gaus(0)",-10,10);
	f->SetParameters(1,0,1);
	f->SetLineColor(colors[order-1]);	
	TFitResultPtr r = hpp->Fit("f","SL","same");
	Double_t fitchi2   = r->Chi2(); 
	if (order == 1) {
	  hpp->SetMaximum(15);
	  hpp->Draw("");
	}
	hpp->Draw("same");
	gPad->Update();

	float myDistance = sqrt(f->GetParameter(1)*f->GetParameter(1) + (f->GetParameter(2)-1)*(f->GetParameter(2)-1));

	cout << "proc WV: " << proc << " cat " << flashggCats_[cat] << " order " << order << " --> Mean = " << f->GetParameter(1) << " Sigma = " << f->GetParameter(2) << " myDist = " << myDistance << " chi2 = " << fitchi2 <<  endl;
	if (dataWV->numEntries() > minNevts){
	  cout << "[TEX]  " << order << " &" << f->GetParameter(1) << " & " << f->GetParameter(2) << " & " << myDistance << " & " << fitchi2 <<  " & " << dataWVbinned->numEntries() <<  " & " << myDistance/dataWVbinned->numEntries()  << "\\\\" << endl;
	}


      
        //get NLL
	double myNll=0.;
	thisNll = fitRes->minNll();
	//double myNll = getMyNLL(mass,pdf,dataWV);
	//thisNll = getMyNLL(mass,pdf,dataWV);
	//RooAbsReal *nll = pdf->createNLL(*dataWV);
	//RooMinuit m(*nll);
	//m.migrad();
	//thisNll = nll->getVal();
	//plot(Form("plots/fTest/%s_cat%d_g%d_wv",
        //  proc.c_str(),cat,order),mass_,mass,dataWV,pdf);

	pdf->plotOn(plotsWV[proc][cat],LineColor(colors[order-1]));
	chi2 = 2.*(prevNll-thisNll);
	float chi2_bis= (plotsWV[proc][cat])->chiSquare();
	if (chi2<0. && order>1) chi2=0.;
	int diffInDof = (2*order+(order-1))-(2*prev_order+(prev_order-1));
	//int diffInDof = (order-prev_order);
	//prob = TMath::Prob(chi2,diffInDof);
	float prob_old = TMath::Prob(chi2,diffInDof);
	prob  = TMath::Prob(chi2_bis,2*order+(order-1));

	//	dataWV->Print();
	//	pdf->Print();
	


	
	//Wilk's theorem
	cout << "[INFO] \t WV: proc " << proc <<" cat " 
	     << flashggCats_[cat] << " order " << order << " diffinDof " 
	     << diffInDof << " prevNll " << prevNll << " thosNll " 
	     << thisNll << " myNll" << myNll << " chi2" << chi2 << " chi2 bis " 
	     << chi2_bis << " prob_old " << prob_old << " prob_new " 
	     << prob <<  endl;

	// wv_results.push_back(std::make_pair(order,prob));
	wv_results.push_back(std::make_pair(order,myDistance)); // MDDB
	wv_chi2.push_back(std::make_pair(order,fitchi2));       // MDDB

	prevNll=thisNll;
	//	cache_order=order;
	prev_order=order;
	order++;
      }

      delete dataWVbinned;
      ccResWV->Print(Form("%s/fTest/res_%s_cat_%s_wv.png", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      ccResWV->Print(Form("%s/fTest/res_%s_cat_%s_wv.pdf", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      delete ccResWV;
      ccPullWV->Print(Form("%s/fTest/pull_%s_cat_%s_wv.png", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      ccPullWV->Print(Form("%s/fTest/pull_%s_cat_%s_wv.pdf", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      delete ccPullWV;
      ccpPullWV->Print(Form("%s/fTest/ppull_%s_cat_%s_wv.png", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      ccpPullWV->Print(Form("%s/fTest/ppull_%s_cat_%s_wv.pdf", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()));	
      delete ccpPullWV;

      if (dataWV->numEntries() > minNevts){
	cout << "[TEX] \\end{tabular}" << endl;
	cout << "[TEX] "<< endl;
	cout << "[TEX] \\begin{figure}[htbp]" << endl;
	cout << "[TEX] \\begin{center}" << endl;
	cout << Form("[TEX] \\includegraphics[width=12.0cm]{{%s/sigfTest/wv_%s_%s}.pdf}"  , outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()) << endl;
	cout << "[TEX] \\end{center}" << endl;
	cout << "[TEX] \\end{figure}" << endl;
	cout << "[TEX] \\begin{figure}[htbp]" << endl;
	cout << "[TEX] \\begin{center}" << endl;
	cout << Form("[TEX] \\includegraphics[width=5.0cm]{{%s/sigfTest/res_%s_cat_%s_wv}.pdf}\\\\"  , outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()) << endl;
	cout << Form("[TEX] \\includegraphics[width=5.0cm]{{%s/sigfTest/pull_%s_cat_%s_wv}.pdf}" , outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()) << endl;
	cout << Form("[TEX] \\includegraphics[width=5.0cm]{{%s/sigfTest/ppull_%s_cat_%s_wv}.pdf}", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()) << endl;
	cout << "[TEX] \\end{center}" << endl;
	cout << "[TEX] \\end{figure}" << endl;
      }

      // original snippet of code for the choice
      // if (prob <wv_prob_limit){
      // 	float maxprob=-1;
      // 	for(unsigned int i =0; i<wv_results.size(); i++){
      // 	  if (wv_results[i].second > maxprob){
      // 	    maxprob=wv_results[i].second;
      // 	    wvChoice=wv_results[i].first;
      // 	  }
      // 	}
      // }else {
      // 	wvChoice=cache_order;
      // }
      
      // Choose the #gaussians WV
      minDist = 9999999;
      minchi2 = 9999999;
      wvChoice = 1;
      if (dataWV->numEntries() > minNevts){
	for(unsigned int i =0; i<wv_results.size(); i++){
	  // if distance is small enough just use this #gauss, where enough is set by itsOK
      	  if (wv_results[i].second < itsOK){
	    wvChoice = wv_results[i].first;	  
	    break;
	  }
	  // look for the "significantly" smaller distance, where significantly is set by myThresholdDist 
	  if (wv_results[i].second < minDist*myThresholdDist){
	    minDist  = wv_results[i].second;
	    minchi2  = wv_chi2[i].second;
	    wvChoice = wv_results[i].first;	  
	  } 
	  else // look for the "significantly" smaller chi2, where significantly is set by myThresholdChi2
	    if (wv_chi2[i].second < minchi2*myThresholdChi2){
	      minDist  = wv_results[i].second;
	      minchi2  = wv_chi2[i].second;
	      wvChoice = wv_results[i].first;	  	    
	    }
	}
      } else {
	wvChoice = -1;
      }

      if (dataWV->numEntries() > minNevts){
	if (wvChoice > 2){
	  cout <<  "[TEX] {\\Large\\color{red}\\textbf{Suggested number of gaussians: " << wvChoice << "}}" << endl;
	}
	else{
	  cout <<  "[TEX] Suggested number of gaussians: " << wvChoice << endl;
	}
	if ((wvChoice > -1) && (wv_results[indChoice].second > 0.5 || wv_chi2[indChoice].second > 25.)){
	  cout << "[TEX] \\newline {\\Huge\\color{red}\\textbf{DANGER!!!}}" << endl;
	}
      }
      cout <<  "MDDB WV parameters of the " << wvChoice << " gaussians " << Form("proc_cat=%s_%s", proc.c_str(), flashggCats_[cat].c_str()) << endl;
      if (wvChoice != -1) wv_args[wvChoice-1].second->Print("s");

      // insert final choices!
      choices.insert(pair<string,pair<int,int> >(
						 //Form("%s %d",proc.c_str(),cat),make_pair(rvChoice,wvChoice)));
						 //
						 //let's see if this works..
						 Form("%s %s",proc.c_str(),flashggCats_[cat].c_str()),
						 make_pair(rvChoice,wvChoice)));
    } 
  }
  
  // now make the plots! Start with legend
  TLegend *leg = new TLegend(0.7,0.7,0.89,0.89);
  leg->SetFillColor(0);
  leg->SetLineColor(0);
  TH1F *h1 = new TH1F("h1","",1,0,1);
  h1->SetLineColor(colors[0]);
  leg->AddEntry(h1,"1st order","L");
  TH1F *h2 = new TH1F("h2","",1,0,1);
  h2->SetLineColor(colors[1]);
  leg->AddEntry(h2,"2nd order","L");
  TH1F *h3 = new TH1F("h3","",1,0,1);
  h3->SetLineColor(colors[2]);
  leg->AddEntry(h3,"3rd order","L");
  TH1F *h4 = new TH1F("h4","",1,0,1);
  h4->SetLineColor(colors[3]);
  leg->AddEntry(h4,"4th order","L");
  leg->SetShadowColor(kWhite);

  // make the canvas
  TCanvas *canv = new TCanvas("c","c",50,50,800,600);
	
  // start with the RV plots
  for (map<string,vector<RooPlot*> >::iterator plotIt=plotsRV.begin();
       plotIt!=plotsRV.end();
       plotIt++){
    string proc = plotIt->first;
    for (int cat=0; cat<ncats_; cat++){
      if ( (considerOnly_[0]).compare("All") != 0 ){
	for (unsigned int j =0; j <considerOnly_.size() ; j++){
	  if ( (considerOnly_[j]).compare(flashggCats_[cat]) != 0) { 
	    //std::cout << " [INFO] skipping " 
	    //  <<  flashggCats_[cat] << std::endl;
	    continueFlag=1;
	  }
	}
      }

      //only make plots we asked for...
      if (continueFlag){ continueFlag=0; continue;}
      
      //actual plotting
      RooPlot *plot = plotIt->second.at(cat);
      plot->Draw();

      //make it a nice CMS style plot!
      string sim="Simulation Preliminary";
      CMS_lumi( canv, 0, 0, sim ); // CMS prefer option 11 i
                                   //but out of frame (0) looks nicer!!!


      //legend
      leg->Draw();
      
      //catname
      TLatex *latex = new TLatex();	
      latex->SetTextSize(0.038);
      latex->SetNDC();
      latex->DrawLatex(0.16,0.78,Form("#splitline{%s}{%s}",proc.c_str(),flashggCats_[cat].c_str()));
      latex->DrawLatex(0.16,0.88,Form("Right Vertex (#Delta Z < 1.)"));

      // save it !
      if (verbose_) cout << "Printing " << Form("%s/fTest/rv_%s_%s.pdf", outdir_.c_str(),proc.c_str(), flashggCats_[cat].c_str()) << endl;

      canv->Print(Form("%s/fTest/rv_%s_%s.pdf",
		       outdir_.c_str(),proc.c_str(),
		       flashggCats_[cat].c_str()));
      canv->Print(Form("%s/fTest/rv_%s_%s.png",
		       outdir_.c_str(),proc.c_str(),
		       flashggCats_[cat].c_str()));
    }
  }

  // now plot the WV
  for (map<string,vector<RooPlot*> >::iterator plotIt=plotsWV.begin();
       plotIt!=plotsWV.end();
       plotIt++){
    string proc = plotIt->first;
    for (int cat=0; cat<ncats_; cat++){
      if ( (considerOnly_[0]).compare("All") != 0 ){
	for (unsigned int j =0; j <considerOnly_.size() ; j++){
	  if ( (considerOnly_[j]).compare(flashggCats_[cat]) != 0) { 
	    //std::cout << " [INFO] skipping " 
	    //  <<  flashggCats_[cat] << std::endl;
	    continueFlag=1;
	  }
	}
      }
      if (continueFlag){ continueFlag=0; continue;}
      RooPlot *plot = plotIt->second.at(cat);
      plot->Draw();

      //cms style
      string sim="Simulation";
      CMS_lumi( canv, 0,0 , sim); // out of frame (0,0) 
      //looks nicer than corner (0,16)

      //legend
      leg->Draw();
      
      //catname
      TLatex *latex = new TLatex();	
      latex->SetTextSize(0.038);
      latex->SetNDC();
      latex->DrawLatex(0.16,0.78,Form("#splitline{%s}{%s}",proc.c_str(),flashggCats_[cat].c_str()));
      latex->DrawLatex(0.16,0.88,Form("Wrong Vertex (#Delta Z #geq 1.)"));

      //save plots
      canv->Print(Form("%s/fTest/wv_%s_%s.pdf",
		       outdir_.c_str(),proc.c_str(),flashggCats_[cat].c_str()));
      canv->Print(Form("%s/fTest/wv_%s_%s.png",
		       outdir_.c_str(),proc.c_str(),flashggCats_[cat].c_str()));
    }
  }
  
  // and the combined plots.
  for (map<string,vector<RooPlot*> >::iterator plotIt=plots.begin();
       plotIt!=plots.end();
       plotIt++){
    string proc = plotIt->first;
    for (int cat=0; cat<ncats_; cat++){
      if ( (considerOnly_[0]).compare("All") != 0 ){
	for (unsigned int j =0; j <considerOnly_.size() ; j++){
	  if ( (considerOnly_[j]).compare(flashggCats_[cat]) != 0) { 
	    //std::cout << " [INFO] skipping " 
	    //  <<  flashggCats_[cat] << std::endl;
	    continueFlag=1;
	  }
	}
      }

      //only make plots we asked for...
      if (continueFlag){ continueFlag=0; continue;}
      
      //actual plotting
      RooPlot *plot = plotIt->second.at(cat);
      plot->Draw();

      //make it a nice CMS style plot!
      string sim="Simulation";
      CMS_lumi( canv, 0, 0, sim ); // CMS prefer option 16 i
                                   //but out of frame (0) looks nicer!!!


      //legend
      leg->Draw();
      
      //catname
      TLatex *latex = new TLatex();	
      latex->SetTextSize(0.038);
      latex->SetNDC();
      latex->DrawLatex(0.16,0.78,Form("#splitline{%s}{%s}",proc.c_str(),flashggCats_[cat].c_str()));
      latex->DrawLatex(0.16,0.88,Form("RV+WV"));

      // save it !
      //canv->Print(Form("%s/fTest/%s_%s.pdf",
      //  outdir_.c_str(),proc.c_str(),
      //  flashggCats_[cat].c_str()));
      //canv->Print(Form("%s/fTest/%s_%s.png",
      //  outdir_.c_str(),proc.c_str(),
      //  flashggCats_[cat].c_str()));
      //  ^ NB at the moment the combiend plot is not terribly useful, so ignore it for now.
    }
  }

  //no memory leaks please!
  delete canv;
  
  //finally, rpint otu recommended options. This is kinda useless right now..
  cout << "[INFO] Recommended options" << endl;
  ofstream output_datfile;

  //write them to a file, I guess..
  output_datfile.open ((datfilename_).c_str());
  if (verbose_) std::cout << "[INFO] Writing to datfilename_ " 
			  << datfilename_ << std::endl;
  output_datfile << "#proc cat nGausRV nGausWV" << std::endl;
  int p =0;

  for (map<string,pair<int,int> >::iterator it=choices.begin();
       it!=choices.end();
       it++){
      
    //print to screen
    cout << "[INFO] \t "  <<" "<< it->first 
	 << " " << it->second.first << " " 
	 << it->second.second << endl;

    //print to file
    output_datfile<< it->first 
		  << " " << it->second.first 
		  << " " << it->second.second << endl;
    p++; // what is p doing ?
  }

  cout << "[TEX]\\end{document}" << endl;
  
  output_datfile.close();
  inWS->Close();
  return 0;
}
