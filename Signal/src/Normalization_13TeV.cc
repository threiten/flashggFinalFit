#include "../interface/Normalization_13TeV.h"

#include "TSystem.h"

Normalization_13TeV::Normalization_13TeV(){}

int Normalization_13TeV::Init(int sqrtS){

    TPython::Exec("import os,imp");
    const char * env = gSystem->Getenv("CMSSW_BASE");
    std::string globeRt = env;
    if (globeRt !=0){
        globeRt = globeRt+"/src/flashggFinalFit/Signal";
    }
    if( ! TPython::Exec(Form("buildSMHiggsSignalXSBR = imp.load_source('*', '%s/python/buildSMHiggsSignalXSBR.py')",globeRt.c_str())) ) {
        std::cout<<  "[ERROR] Importing buildSMHiggsSignalXSBR from python failed. exit." << std::endl;
        return 0;
    }
    TPython::Eval(Form("buildSMHiggsSignalXSBR.Init%dTeV()", sqrtS));
    
    for (double mH=120;mH<130.05;mH+=0.1){ // breaks when extended beyond 130
        double valBR           = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getBR(%f)",mH));
        double valXSggH        = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getXS(%f,'%s')",mH,"ggH"));
        double valXSqqH        = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getXS(%f,'%s')",mH,"qqH"));
        double valXSttH        = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getXS(%f,'%s')",mH,"ttH"));
        double valXSWH         = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getXS(%f,'%s')",mH,"WH"));
        double valXSZH         = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getXS(%f,'%s')",mH,"ZH"));
        double valXSbbH        = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getXS(%f,'%s')",mH,"bbH"));
        double valXStHq        = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getXS(%f,'%s')",mH,"tHq"));
        double valXStHW        = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getXS(%f,'%s')",mH,"tHW"));
        double valXSggZH       = (double)TPython::Eval(Form("buildSMHiggsSignalXSBR.getXS(%f,'%s')",mH,"ggZH"));
        double valXSQQ2HLNU    = valXSWH*(3.*10.86*0.01)/*3xBR(W to lv)*/;  
        double valXSQQ2HLL     = valXSZH*(3*3.3658*0.01 + 20.00*0.01)/*BR(Z to ll) + BR(Z to invisible)*/;  
        double valXSVH2HQQ     = valXSWH*(67.41*0.01)/*BR(W to hadrons)*/ + valXSZH*(69.91*0.01)/*BR(Z to hadrons)*/; 
        double valXSWH2HQQ     = valXSWH*(67.41*0.01)/*BR(W to hadrons)*/;
        double valXSZH2HQQ     = valXSZH*(69.91*0.01)/*BR(Z to hadrons)*/;  

        BranchingRatioMap[mH] = valBR;

        XSectionMap_ggh[mH] = valXSggH;   
        XSectionMap_vbf[mH] = valXSqqH;   
        XSectionMap_tth[mH] = valXSttH;   
        XSectionMap_wh[mH]  = valXSWH;  
        XSectionMap_zh[mH]  = valXSZH;  

        XSectionMap_QQ2HLNU[mH] = valXSQQ2HLNU;
        XSectionMap_QQ2HLL[mH]  = valXSQQ2HLL;
        XSectionMap_VH2HQQ[mH]  = valXSVH2HQQ;
        XSectionMap_WH2HQQ[mH]  = valXSWH2HQQ;
        XSectionMap_ZH2HQQ[mH]  = valXSZH2HQQ;

        /*
        //Stage 1 
        XSectionMap_GG2H_0J[mH]                  = 0.91 * 0.6236 * valXSggH; // frac(central) * frac(of central) * totXS
        XSectionMap_GG2H_1J_PTH_0_60[mH]         = 0.91 * 0.1508 * valXSggH;
        XSectionMap_GG2H_1J_PTH_60_120[mH]       = 0.91 * 0.1008 * valXSggH;
        XSectionMap_GG2H_1J_PTH_120_200[mH]      = 0.91 * 0.0173 * valXSggH;
        XSectionMap_GG2H_1J_PTH_GT200[mH]        = 0.91 * 0.0027 * valXSggH;
        XSectionMap_GG2H_GE2J_PTH_0_60[mH]       = 0.91 * 0.0233 * valXSggH;
        XSectionMap_GG2H_GE2J_PTH_60_120[mH]     = 0.91 * 0.0406 * valXSggH;
        XSectionMap_GG2H_GE2J_PTH_120_200[mH]    = 0.91 * 0.0170 * valXSggH;
        XSectionMap_GG2H_GE2J_PTH_GT200[mH]      = 0.91 * 0.0097 * valXSggH;
        XSectionMap_GG2H_VBFTOPO_JET3VETO[mH]    = 0.91 * 0.0063 * valXSggH;
        XSectionMap_GG2H_VBFTOPO_JET3[mH]        = 0.91 * 0.0079 * valXSggH;

        XSectionMap_VBF_PTJET1_GT200[mH]         = 0.93 * 0.0508 * valXSqqH;
        XSectionMap_VBF_VH2JET[mH]               = 0.93 * 0.0172 * valXSqqH;
        XSectionMap_VBF_VBFTOPO_JET3VETO[mH]     = 0.93 * 0.2624 * valXSqqH;
        XSectionMap_VBF_VBFTOPO_JET3[mH]         = 0.93 * 0.1017 * valXSqqH;
        XSectionMap_VBF_REST[mH]                 = 0.93 * 0.5680 * valXSqqH;

        XSectionMap_QQ2HLNU_PTV_0_150[mH]        = 0.9467*valXSQQ2HLNU;
        XSectionMap_QQ2HLNU_PTV_150_250_0J[mH]   = 0.0203*valXSQQ2HLNU;
        XSectionMap_QQ2HLNU_PTV_150_250_GE1J[mH] = 0.0260*valXSQQ2HLNU;
        XSectionMap_QQ2HLNU_PTV_GT250[mH]        = 0.0070*valXSQQ2HLNU;

        XSectionMap_QQ2HLL_PTV_0_150[mH]         = 0.9413*valXSQQ2HLL;
        XSectionMap_QQ2HLL_PTV_150_250_0J[mH]    = 0.0242*valXSQQ2HLL;
        XSectionMap_QQ2HLL_PTV_150_250_GE1J[mH]  = 0.0277*valXSQQ2HLL;
        XSectionMap_QQ2HLL_PTV_GT250[mH]         = 0.0068*valXSQQ2HLL;

        XSectionMap_WH2HQQ_PTJET1_GT200[mH]      = 0.0417*valXSWH2HQQ;
        XSectionMap_WH2HQQ_VH2JET[mH]            = 0.2914*valXSWH2HQQ;
        XSectionMap_WH2HQQ_VBFTOPO_JET3VETO[mH]  = 0.0033*valXSWH2HQQ;
        XSectionMap_WH2HQQ_VBFTOPO_JET3[mH]      = 0.0092*valXSWH2HQQ;
        XSectionMap_WH2HQQ_REST[mH]              = 0.6543*valXSWH2HQQ;

        XSectionMap_ZH2HQQ_PTJET1_GT200[mH]      = 0.0382*valXSZH2HQQ;
        XSectionMap_ZH2HQQ_VH2JET[mH]            = 0.3337*valXSZH2HQQ;
        XSectionMap_ZH2HQQ_VBFTOPO_JET3VETO[mH]  = 0.0016*valXSZH2HQQ;
        XSectionMap_ZH2HQQ_VBFTOPO_JET3[mH]      = 0.0080*valXSZH2HQQ;
        XSectionMap_ZH2HQQ_REST[mH]              = 0.6185*valXSZH2HQQ;

        XSectionMap_TTH[mH]                      = valXSttH;
        XSectionMap_BBH[mH]                      = valXSbbH;
        XSectionMap_THQ[mH]                      = valXStHq;
        XSectionMap_THW[mH]                      = valXStHW;
        XSectionMap_GGZH[mH]                     = valXSggZH;
        */

        //Stage 1.1
        XSectionMap_GG2H_PTH_GT200[mH] = 0.91 * 0.0286 * valXSggH;
        XSectionMap_GG2H_0J_PTH_0_10[mH] = 0.91 * 0.1576 * valXSggH;
        XSectionMap_GG2H_0J_PTH_GT10[mH] = 0.91 * 0.4334 * valXSggH;
        XSectionMap_GG2H_1J_PTH_0_60[mH] = 0.91 * 0.1460 * valXSggH;
        XSectionMap_GG2H_1J_PTH_60_120[mH] = 0.91 * 0.1039 * valXSggH;
        XSectionMap_GG2H_1J_PTH_120_200[mH] = 0.91 * 0.0215 * valXSggH;
        XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_0_60[mH] = 0.91 * 0.0247 * valXSggH;
        XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_60_120[mH] = 0.91 * 0.0378 * valXSggH;
        XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_120_200[mH] = 0.91 * 0.0246 * valXSggH;
        XSectionMap_GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25[mH] = 0.91 * 0.0073 * valXSggH;
        XSectionMap_GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25[mH] = 0.91 * 0.0078 * valXSggH;
        XSectionMap_GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25[mH] = 0.91 * 0.0029 * valXSggH;
        XSectionMap_GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25[mH] = 0.91 * 0.0038 * valXSggH;       

        XSectionMap_VBF_0J[mH] = 0.93 * 0.0686 * valXSqqH;
        XSectionMap_VBF_1J[mH] = 0.93 * 0.3475 * valXSqqH;
        XSectionMap_VBF_GE2J_MJJ_0_60[mH] = 0.93 * 0.0108 * valXSqqH;
        XSectionMap_VBF_GE2J_MJJ_60_120[mH] = 0.93 * 0.0181 * valXSqqH;
        XSectionMap_VBF_GE2J_MJJ_120_350[mH] = 0.93 * 0.1065 * valXSqqH;
        XSectionMap_VBF_GE2J_MJJ_GT350_PTH_GT200[mH] = 0.93 * 0.0590 * valXSqqH;
        XSectionMap_VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25[mH] = 0.93 * 0.1276 * valXSqqH;
        XSectionMap_VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25[mH] = 0.93 * 0.0250 * valXSqqH;
        XSectionMap_VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25[mH] = 0.93 * 0.1878 * valXSqqH;
        XSectionMap_VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25[mH] = 0.93 * 0.0029 * valXSqqH;

        XSectionMap_WH2HQQ_0J[mH] = 0.001 * valXSWH2HQQ;
        XSectionMap_WH2HQQ_1J[mH] = 0.001 * valXSWH2HQQ;
        XSectionMap_WH2HQQ_GE2J_MJJ_0_60[mH] = 0.001 * valXSWH2HQQ;
        XSectionMap_WH2HQQ_GE2J_MJJ_60_120[mH] = 0.99 * valXSWH2HQQ;
        XSectionMap_WH2HQQ_GE2J_MJJ_120_350[mH] = 0.001 * valXSWH2HQQ;
        XSectionMap_WH2HQQ_GE2J_MJJ_GT350_PTH_GT200[mH] = 0.001 * valXSWH2HQQ;
        XSectionMap_WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25[mH] = 0.001 * valXSWH2HQQ;
        XSectionMap_WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25[mH] = 0.001 * valXSWH2HQQ;
        XSectionMap_WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25[mH] = 0.001 * valXSWH2HQQ;
        XSectionMap_WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25[mH] = 0.001 * valXSWH2HQQ;

        XSectionMap_ZH2HQQ_0J[mH] = 0.001 * valXSZH2HQQ;
        XSectionMap_ZH2HQQ_1J[mH] = 0.001 * valXSZH2HQQ;
        XSectionMap_ZH2HQQ_GE2J_MJJ_0_60[mH] = 0.001 * valXSZH2HQQ;
        XSectionMap_ZH2HQQ_GE2J_MJJ_60_120[mH] = 0.99 * valXSZH2HQQ;
        XSectionMap_ZH2HQQ_GE2J_MJJ_120_350[mH] = 0.001 * valXSZH2HQQ;
        XSectionMap_ZH2HQQ_GE2J_MJJ_GT350_PTH_GT200[mH] = 0.001 * valXSZH2HQQ;
        XSectionMap_ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25[mH] = 0.001 * valXSZH2HQQ;
        XSectionMap_ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25[mH] = 0.001 * valXSZH2HQQ;
        XSectionMap_ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25[mH] = 0.001 * valXSZH2HQQ;
        XSectionMap_ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25[mH] = 0.001 * valXSZH2HQQ;

        XSectionMap_QQ2HLNU_PTV_0_75[mH] = 0.5 * valXSQQ2HLNU;
        XSectionMap_QQ2HLNU_PTV_75_150[mH] = 0.4467 * valXSQQ2HLNU;
        XSectionMap_QQ2HLNU_PTV_150_250_0J[mH] = 0.0203 * valXSQQ2HLNU;
        XSectionMap_QQ2HLNU_PTV_150_250_GE1J[mH] = 0.0260 * valXSQQ2HLNU;
        XSectionMap_QQ2HLNU_PTV_GT250[mH] = 0.0070 * valXSQQ2HLNU;

        XSectionMap_QQ2HLL_PTV_0_75[mH] = 0.5 * valXSQQ2HLL;
        XSectionMap_QQ2HLL_PTV_75_150[mH] = 0.4413 * valXSQQ2HLL;
        XSectionMap_QQ2HLL_PTV_150_250_0J[mH] = 0.0242 * valXSQQ2HLL;
        XSectionMap_QQ2HLL_PTV_150_250_GE1J[mH] = 0.0260 * valXSQQ2HLL;
        XSectionMap_QQ2HLL_PTV_GT250[mH] = 0.0068 * valXSQQ2HLL;

        XSectionMap_TTH[mH]                      = valXSttH;
        XSectionMap_BBH[mH]                      = valXSbbH;
        XSectionMap_THQ[mH]                      = valXStHq;
        XSectionMap_THW[mH]                      = valXStHW;
        XSectionMap_GGZH[mH]                     = valXSggZH;
    }
}

TGraph * Normalization_13TeV::GetSigmaGraph(TString process)
{
  TGraph * gr = new TGraph();
  std::map<double, double> * XSectionMap = 0;
  // Stage 1.1
  if ( process == "GG2H_PTH_GT200" ){
          XSectionMap = &XSectionMap_GG2H_PTH_GT200;
  } else if ( process == "GG2H_0J_PTH_0_10" ){
          XSectionMap = &XSectionMap_GG2H_0J_PTH_0_10;
  } else if ( process == "GG2H_0J_PTH_GT10" ){
          XSectionMap = &XSectionMap_GG2H_0J_PTH_GT10;
  } else if ( process == "GG2H_1J_PTH_0_60" ){
          XSectionMap = &XSectionMap_GG2H_1J_PTH_0_60;
  } else if ( process == "GG2H_1J_PTH_60_120" ){
          XSectionMap = &XSectionMap_GG2H_1J_PTH_60_120;
  } else if ( process == "GG2H_1J_PTH_120_200" ){
          XSectionMap = &XSectionMap_GG2H_1J_PTH_120_200;
  } else if ( process == "GG2H_GE2J_MJJ_0_350_PTH_0_60" ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_0_60;
  } else if ( process == "GG2H_GE2J_MJJ_0_350_PTH_60_120" ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_60_120;
  } else if ( process == "GG2H_GE2J_MJJ_0_350_PTH_120_200" ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_120_200;
  } else if ( process == "GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25" ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  } else if ( process == "GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25" ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  } else if ( process == "GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25" ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  } else if ( process == "GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25" ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  } else if ( process == "VBF_0J" ){
          XSectionMap = &XSectionMap_VBF_0J;
  } else if ( process == "VBF_1J" ){
          XSectionMap = &XSectionMap_VBF_1J;
  } else if ( process == "VBF_GE2J_MJJ_0_60" ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_0_60;
  } else if ( process == "VBF_GE2J_MJJ_60_120" ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_60_120;
  } else if ( process == "VBF_GE2J_MJJ_120_350" ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_120_350;
  } else if ( process == "VBF_GE2J_MJJ_GT350_PTH_GT200" ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_GT350_PTH_GT200;
  } else if ( process == "VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25" ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  } else if ( process == "VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25" ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  } else if ( process == "VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25" ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  } else if ( process == "VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25" ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  } else if ( process == "WH2HQQ_0J" ){
          XSectionMap = &XSectionMap_WH2HQQ_0J;
  } else if ( process == "WH2HQQ_1J" ){
          XSectionMap = &XSectionMap_WH2HQQ_1J;
  } else if ( process == "WH2HQQ_GE2J_MJJ_0_60" ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_0_60;
  } else if ( process == "WH2HQQ_GE2J_MJJ_60_120" ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_60_120;
  } else if ( process == "WH2HQQ_GE2J_MJJ_120_350" ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_120_350;
  } else if ( process == "WH2HQQ_GE2J_MJJ_GT350_PTH_GT200" ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_GT350_PTH_GT200;
  } else if ( process == "WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25" ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  } else if ( process == "WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25" ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  } else if ( process == "WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25" ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  } else if ( process == "WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25" ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  } else if ( process == "ZH2HQQ_0J" ){
          XSectionMap = &XSectionMap_ZH2HQQ_0J;
  } else if ( process == "ZH2HQQ_1J" ){
          XSectionMap = &XSectionMap_ZH2HQQ_1J;
  } else if ( process == "ZH2HQQ_GE2J_MJJ_0_60" ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_0_60;
  } else if ( process == "ZH2HQQ_GE2J_MJJ_60_120" ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_60_120;
  } else if ( process == "ZH2HQQ_GE2J_MJJ_120_350" ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_120_350;
  } else if ( process == "ZH2HQQ_GE2J_MJJ_GT350_PTH_GT200" ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_GT350_PTH_GT200;
  } else if ( process == "ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25" ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  } else if ( process == "ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25" ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  } else if ( process == "ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25" ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  } else if ( process == "ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25" ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  } else if ( process == "QQ2HLNU_PTV_0_75" ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_0_75;
  } else if ( process == "QQ2HLNU_PTV_75_150" ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_75_150;
  } else if ( process == "QQ2HLNU_PTV_150_250_0J" ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_150_250_0J;
  } else if ( process == "QQ2HLNU_PTV_150_250_GE1J" ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_150_250_GE1J;
  } else if ( process == "QQ2HLNU_PTV_GT250" ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_GT250;
  } else if ( process == "QQ2HLL_PTV_0_75" ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_0_75;
  } else if ( process == "QQ2HLL_PTV_75_150" ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_75_150;
  } else if ( process == "QQ2HLL_PTV_150_250_0J" ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_150_250_0J;
  } else if ( process == "QQ2HLL_PTV_150_250_GE1J" ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_150_250_GE1J;
  } else if ( process == "QQ2HLL_PTV_GT250" ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_GT250;
  } else if ( process == "BBH" ){
          XSectionMap = &XSectionMap_BBH;
  } else if ( process == "TTH" ){
          XSectionMap = &XSectionMap_TTH;
  } else if ( process == "THW" ){
          XSectionMap = &XSectionMap_THW;
  } else if ( process == "THQ" ){
          XSectionMap = &XSectionMap_THQ;
  } else if ( process == "GGZH" ){
          XSectionMap = &XSectionMap_GGZH;  
  } else if ( process == "ggh" || process=="GG2H" ) {
    XSectionMap = &XSectionMap_ggh;
  } else if ( process == "vbf" || process=="VBF" ) {
    XSectionMap = &XSectionMap_vbf;
  } else if ( process == "tth" || process=="TTH" ) {
    XSectionMap = &XSectionMap_tth;
  } else if ( process == "wh") {
    XSectionMap = &XSectionMap_wh;
  } else if ( process == "zh") {
    XSectionMap = &XSectionMap_zh;
  } else if ( process=="QQ2HLNU" ) {
    XSectionMap = &XSectionMap_QQ2HLNU;
  } else if ( process=="QQ2HLL" ) {
    XSectionMap = &XSectionMap_QQ2HLL;
  } else if ( process=="VH2HQQ" ) {
    XSectionMap = &XSectionMap_VH2HQQ;
  } else if ( process=="WH2HQQ" ) {
    XSectionMap = &XSectionMap_WH2HQQ;
  } else if ( process=="ZH2HQQ" ) {
    XSectionMap = &XSectionMap_ZH2HQQ;
  } else {
    std::cout << "[WARNING] Normalization_13TeV: No known process found in the name!!" << std::endl;
    std::cout << "[DEBUG] Normalization_13TeV failed for process = " << process << std::endl;
    //exit(1);
  }

  for (std::map<double, double>::const_iterator iter = XSectionMap->begin();  iter != XSectionMap->end(); ++iter) {
      gr->SetPoint(gr->GetN(),iter->first, iter->second );
  }

  return gr;
}

TGraph * Normalization_13TeV::GetBrGraph()
{
  TGraph * gr = new TGraph();
  for (std::map<double, double>::const_iterator iter = BranchingRatioMap.begin();  iter != BranchingRatioMap.end(); ++iter) {
      gr->SetPoint(gr->GetN(),iter->first, iter->second );
  }
  return gr;
}

double Normalization_13TeV::GetBR(double mass) {

    for (std::map<double, double>::const_iterator iter = BranchingRatioMap.begin();  iter != BranchingRatioMap.end(); ++iter) {
        if (abs(mass-iter->first)<0.001) return iter->second;
        if (mass>iter->first) {
            double lowmass = iter->first;
            double lowbr = iter->second;
            ++iter;
            if (mass<iter->first) {
                double highmass = iter->first;
                double highbr = iter->second;
                double br = (highbr-lowbr)/(highmass-lowmass)*(mass-lowmass)+lowbr;
                return br;
            }
            --iter;
        }
    }

    std::cout << "[WARNING] Warning branching ratio outside range of 90-250GeV!!!!" << std::endl;
    //std::exit(1);
    return -1;
}

double Normalization_13TeV::GetXsection(double mass, TString HistName) {

  std::map<double,double> *XSectionMap;
  // Stage 1.1
  if ( HistName.Contains("GG2H_PTH_GT200") ){
          XSectionMap = &XSectionMap_GG2H_PTH_GT200;
  } else if ( HistName.Contains("GG2H_0J_PTH_0_10") ){
          XSectionMap = &XSectionMap_GG2H_0J_PTH_0_10;
  } else if ( HistName.Contains("GG2H_0J_PTH_GT10") ){
          XSectionMap = &XSectionMap_GG2H_0J_PTH_GT10;
  } else if ( HistName.Contains("GG2H_1J_PTH_0_60") ){
          XSectionMap = &XSectionMap_GG2H_1J_PTH_0_60;
  } else if ( HistName.Contains("GG2H_1J_PTH_60_120") ){
          XSectionMap = &XSectionMap_GG2H_1J_PTH_60_120;
  } else if ( HistName.Contains("GG2H_1J_PTH_120_200") ){
          XSectionMap = &XSectionMap_GG2H_1J_PTH_120_200;
  } else if ( HistName.Contains("GG2H_GE2J_MJJ_0_350_PTH_0_60") ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_0_60;
  } else if ( HistName.Contains("GG2H_GE2J_MJJ_0_350_PTH_60_120") ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_60_120;
  } else if ( HistName.Contains("GG2H_GE2J_MJJ_0_350_PTH_120_200") ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_120_200;
  } else if ( HistName.Contains("GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25") ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  } else if ( HistName.Contains("GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25") ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  } else if ( HistName.Contains("GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25") ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  } else if ( HistName.Contains("GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25") ){
          XSectionMap = &XSectionMap_GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  } else if ( HistName.Contains("VBF_0J") ){
          XSectionMap = &XSectionMap_VBF_0J;
  } else if ( HistName.Contains("VBF_1J") ){
          XSectionMap = &XSectionMap_VBF_1J;
  } else if ( HistName.Contains("VBF_GE2J_MJJ_0_60") ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_0_60;
  } else if ( HistName.Contains("VBF_GE2J_MJJ_60_120") ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_60_120;
  } else if ( HistName.Contains("VBF_GE2J_MJJ_120_350") ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_120_350;
  } else if ( HistName.Contains("VBF_GE2J_MJJ_GT350_PTH_GT200") ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_GT350_PTH_GT200;
  } else if ( HistName.Contains("VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25") ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  } else if ( HistName.Contains("VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25") ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  } else if ( HistName.Contains("VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25") ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  } else if ( HistName.Contains("VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25") ){
          XSectionMap = &XSectionMap_VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  } else if ( HistName.Contains("WH2HQQ_0J") ){
          XSectionMap = &XSectionMap_WH2HQQ_0J;
  } else if ( HistName.Contains("WH2HQQ_1J") ){
          XSectionMap = &XSectionMap_WH2HQQ_1J;
  } else if ( HistName.Contains("WH2HQQ_GE2J_MJJ_0_60") ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_0_60;
  } else if ( HistName.Contains("WH2HQQ_GE2J_MJJ_60_120") ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_60_120;
  } else if ( HistName.Contains("WH2HQQ_GE2J_MJJ_120_350") ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_120_350;
  } else if ( HistName.Contains("WH2HQQ_GE2J_MJJ_GT350_PTH_GT200") ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_GT350_PTH_GT200;
  } else if ( HistName.Contains("WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25") ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  } else if ( HistName.Contains("WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25") ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  } else if ( HistName.Contains("WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25") ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  } else if ( HistName.Contains("WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25") ){
          XSectionMap = &XSectionMap_WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  } else if ( HistName.Contains("ZH2HQQ_0J") ){
          XSectionMap = &XSectionMap_ZH2HQQ_0J;
  } else if ( HistName.Contains("ZH2HQQ_1J") ){
          XSectionMap = &XSectionMap_ZH2HQQ_1J;
  } else if ( HistName.Contains("ZH2HQQ_GE2J_MJJ_0_60") ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_0_60;
  } else if ( HistName.Contains("ZH2HQQ_GE2J_MJJ_60_120") ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_60_120;
  } else if ( HistName.Contains("ZH2HQQ_GE2J_MJJ_120_350") ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_120_350;
  } else if ( HistName.Contains("ZH2HQQ_GE2J_MJJ_GT350_PTH_GT200") ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_GT350_PTH_GT200;
  } else if ( HistName.Contains("ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25") ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  } else if ( HistName.Contains("ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25") ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  } else if ( HistName.Contains("ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25") ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  } else if ( HistName.Contains("ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25") ){
          XSectionMap = &XSectionMap_ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  } else if ( HistName.Contains("QQ2HLNU_PTV_0_75") ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_0_75;
  } else if ( HistName.Contains("QQ2HLNU_PTV_75_150") ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_75_150;
  } else if ( HistName.Contains("QQ2HLNU_PTV_150_250_0J") ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_150_250_0J;
  } else if ( HistName.Contains("QQ2HLNU_PTV_150_250_GE1J") ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_150_250_GE1J;
  } else if ( HistName.Contains("QQ2HLNU_PTV_GT250") ){
          XSectionMap = &XSectionMap_QQ2HLNU_PTV_GT250;
  } else if ( HistName.Contains("QQ2HLL_PTV_0_75") ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_0_75;
  } else if ( HistName.Contains("QQ2HLL_PTV_75_150") ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_75_150;
  } else if ( HistName.Contains("QQ2HLL_PTV_150_250_0J") ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_150_250_0J;
  } else if ( HistName.Contains("QQ2HLL_PTV_150_250_GE1J") ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_150_250_GE1J;
  } else if ( HistName.Contains("QQ2HLL_PTV_GT250") ){
          XSectionMap = &XSectionMap_QQ2HLL_PTV_GT250;
  } else if ( HistName.Contains("BBH") ){
          XSectionMap = &XSectionMap_BBH;
  } else if ( HistName.Contains("TTH") ){
          XSectionMap = &XSectionMap_TTH;
  } else if ( HistName.Contains("THW") ){
          XSectionMap = &XSectionMap_THW;
  } else if ( HistName.Contains("THQ") ){
          XSectionMap = &XSectionMap_THQ;
  } else if ( HistName.Contains("GGZH") ){
          XSectionMap = &XSectionMap_GGZH;
  } else if ( HistName.Contains("ggh") || HistName.Contains("GG2H") ) {
    XSectionMap = &XSectionMap_ggh;
  } else if ( HistName.Contains("vbf") || HistName.Contains("VBF") ) {
    XSectionMap = &XSectionMap_vbf;
  } else if ( HistName.Contains("tth") || HistName.Contains("TTH") ) {
    XSectionMap = &XSectionMap_tth;
  } else if ( HistName.Contains("wh") ) {
    XSectionMap = &XSectionMap_wh;
  } else if ( HistName.Contains("zh") ) {
    XSectionMap = &XSectionMap_zh;
  } else if ( HistName.Contains("QQ2HLNU") ) {
    XSectionMap = &XSectionMap_QQ2HLNU;
  } else if ( HistName.Contains("QQ2HLL") ) {
    XSectionMap = &XSectionMap_QQ2HLL;
  } else if ( HistName.Contains("VH2HQQ") ) {
    XSectionMap = &XSectionMap_VH2HQQ;
  } else if ( HistName.Contains("WH2HQQ") ) {
    XSectionMap = &XSectionMap_WH2HQQ;
  } else if ( HistName.Contains("ZH2HQQ") ) {
    XSectionMap = &XSectionMap_ZH2HQQ;
  } else if ( HistName.Contains("BBH") ) {
    XSectionMap = &XSectionMap_BBH;
  } else if ( HistName.Contains("THQ") ) {
    XSectionMap = &XSectionMap_THQ;
  } else if ( HistName.Contains("THW") ) {
    XSectionMap = &XSectionMap_THW;
  } else if ( HistName.Contains("GGZH") ) {
    XSectionMap = &XSectionMap_GGZH;
  } else {
    std::cout << "[WARNING] Normalization_13TeV: No known process found in the name!!" << HistName << std::endl;
    //exit(1);
  }

  for (std::map<double, double>::const_iterator iter = XSectionMap->begin();  iter != XSectionMap->end(); ++iter) {
      if (abs(mass-iter->first)<0.001) return iter->second;
      if (mass>iter->first) {
          double lowmass = iter->first;
          double lowxsec = iter->second;
          ++iter;
          if (mass<iter->first) {
              double highmass = iter->first;
              double highxsec = iter->second;
              double xsec = (highxsec-lowxsec)/(highmass-lowmass)*(mass-lowmass)+lowxsec;
              return xsec;
          }
          --iter;
      }
  }

  std::cout << "[WARNING] Warning cross section outside range of 80-300GeV!!!!" << std::endl;
  //exit(1);
  return -1;
}

double Normalization_13TeV::GetXsection(double mass) {
    return GetXsection(mass,"ggh") + GetXsection(mass,"vbf") + GetXsection(mass,"wh") + GetXsection(mass,"zh") + GetXsection(mass,"tth");
}

double Normalization_13TeV::GetNorm(double mass1, TH1F* hist1, double mass2, TH1F* hist2, double mass) {

  double br = GetBR(mass);
  double br1 = GetBR(mass1);
  double br2 = GetBR(mass2);

  double xsec = GetXsection(mass, hist1->GetName());
  double xsec1 = GetXsection(mass1, hist1->GetName());
  double xsec2 = GetXsection(mass2, hist2->GetName());

  double alpha = 1.0*(mass-mass1)/(mass2-mass1);
  double effAcc1 = hist1->Integral()/(xsec1*br1);
  double effAcc2 = hist2->Integral()/(xsec2*br2);

  double Normalization = (xsec*br)*(effAcc1 + alpha * (effAcc2 - effAcc1));

  return Normalization;
}
