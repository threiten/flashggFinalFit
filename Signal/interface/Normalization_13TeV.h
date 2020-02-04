#ifndef Normalization_13TeV_h
#define Normalization_13TeV_h

#include <vector>
#include <map>
#include <iostream>

#include "TH1F.h"
#include "TGraph.h"
#include "TCanvas.h"
#include "TString.h"
#include "TROOT.h"
#include "TLegend.h"
#include "TPython.h"
#include "HiggsAnalysis/CombinedLimit/interface/RooSpline1D.h"

using namespace std;

class Normalization_13TeV {

  public:
	Normalization_13TeV();

	int Init(int sqrtS);

	double GetBR(double);
	double GetXsection(double,TString);
	double GetXsection(double);
	double GetNorm(double,TH1F*,double, TH1F*,double);

	TGraph * GetSigmaGraph(TString process);
	TGraph * GetBrGraph();

  private:
	std::map<double,double> BranchingRatioMap;
	std::map<double,double> XSectionMap_ggh;
	std::map<double,double> XSectionMap_vbf;
	std::map<double,double> XSectionMap_wh;
	std::map<double,double> XSectionMap_zh;
	std::map<double,double> XSectionMap_tth;

  // Stage 0 STXS
	std::map<double,double> XSectionMap_QQ2HLNU;
	std::map<double,double> XSectionMap_QQ2HLL;
	std::map<double,double> XSectionMap_VH2HQQ;
	std::map<double,double> XSectionMap_WH2HQQ;
	std::map<double,double> XSectionMap_ZH2HQQ;

  /*
  // Stage 1 STXS
  std::map<double,double> XSectionMap_GG2H_0J;
  std::map<double,double> XSectionMap_GG2H_1J_PTH_0_60;
  std::map<double,double> XSectionMap_GG2H_1J_PTH_60_120;
  std::map<double,double> XSectionMap_GG2H_1J_PTH_120_200;
  std::map<double,double> XSectionMap_GG2H_1J_PTH_GT200;
  std::map<double,double> XSectionMap_GG2H_GE2J_PTH_0_60;
  std::map<double,double> XSectionMap_GG2H_GE2J_PTH_60_120;
  std::map<double,double> XSectionMap_GG2H_GE2J_PTH_120_200;
  std::map<double,double> XSectionMap_GG2H_GE2J_PTH_GT200;
  std::map<double,double> XSectionMap_GG2H_VBFTOPO_JET3VETO;
  std::map<double,double> XSectionMap_GG2H_VBFTOPO_JET3;
  std::map<double,double> XSectionMap_VBF_PTJET1_GT200;
  std::map<double,double> XSectionMap_VBF_VH2JET;
  std::map<double,double> XSectionMap_VBF_VBFTOPO_JET3VETO;
  std::map<double,double> XSectionMap_VBF_VBFTOPO_JET3;
  std::map<double,double> XSectionMap_VBF_REST;
  std::map<double,double> XSectionMap_TTH;
  std::map<double,double> XSectionMap_QQ2HLNU_PTV_GT250;
  std::map<double,double> XSectionMap_QQ2HLNU_PTV_150_250_GE1J;
  std::map<double,double> XSectionMap_QQ2HLNU_PTV_150_250_0J;
  std::map<double,double> XSectionMap_QQ2HLNU_PTV_0_150;
  std::map<double,double> XSectionMap_WH2HQQ_PTJET1_GT200;
  std::map<double,double> XSectionMap_WH2HQQ_VH2JET;
  std::map<double,double> XSectionMap_WH2HQQ_VBFTOPO_JET3VETO;
  std::map<double,double> XSectionMap_WH2HQQ_VBFTOPO_JET3;
  std::map<double,double> XSectionMap_WH2HQQ_REST;
  std::map<double,double> XSectionMap_QQ2HLL_PTV_GT250;
  std::map<double,double> XSectionMap_QQ2HLL_PTV_150_250_GE1J;
  std::map<double,double> XSectionMap_QQ2HLL_PTV_150_250_0J;
  std::map<double,double> XSectionMap_QQ2HLL_PTV_0_150;
  std::map<double,double> XSectionMap_ZH2HQQ_PTJET1_GT200;
  std::map<double,double> XSectionMap_ZH2HQQ_VH2JET;
  std::map<double,double> XSectionMap_ZH2HQQ_VBFTOPO_JET3VETO;
  std::map<double,double> XSectionMap_ZH2HQQ_VBFTOPO_JET3;
  std::map<double,double> XSectionMap_ZH2HQQ_REST;
  std::map<double,double> XSectionMap_BBH;
  std::map<double,double> XSectionMap_THQ;
  std::map<double,double> XSectionMap_THW;
  std::map<double,double> XSectionMap_GGZH;
  */

  // Stage 1.1 STXS
  std::map<double,double> XSectionMap_GG2H_PTH_GT200;
  std::map<double,double> XSectionMap_GG2H_0J_PTH_0_10;
  std::map<double,double> XSectionMap_GG2H_0J_PTH_GT10;
  std::map<double,double> XSectionMap_GG2H_1J_PTH_0_60;
  std::map<double,double> XSectionMap_GG2H_1J_PTH_60_120;
  std::map<double,double> XSectionMap_GG2H_1J_PTH_120_200;
  std::map<double,double> XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_0_60;
  std::map<double,double> XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_60_120;
  std::map<double,double> XSectionMap_GG2H_GE2J_MJJ_0_350_PTH_120_200;
  std::map<double,double> XSectionMap_GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  std::map<double,double> XSectionMap_GG2H_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  std::map<double,double> XSectionMap_GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  std::map<double,double> XSectionMap_GG2H_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  std::map<double,double> XSectionMap_VBF_0J;
  std::map<double,double> XSectionMap_VBF_1J;
  std::map<double,double> XSectionMap_VBF_GE2J_MJJ_0_60;
  std::map<double,double> XSectionMap_VBF_GE2J_MJJ_60_120;
  std::map<double,double> XSectionMap_VBF_GE2J_MJJ_120_350;
  std::map<double,double> XSectionMap_VBF_GE2J_MJJ_GT350_PTH_GT200;
  std::map<double,double> XSectionMap_VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  std::map<double,double> XSectionMap_VBF_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  std::map<double,double> XSectionMap_VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  std::map<double,double> XSectionMap_VBF_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  std::map<double,double> XSectionMap_WH2HQQ_0J;
  std::map<double,double> XSectionMap_WH2HQQ_1J;
  std::map<double,double> XSectionMap_WH2HQQ_GE2J_MJJ_0_60;
  std::map<double,double> XSectionMap_WH2HQQ_GE2J_MJJ_60_120;
  std::map<double,double> XSectionMap_WH2HQQ_GE2J_MJJ_120_350;
  std::map<double,double> XSectionMap_WH2HQQ_GE2J_MJJ_GT350_PTH_GT200;
  std::map<double,double> XSectionMap_WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  std::map<double,double> XSectionMap_WH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  std::map<double,double> XSectionMap_WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  std::map<double,double> XSectionMap_WH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  std::map<double,double> XSectionMap_ZH2HQQ_0J;
  std::map<double,double> XSectionMap_ZH2HQQ_1J;
  std::map<double,double> XSectionMap_ZH2HQQ_GE2J_MJJ_0_60;
  std::map<double,double> XSectionMap_ZH2HQQ_GE2J_MJJ_60_120;
  std::map<double,double> XSectionMap_ZH2HQQ_GE2J_MJJ_120_350;
  std::map<double,double> XSectionMap_ZH2HQQ_GE2J_MJJ_GT350_PTH_GT200;
  std::map<double,double> XSectionMap_ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_0_25;
  std::map<double,double> XSectionMap_ZH2HQQ_GE2J_MJJ_350_700_PTH_0_200_PTHJJ_GT25;
  std::map<double,double> XSectionMap_ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_0_25;
  std::map<double,double> XSectionMap_ZH2HQQ_GE2J_MJJ_GT700_PTH_0_200_PTHJJ_GT25;
  std::map<double,double> XSectionMap_QQ2HLNU_PTV_0_75;
  std::map<double,double> XSectionMap_QQ2HLNU_PTV_75_150;
  std::map<double,double> XSectionMap_QQ2HLNU_PTV_150_250_0J;
  std::map<double,double> XSectionMap_QQ2HLNU_PTV_150_250_GE1J;
  std::map<double,double> XSectionMap_QQ2HLNU_PTV_GT250;
  std::map<double,double> XSectionMap_QQ2HLL_PTV_0_75;
  std::map<double,double> XSectionMap_QQ2HLL_PTV_75_150;
  std::map<double,double> XSectionMap_QQ2HLL_PTV_150_250_0J;
  std::map<double,double> XSectionMap_QQ2HLL_PTV_150_250_GE1J;
  std::map<double,double> XSectionMap_QQ2HLL_PTV_GT250;
  std::map<double,double> XSectionMap_BBH;
  std::map<double,double> XSectionMap_TTH;
  std::map<double,double> XSectionMap_THQ;
  std::map<double,double> XSectionMap_THW;
  std::map<double,double> XSectionMap_GGZH;

};
#endif
