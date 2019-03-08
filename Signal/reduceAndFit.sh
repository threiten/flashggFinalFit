OBS="test"
EXT="test_EXT"

DOSPLIT=1
echo "DOSPLIT n.1"
echo $DOSPLIT


usage(){
    echo "The script runs splitting of the workspaces, sig and bkg model construction and final combine fits:"
    echo "options:"
    echo "-h|--help)" 
    echo "--skipSplit, skip splitting of workspaces)" 
    echo "--obs, observable name in trees)" 
    echo "--ext, extension of this round of this obervable)" 
    echo "--dirIA, directory of Inside Acceptance ntuples)" 
    echo "--dirOA, directory of Outside Acceptance ntuples)" 
    echo "--dirData, directory of Data ntuples)" 
##    echo "--replacements, replacements for signal model)" 
    echo "--refTagDiff, reference replacement tag for differentials )" 
    echo "--refTagWV, reference replacement tag for WV datasets )" 
    echo "--refProc, reference replacement process )" 
    echo "--refProcDiff, reference replacement process for differentials )" 
    echo "--refProcWV, reference replacement process for WV datasets )" 
    echo "--range, range for POIs scan )" 
    echo "--npoints, number of points for POIs scan )" 
    echo "--noSkip, do not skip datasets below min conditions for signal model)" 
    echo "--sigModOpt, more options to be propagated to signal model scripts )" 
    echo "--bkgModOpt, more options to be propagated to bkg model scripts )"    
    echo "--DatacardOpt, more options to be propagated to datacard scripts )"      
    echo "--runCombineOnly, assume models and datacard are there and only run combine )" 
    echo "--runDatacardOnly, produce datacard only, assuming inputs are already there )" 
    echo "--runSignalOnly, produce singnal models only )" 
    echo "--runBackgorundOnly, produce bkg only )" 
    echo "--shiftOffDiag, correct for bias in off-diag elements when using diag replacement )" 
    echo "--unblind, unblind )" 
}

if ! options=$(getopt -u -o hi:p:f: -l help,skipSplit,obs:,ext:,dirIA:,dirOA:,dirData:,shiftOffDiag:,refTagDiff:,refTagWV:,refProc:,refProcDiff:,refProcWV:,range:,npoints:,noSkip:,sigModOpt:,bkgModOpt:,DatacardOpt:,runCombineOnly:,runDatacardOnly:,runSignalOnly:,runBackgroundOnly:,unblind: -- "$@")
then
# something went wrong, getopt will put out an error message for us
    exit 1
fi
set -- $options


NTUPLES_DIR_IA=""
NTUPLES_DIR_OA=""
NTUPLES_DIR_DATA=""
#REPLACEMENTS=""
REFTAGDIFF="cat0"
REFTAGWV="cat0"
REFPROC="ggh"
REFPROCDIFF="ggh"
REFPROCWV="ggh"
NOSKIP=0
SHIFTOFFDIAG=1
SIGMODOPT=""
BKGMODOPT=""
DATACARDOPT=""
COMBINEONLY=0
DATACARDONLY=0
SIGNALONLY=0
BKGONLY=0
RANGE="[1,-1.0,3.0]"
NPOINTS="20"
UNBLIND=0


echo "DOSPLIT n.2"
echo $DOSPLIT


while [ $# -gt 0 ]
do
    case $1 in
	-h|--help) usage; exit 0;;
	--skipSplit) DOSPLIT=0;;
	--obs) OBS=$2; shift ;;
	--ext) EXT=$2; shift ;;
	--dirIA) NTUPLES_DIR_IA=$2; shift ;;
	--dirOA) NTUPLES_DIR_OA=$2; shift ;;
	--dirData) NTUPLES_DIR_DATA=$2; shift ;;
##	--replacements) REPLACEMENTS=$@2; shift ;;
	--refTagDiff) REFTAGDIFF=$2; shift;;
	--refTagWV) REFTAGWV=$2; shift;;
	--refProc) REFPROC=$2; shift;;
	--refProcDiff) REFPROCDIFF=$2; shift;;
	--refProcWV) REFPROCWV=$2; shift;;
	--range) RANGE=$2; shift;;
	--npoints) NPOINTS=$2; shift;;
	--noSkip) NOSKIP=$2; shift;;
	--sigModOpt) SIGMODOPT=$2; shift;;
	--bkgModOpt) BKGMODOPT=$2; shift;;
	--DatacardOpt) DATACARDOPT=$2; shift;;
	--runCombineOnly) COMBINEONLY=$2; shift;;
	--runDatacardOnly) DATACARDONLY=$2; shift;;
	--runSignalOnly) SIGNALONLY=$2; shift;;
	--runBackgroundOnly) BKGONLY=$2; shift;;
	--shiftOffDiag) SHIFTOFFDIAG=$2; shift;;
	--unblind) UNBLIND=$2; shift;;

	(--) shift; break;;
	(-*) usage; echo "$0: error - reduceAndFit.sh - unrecognized option $1" 1>&2; usage >> /dev/stderr; exit 1;;
	(*) break;;
    esac
    shift
done

echo "DOSPLIT n.3"
echo $DOSPLIT


echo "EXT"
echo $EXT

echo "Replacements"
echo "$REPLACEMENTS"

echo "DATACARDOPT"
echo "${DATACARDOPT}"

echo "DOSPLIT"
echo $DOSPLIT

if [ $DOSPLIT == 1 ]; then
 #   NTUPLES_DIR_IA="root://t3dcachedb.psi.ch:1094///pnfs/psi.ch/cms/trivcat/store/user/vtavolar/Differentials_ntuples/July_tightPuJetId/IA/"
 #   NTUPLES_DIR_OA="root://t3dcachedb.psi.ch:1094///pnfs/psi.ch/cms/trivcat/store/user/vtavolar/Differentials_ntuples/July_tightPuJetId/OA/"
 #   NTUPLES_DIR_DATA="root://t3dcachedb.psi.ch:1094///pnfs/psi.ch/cms/trivcat/store/user/vtavolar/Differentials_ntuples/July_tightPuJetId/data/"
    
    python submitSplitWs.py -q "short.q" -b "T3CH" -i ${NTUPLES_DIR_IA}/allSig_120.root -j jsons/${EXT}/reduce_cfg_mc_${OBS}_120.json -o reduced${OBS}IA.root  --outdir m120_${EXT}/
    python submitSplitWs.py -q "short.q" -b "T3CH" -i ${NTUPLES_DIR_IA}/allSig_130.root -j jsons/${EXT}/reduce_cfg_mc_${OBS}_130.json -o reduced${OBS}IA.root  --outdir m130_${EXT}/
    python submitSplitWs.py -q "short.q" -b "T3CH" -i ${NTUPLES_DIR_IA}/allSig_125.root -j jsons/${EXT}/reduce_cfg_mc_${OBS}_125.json -o reduced${OBS}IA.root  --outdir m125_${EXT}/  --doSysts ###--doPdfWeights
    
    RUN=0
    DONE=0
    FAIL=0
    TOTAL=0
    DIR=m125_${EXT}
    TOTAL=`find ${DIR} -name "*.sh" | wc -l`
    RUN=`find ${DIR} -name "*.sh.run" | wc -l`
    DONE=`find ${DIR} -name "*.sh.done" | wc -l`
    FAIL=`find ${DIR} -name "*.sh.fail" | wc -l`
    
    while [ $DONE -lt $TOTAL  ]
    do
	TOTAL=`find ${DIR} -name "*.sh" | wc -l`
	RUN=`find ${DIR} -name "*.sh.run" | wc -l`
	DONE=`find ${DIR} -name "*.sh.done" | wc -l`
	FAIL=`find ${DIR} -name "*.sh.fail" | wc -l`
	if [ $FAIL -gt 0 ]; then
	    echo "TOTAL: ${TOTAL}, RUNNING: ${RUN}, DONE: ${DONE}, FAILED: ${FAIL}"
	    exit
	fi
	sleep 5
	echo "TOTAL: ${TOTAL}, RUNNING: ${RUN}, DONE: ${DONE}, FAILED: ${FAIL}"
	done
fi	

PROCNAMES=""
CATNAMES=""
cd m125_${EXT}/
if [ $DOSPLIT == 1 ]; then
    ../bin/hadd_workspaces reduced${OBS}IA.root reduced${OBS}IA_gen${OBS}_*root
fi
PROCNAMES=$PROCNAMES`head -n1 proc_cat_names_reco${OBS}.txt | tail -1`
CATNAMES=$CATNAMES`head -n2 proc_cat_names_reco${OBS}.txt | tail -1`
cd -
echo "PROCNAMES"
echo $PROCNAMES
echo "CATNAMES" 
echo $CATNAMES


if [ $DOSPLIT == 1 ]; then
    python submitSplitWs.py -q "short.q" -b "T3CH" -i ${NTUPLES_DIR_OA}/allSig_OA_120.root -j jsons/${EXT}/reduce_cfg_mc_${OBS}_OA_120.json -o reduced${OBS}OA.root  --outdir m120_OA_${EXT}/
    python submitSplitWs.py -q "short.q" -b "T3CH" -i ${NTUPLES_DIR_OA}/allSig_OA_130.root -j jsons/${EXT}/reduce_cfg_mc_${OBS}_OA_130.json -o reduced${OBS}OA.root  --outdir m130_OA_${EXT}/
    python submitSplitWs.py -q "short.q" -b "T3CH" -i ${NTUPLES_DIR_OA}/allSig_OA_125.root -j jsons/${EXT}/reduce_cfg_mc_${OBS}_OA_125.json -o reduced${OBS}OA.root  --outdir m125_OA_${EXT}/ --doSysts ###--doPdfWeights
    
    RUN=0
    DONE=0
    FAIL=0
    TOTAL=0
    DIR=m125_OA_${EXT}
    TOTAL=`find ${DIR} -name "*.sh" | wc -l`
    RUN=`find ${DIR} -name "*.sh.run" | wc -l`
    DONE=`find ${DIR} -name "*.sh.done" | wc -l`
    FAIL=`find ${DIR} -name "*.sh.fail" | wc -l`
    
    while [ $DONE -lt $TOTAL  ]
    do
	TOTAL=`find ${DIR} -name "*.sh" | wc -l`
	RUN=`find ${DIR} -name "*.sh.run" | wc -l`
	DONE=`find ${DIR} -name "*.sh.done" | wc -l`
	FAIL=`find ${DIR} -name "*.sh.fail" | wc -l`
	if [ $FAIL -gt 0 ]; then
	    echo "TOTAL: ${TOTAL}, RUNNING: ${RUN}, DONE: ${DONE}, FAILED: ${FAIL}"
	    exit
	fi
	sleep 5
	echo "TOTAL: ${TOTAL}, RUNNING: ${RUN}, DONE: ${DONE}, FAILED: ${FAIL}"
	done
	
	cd m125_OA_${EXT}/
	../bin/hadd_workspaces reduced${OBS}OA.root reduced${OBS}OA_*root
	cd -
fi

PROCNAMES=$PROCNAMES",OutsideAcceptance"
echo "PROCNAMES"
echo $PROCNAMES
echo "CATNAMES" 
echo $CATNAMES

if [ $DOSPLIT == 1 ]; then
    cd m125_${EXT}/
    ../bin/hadd_workspaces reduced${OBS}IAOA.root reduced${OBS}IA.root ../m125_OA_${EXT}/reduced${OBS}OA.root
    cd -
    
    
    python submitSplitWs.py -q "short.q" -b "T3CH" -i ${NTUPLES_DIR_DATA}/allData.root  -j jsons/${EXT}/reduce_cfg_data_${OBS}_data.json -o reduced${OBS}Data.root --outdir data_${EXT}/
    RUN=0
    DONE=0
    FAIL=0
    TOTAL=0
    DIR=data_${EXT}
    TOTAL=`find ${DIR} -name "*.sh" | wc -l`
    RUN=`find ${DIR} -name "*.sh.run" | wc -l`
    DONE=`find ${DIR} -name "*.sh.done" | wc -l`
    FAIL=`find ${DIR} -name "*.sh.fail" | wc -l`
    
    while [ $DONE -lt $TOTAL  ]
    do
	TOTAL=`find ${DIR} -name "*.sh" | wc -l`
	RUN=`find ${DIR} -name "*.sh.run" | wc -l`
	DONE=`find ${DIR} -name "*.sh.done" | wc -l`
	FAIL=`find ${DIR} -name "*.sh.fail" | wc -l`
	if [ $FAIL -gt 0 ]; then
	    echo "TOTAL: ${TOTAL}, RUNNING: ${RUN}, DONE: ${DONE}, FAILED: ${FAIL}"
	    exit
	fi
	sleep 5
	echo "TOTAL: ${TOTAL}, RUNNING: ${RUN}, DONE: ${DONE}, FAILED: ${FAIL}"
	done
fi


cd ..

if [ ! -e runFinalFitsScripts_differential_${EXT}.sh ]; then
#    cp runFinalFitsScripts_differential_MjjEta2p5_template.sh runFinalFitsScripts_differential_${EXT}.sh
    cp runFinalFitsScripts_differential_template.sh runFinalFitsScripts_differential_${EXT}.sh
fi
###usage: bash runFinalFitsScripts_differential_${EXT}.sh <OBS> <EXT> <PROCS> <CATS> <refTagDiff> <refTagWV> <refProcWV> <refProcDiff> <refProc>
set -x
##bash runFinalFitsScripts_differential_${EXT}.sh --obs=$OBS --ext=$EXT --procs=$PROCNAMES --cats=$CATNAMES --refTagDiff="recoAbsDeltaPhiGgJjEta4p7VBFlike_2p9_3p05_SigmaMpTTag_1" --refTagWV="recoAbsDeltaPhiGgJjEta4p7VBFlike_m1000p0_0p0_SigmaMpTTag_1" --refProcWV="InsideAcceptance_genAbsDeltaPhiGgJjEta4p7VBFlike_m1000p0_0p0" --refProcDiff="InsideAcceptance_genAbsDeltaPhiGgJjEta4p7VBFlike_2p9_3p05" --refProc="InsideAcceptance_genAbsDeltaPhiGgJjEta4p7VBFlike_2p9_3p05" --range="[1,-1.0,3.0]"

echo "reduceAndFit_Jets.sh: RANGE"
echo ${RANGE}

bash runFinalFitsScripts_differential_${EXT}.sh --obs=$OBS --ext=$EXT --procs=$PROCNAMES --cats=$CATNAMES --refTagDiff=$REFTAGDIFF --refTagWV=$REFTAGWV --refProcWV=$REFPROCWV --refProcDiff=$REFPROCDIFF --refProc=$REFPROC --noSkip=$NOSKIP --runCombineOnly=$COMBINEONLY --runDatacardOnly=$DATACARDONLY --runSignalOnly=$SIGNALONLY --runBackgroundOnly=$BKGONLY --range=${RANGE} --npoints=${NPOINTS} --shiftOffDiag=${SHIFTOFFDIAG} --unblind=${UNBLIND} --inputDir="/t3home/threiten/Hgg/Differentials/CMSSW_7_4_7/src/flashggFinalFit/Signal" ##--sigModOpt="${SIGMODOPT}"  ### --DatacardOpt="${DATACARDOPT} " #####--bkgModOpt="${BKGMODOPT}"    ##"${SPECIFICOPTS}" 

mkdir /afs/cern.ch/user/v/vtavolar/www/DiffHggPt/outdir_Differential_SignalModel_ICHEPconditions_1bis_newphp/afterMoriond17/ub17025/outdir_differential_${EXT}
cp Background/outdir_differential_${EXT}/bkgPlots-Data/bkgplot_* /afs/cern.ch/user/v/vtavolar/www/DiffHggPt/outdir_Differential_SignalModel_ICHEPconditions_1bis_newphp/afterMoriond17/ub17025/outdir_differential_${EXT}

#python plotMusFromTxt.py --files /mnt/t3nfs01/data01/shome/vtavolar/FinalFits/CMSSW_7_1_5/src/flashggFinalFit/Plots/FinalResults/expectedPrecision_differential_${EXT}_np20.txt -V ${OBS} --resizeFirst -1  --skipFirstInMean --outdir m125_${EXT}
#python plotMusFromTxt.py --files /mnt/t3nfs01/data01/shome/vtavolar/FinalFits/CMSSW_7_1_5/src/flashggFinalFit/Plots/FinalResults/expectedPrecision_differential_${EXT}_np20.txt -V ${OBS} --resizeFirst -1  --skipFirstInMean
#python makeResponseMatrix.py -V ${OBS} --files  m125_${EXT}/reduced${OBS}IA_gen${OBS}_nominal.root   --logz  --resizeFirst -1 --outdir m125_${EXT}/
#python makeResponseMatrix.py -V ${OBS} --files  m125_${EXT}/reduced${OBS}IA_gen${OBS}_nominal.root   --logz  --resizeFirst -1 --outdir plots/
#mkdir /afs/cern.ch/user/v/vtavolar/www/DiffHggPt/outdir_Differential_SignalModel_ICHEPconditions_1bis_newphp/afterMoriond17/Background/${EXT}
#cp ../Background/outdir_differential_${EXT}/bkgfTest-Data/multipdf_reco${OBS}_* /afs/cern.ch/user/v/vtavolar/www/DiffHggPt/outdir_Differential_SignalModel_ICHEPconditions_1bis_newphp/afterMoriond17/Background/${EXT}
