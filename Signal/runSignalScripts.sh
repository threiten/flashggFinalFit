#!/bin/bash

#bash variables
FILE="";
EXT="auto"; #extensiom for all folders and files created by this script
PROCS="ggh"
CATS="UntaggedTag_0,UntaggedTag_1,UntaggedTag_2,UntaggedTag_3,UntaggedTag_4,VBFTag_0,VBFTag_1,VBFTag_2"
SCALES="HighR9EE,LowR9EE,HighR9EB,LowR9EB"
SCALESCORR="MaterialCentral,MaterialForward"
SCALESGLOBAL="NonLinearity,Geant4,LightYield,Absolute"
SMEARS="HighR9EE,LowR9EE,HighR9EB,LowR9EB" #DRY RUN
<<<<<<< HEAD
=======
MASSLIST="120,125,130"
ANALYSIS="hig-16-040"
YEAR="2016"
>>>>>>> 558de8f... Background compatible with HTCondor submission
FTESTONLY=0
CALCPHOSYSTONLY=0
SKIPCALCPHOSYST=0
SIGFITONLY=0
SIGPLOTSONLY=0
INTLUMI=1
BATCH=""
QUEUE=""
BS=""
MHREF=""
REFPROC=""
REFPROCDIFF=""
REFTAGDIFF=""
REFTAGWV=""
REFPROCWV=""
NORMCUT=""
KEEPCURRENTFITS=0
NOSYSTS=0
SKIPSECONDARYMODELS=0
USEFTEST=0
NOSKIP=0
usage(){
	echo "The script runs three signal scripts in this order:"
		echo "signalFTest --> determines number of gaussians to use for fits of each Tag/Process"
		echo "calcPhotonSystConsts --> scale and smear ets of photons systematic variations"
		echo "SignalFit --> actually determine the number of gaussians to fit"
		echo "options:"
		echo "-h|--help) "
		echo "-i|--inputFile) "
		echo "-p|--procs) "
		echo "-f|--flashggCats) (default UntaggedTag_0,UntaggedTag_1,UntaggedTag_2,UntaggedTag_3,UntaggedTag_4,VBFTag_0,VBFTag_1,VBFTag_2,TTHHadronicTag,TTHLeptonicTag,VHHadronicTag,VHTightTag,VHLooseTag,VHEtTag)"
		echo "--ext)  (default auto)"
		echo "--fTestOnly) "
		echo "--calcPhoSystOnly) "
		echo "--skipCalcPhoSyst) "
		echo "--sigFitOnly) "
		echo "--sigPlotsOnly) "
		echo "--intLumi) specified in fb^-{1} (default $INTLUMI)) "
		echo "--year) Dataset year (default $YEAR)) "
		echo "--batch) which batch system to use (None (''),LSF,IC) (default '$BATCH')) "
		echo "--MHref)  reference mh for xsec ) "
		echo "--keepCurrentFits)  keep existing results from fit jobs, if there ) "
		echo "--noSysts)  no systematics included in signal model) "
		echo "--shiftOffDiag)  shift scale in off-diag elements of diff analysis) "
		echo "--noSkip)  do not skip datasets with conditions below minimum) "
		echo "--refProc)  ref replacement process)"                    
		echo "--refProcDiff)  ref replacement process for differentials)"
		echo "--refTagDiff)  ref replacement tag for differentials)"  
		echo "--refTagWV)  ref replacement tag for WV)"      
		echo "--refProcWV)  ref replacement proc for WV)"      
		echo "--normalisationCut) cut on datasets for final signal normalisation)"      
		echo "--skipSecondaryModels) Turn off creation of all additional models) "
		echo "--useFtest) Use f-test result as it is, without manual tuning) "
		echo "--queue) queue to submit jobs to (specific to batch)) "
}


#------------------------------ parsing


# options may be followed by one colon to indicate they have a required argument
if ! options=$(getopt -u -o hi:p:f: -l help,inputFile:,procs:,bs:,smears:,scales:,scalesCorr:,scalesGlobal:,flashggCats:,ext:,fTestOnly,calcPhoSystOnly,skipCalcPhoSyst,sigFitOnly,sigPlotsOnly,intLumi:,batch:,MHref:,keepCurrentFits,noSysts,shiftOffDiag,noSkip,refProc:,refProcDiff:,refTagDiff:,refTagWV:,refProcWV:,normalisationCut:,skipSecondaryModels,useFtest,queue: -- "$@")
then
# something went wrong, getopt will put out an error message for us
exit 1
fi
set -- $options

while [ $# -gt 0 ]
do
case $1 in
-h|--help) usage; exit 0;;
-i|--inputFile) FILE=$2; shift ;;
-p|--procs) PROCS=$2; shift ;;
--smears) SMEARS=$2; shift ;;
--scales) SCALES=$2; shift ;;
--scalesCorr) SCALESCORR=$2; shift ;;
--scalesGlobal) SCALESGLOBAL=$2; shift ;;
--bs) BS=$2; shift ;;
-f|--flashggCats) CATS=$2; shift ;;
--ext) EXT=$2; echo "test" ; shift ;;
--fTestOnly) FTESTONLY=1; echo "ftest" ;;
--calcPhoSystOnly) CALCPHOSYSTONLY=1;;
--skipCalcPhoSyst) SKIPCALCPHOSYST=1;;
--sigFitOnly) SIGFITONLY=1;;
--keepCurrentFits) KEEPCURRENTFITS=1;;
--noSysts) NOSYSTS=1;;
--shiftOffDiag) SHIFTOFFDIAG=1;;
--noSkip) NOSKIP=1;;
--sigPlotsOnly) SIGPLOTSONLY=1;;
--intLumi) INTLUMI=$2; shift ;;
--year) YEAR=$2; shift ;;
--batch) BATCH=$2; shift;;
--MHref) MHREF=$2; shift;;
--refProc) REFPROC=$2; shift;;
--refProcDiff) REFPROCDIFF=$2; shift;;
--refTagDiff) REFTAGDIFF=$2; shift;;
--refTagWV) REFTAGWV=$2; shift;;
--refProcWV) REFPROCWV=$2; shift;;
--normalisationCut) NORMCUT=$2; shift;;
--skipSecondaryModels) SKIPSECONDARYMODELS=1;;
--useFtest) USEFTEST=1;;
--queue) QUEUE=$2; shift;;

(--) shift; break;;
(-*) usage; echo "$0: error - unrecognized option $1" 1>&2; usage >> /dev/stderr; exit 1;;
(*) break;;
esac
shift
done

echo "INTLUMI $INTLUMI"

OUTDIR="outdir_$EXT"
echo "[INFO] outdir is $OUTDIR" 
if [[ $FILE == "" ]];then
echo "ERROR, input file (--inputFile or -i) is mandatory!"
exit 0
fi

if [ $FTESTONLY == 0 -a $CALCPHOSYSTONLY == 0 -a $SIGFITONLY == 0 -a $SIGPLOTSONLY == 0 ]; then
#IF not particular script specified, run all!
FTESTONLY=1
CALCPHOSYSTONLY=1
SIGFITONLY=1
SIGPLOTSONLY=1
fi
if [ $SKIPCALCPHOSYST == 1  ];then
CALCPHOSYSTONLY=0
fi

if [[ $BATCH == "IC" ]]; then
QUEUE=hep.q
echo "[INFO] Batch = $BATCH, using QUEUE = $QUEUE"
fi
if [[ $BATCH == "HTCONDOR" ]]; then
  if [[ $QUEUE == "" ]]; then
    QUEUE=espresso
    echo "[INFO] Batch = $BATCH, QUEUE not specified. Using QUEUE = $QUEUE"
  fi
  else
    echo "[INFO] Batch = $BATCH, Using QUEUE = $QUEUE"
fi

if [[ $BATCH == "T3CH" ]]; then
QUEUE='short.q -l h_vmem=4g'
fi

BSOPT=""
if [[ $BS == "" ]]; then
echo "NO BS SPECIFIED"
else
echo "BS IS $BS"
BSOPT=" --bs $BS"
fi

if [[ $MHREF == "" ]]; then
echo "NO MHREF SPECIFIED IN SIGNAL SH"
else
echo "MHREF IS $MHREF"
MHREFOPT=" --MHref $MHREF"
fi
####################################################
################## SIGNAL F-TEST ###################
####################################################
#ls dat/newConfig_${EXT}.dat
if [ -e ${OUTDIR}/dat/newConfig_${EXT}.dat ]; then
  echo "[INFO] sigFTest dat file $OUTDIR/dat/newConfig_${EXT}.dat already exists, so SKIPPING SIGNAL FTEST"
else
  echo "[INFO] sigFTest dat file $OUTDIR/dat/newConfig_${EXT}.dat  DOES NOT already exist, so PERFORMING SIGNAL FTEST"
  if [ $FTESTONLY == 1 ]; then
    mkdir -p $OUTDIR/fTest
    echo "=============================="
    echo "Running Signal F-Test"
    echo "-->Determine Number of gaussians"
    echo "=============================="
    if [ -z $BATCH ]; then
      echo "./bin/signalFTest -i $FILE -d dat/newConfig_$EXT.dat -p $PROCS -f $CATS -o $OUTDIR"
      ./bin/signalFTest -i $FILE -d dat/newConfig_$EXT.dat -p $PROCS -f $CATS -o $OUTDIR
    else
	echo "./python/submitSignalFTest.py --procs $PROCS --flashggCats $CATS --outDir $OUTDIR --i $FILE --batch $BATCH -q '$QUEUE'"
	./python/submitSignalFTest.py --procs $PROCS --flashggCats $CATS --outDir $OUTDIR --i $FILE --batch $BATCH -q "$QUEUE"
	wait $!
      PEND=`ls -l $OUTDIR/fTestJobs/sub*| grep -v "\.run" | grep -v "\.done" | grep -v "\.fail" | grep -v "\.err" |grep -v "\.sh" |grep -v "\.sub" |wc -l`
      TOTAL=`ls -l $OUTDIR/fTestJobs/sub*| grep "\.log"  |wc -l`
      echo "PEND $PEND"
      while (( $PEND > 0 )) ; do
        PEND=`ls -l $OUTDIR/fTestJobs/sub* | grep -v "\.run" | grep -v "\.done" | grep -v "\.fail" | grep -v "\.err" | grep -v "\.sh" | grep -v "\.out" | grep -v "\.sub" | wc -l`
        RUN=`ls -l $OUTDIR/fTestJobs/sub* | grep "\.run" | wc -l`
        FAIL=`ls -l $OUTDIR/fTestJobs/sub* | grep "\.fail" | wc -l`
        DONE=`ls -l $OUTDIR/fTestJobs/sub* | grep "\.done" | wc -l`
        (( PEND=$PEND-$RUN-$FAIL-$DONE ))
        echo " PEND $PEND - RUN $RUN - DONE $DONE - FAIL $FAIL"
        if (( $RUN > 0 )) ; then PEND=1 ; fi
	if (( $DONE == $TOTAL )) ; then PEND=0; fi
        if (( $FAIL > 0 )) ; then 
          echo "ERROR at least one job failed :"
          ls -l $OUTDIR/fTestJobs/sub* | grep "\.fail"
          exit 1
        fi
        sleep 10
      done
    fi
    mkdir -p $OUTDIR/dat
    cat $OUTDIR/fTestJobs/outputs/* > dat/newConfig_${EXT}_temp.dat
    sort -u dat/newConfig_${EXT}_temp.dat  > dat/tmp_newConfig_${EXT}_temp.dat 
    mv dat/tmp_newConfig_${EXT}_temp.dat dat/newConfig_${EXT}_temp.dat
    cp dat/newConfig_${EXT}_temp.dat $OUTDIR/dat/copy_newConfig_${EXT}_temp.dat
    rm -rf $OUTDIR/sigfTest
    mv $OUTDIR/fTest $OUTDIR/sigfTest
  fi
  echo "USEFTEST is (sigscript)"
  echo $USEFTEST
  if [ $USEFTEST == 1 ];then
      echo "[INFO] sigFtest completed"
      echo "[INFO] using the results of the F-test as they are and building the signal model"
      echo "If you want to amend the number of gaussians, do it in $PWD/dat/newConfig_${EXT}.dat and re-run!"
      cp dat/newConfig_${EXT}_temp.dat dat/newConfig_${EXT}.dat
      cp dat/newConfig_${EXT}_temp.dat $OUTDIR/dat/newConfig_${EXT}.dat
  else
      echo "[INFO] sigFTest jobs completed, check output and do:"
      echo "cp $PWD/dat/newConfig_${EXT}_temp.dat $PWD/dat/newConfig_${EXT}.dat"
      echo "and manually amend chosen number of gaussians using the output pdfs here:"
      echo "Signal/outdir_${EXT}/sigfTest/"
      echo "then re-run the same command to continue !"
      CALCPHOSYSTONLY=0
      SIGFITONLY=0
      SIGPLOTSONLY=0
      exit 1
  fi
fi
####################################################
################## CALCPHOSYSTCONSTS ###################
####################################################

if [ $CALCPHOSYSTONLY == 1 ]; then

  echo "=============================="
  echo "Running calcPho"
  echo "-->Determine effect of photon systematics"
  echo "=============================="

  echo "./bin/calcPhotonSystConsts --doPlots true -i $FILE -o dat/photonCatSyst_$EXT.dat -p $PROCS -s $SCALES -S $SCALESCORR -g $SCALESGLOBAL -r $SMEARS -D $OUTDIR -f $CATS"
  ./bin/calcPhotonSystConsts --doPlots true -i $FILE -o dat/photonCatSyst_$EXT.dat -p $PROCS -s $SCALES -S $SCALESCORR -g $SCALESGLOBAL -r $SMEARS -D $OUTDIR -f $CATS 
  cp dat/photonCatSyst_$EXT.dat $OUTDIR/dat/copy_photonCatSyst_$EXT.dat
fi
####################################################
####################### SIGFIT #####################
####################################################
if [ $SIGFITONLY == 1 ]; then
  DONE=`ls -l $OUTDIR/sigfit/SignalFitJobs/sub* | grep "\.done" |wc -l`
  LOGS=`ls -l $OUTDIR/sigfit/SignalFitJobs/sub* | grep "\.log" |wc -l`
  ERRS=`ls -l $OUTDIR/sigfit/SignalFitJobs/sub* | grep "\.err" |wc -l`
  if [ $DONE == $LOGS -a $LOGS == $ERRS -a $KEEPCURRENTFITS == 1 ]; then
      $KEEPCURRENTFITS == 1
  else
      $KEEPCURRENTFITS == 0
  fi
  if [ $KEEPCURRENTFITS == 0 ]; then  
    echo "=============================="
    echo "Running SignalFit"
    echo "-->Create actual signal model"
    echo "=============================="
  
#    SIGFITOPTS=''
#    if [ $SHIFTOFFDIAG == 1 ]; then
#	SIGFITOPTS= "${SIGFITOPTS} --shiftOffDiag"
#    fi
    
    echo "shiftOffDiag is $SHIFTOFFDIAG"
    SIGFITOPTS=""
    if [ $SHIFTOFFDIAG == 1 ]; then
	SIGFITOPTS="${SIGFITOPTS} --shiftOffDiag 1"
    fi
    if [ $NOSKIP == 1 ]; then
	SIGFITOPTS="${SIGFITOPTS} --noSkip 1"
    fi
    if [[ $REFPROC ]]; then
	SIGFITOPTS="${SIGFITOPTS} --refProc $REFPROC"
    fi
    if [[ $REFPROCDIFF ]]; then
	SIGFITOPTS="${SIGFITOPTS} --refProcDiff $REFPROCDIFF"
    fi
    if [[ $REFTAGDIFF ]]; then
	SIGFITOPTS="${SIGFITOPTS} --refTagDiff $REFTAGDIFF"
    fi
    if [[ $REFTAGWV ]]; then
	SIGFITOPTS="${SIGFITOPTS} --refTagWV $REFTAGWV"
    fi
    if [[ $REFPROCWV ]]; then
	SIGFITOPTS="${SIGFITOPTS} --refProcWV $REFPROCWV"
    fi
    if [[ $NORMCUT ]]; then
	SIGFITOPTS="${SIGFITOPTS} --normalisationCut $NORMCUT"
    fi
    if [ $SKIPSECONDARYMODELS == 1 ]; then
	SIGFITOPTS="${SIGFITOPTS} --skipSecondaryModels"
    fi    

    echo "runsigopt is $SIGFITOPTS"
    if [[ $BATCH == "" ]]; then
	if [[ $NOSYSTS == 0 ]]; then
	    echo "./bin/SignalFit -i $FILE -d dat/newConfig_$EXT.dat  --mhLow=120 --mhHigh=130 -s dat/photonCatSyst_$EXT.dat --procs $PROCS -o $OUTDIR/CMS-HGG_mva_13TeV_sigfit.root -p $OUTDIR/sigfit -f $CATS --changeIntLumi $INTLUMI $MHREFOPT $SIGFITOPTS"
	    ./bin/SignalFit -i $FILE -d dat/newConfig_$EXT.dat  --mhLow=120 --mhHigh=130 -s dat/photonCatSyst_$EXT.dat --procs $PROCS -o $OUTDIR/CMS-HGG_mva_13TeV_sigfit.root -p $OUTDIR/sigfit -f $CATS --changeIntLumi $INTLUMI $MHREFOPT $SIGFITOPTS#--pdfWeights 26 
	else
	    echo "./bin/SignalFit -i $FILE -d dat/newConfig_$EXT.dat  --mhLow=120 --mhHigh=130  --procs $PROCS -o $OUTDIR/CMS-HGG_mva_13TeV_sigfit.root -p $OUTDIR/sigfit -f $CATS --changeIntLumi $INTLUMI $MHREFOPT $SIGFITOPTS"
	    ./bin/SignalFit -i $FILE -d dat/newConfig_$EXT.dat  --mhLow=120 --mhHigh=130  --procs $PROCS -o $OUTDIR/CMS-HGG_mva_13TeV_sigfit.root -p $OUTDIR/sigfit -f $CATS --changeIntLumi $INTLUMI $MHREFOPT $SIGFITOPTS #--pdfWeights 26
	fi
    else
	if [[ $NOSYSTS == 0 ]]; then
	    echo " ./python/submitSignalFit.py -i $FILE -d dat/newConfig_$EXT.dat  --mhLow=120 --mhHigh=130 -s dat/photonCatSyst_$EXT.dat --procs $PROCS -o $OUTDIR/CMS-HGG_sigfit_$EXT.root -p $OUTDIR/sigfit -f $CATS --changeIntLumi $INTLUMI --batch $BATCH -q $QUEUE $BSOPT $SIGFITOPTS "
	    ./python/submitSignalFit.py -i $FILE -d dat/newConfig_$EXT.dat  --mhLow=120 --mhHigh=130 -s dat/photonCatSyst_$EXT.dat --procs $PROCS -o $OUTDIR/CMS-HGG_sigfit_$EXT.root -p $OUTDIR/sigfit -f $CATS --changeIntLumi $INTLUMI --batch $BATCH -q "$QUEUE" $BSOPT $MHREFOPT $SIGFITOPTS
	else
	    echo " ./python/submitSignalFit.py -i $FILE -d dat/newConfig_$EXT.dat  --mhLow=120 --mhHigh=130  --procs $PROCS -o $OUTDIR/CMS-HGG_sigfit_$EXT.root -p $OUTDIR/sigfit -f $CATS --changeIntLumi $INTLUMI --batch $BATCH -q $QUEUE $BSOPT $SIGFITOPTS"
	    ./python/submitSignalFit.py -i $FILE -d dat/newConfig_$EXT.dat  --mhLow=120 --mhHigh=130  --procs $PROCS -o $OUTDIR/CMS-HGG_sigfit_$EXT.root -p $OUTDIR/sigfit -f $CATS --changeIntLumi $INTLUMI --batch $BATCH -q "$QUEUE" $BSOPT $MHREFOPT $SIGFITOPTS
	fi
  
      PEND=`ls -l $OUTDIR/sigfit/SignalFitJobs/sub*| grep -v "\.run" | grep -v "\.done" | grep -v "\.fail" | grep -v "\.err" | grep -v "\.out" |grep -v "\.sub" |grep -v "\.sh" |wc -l`
      echo "PEND $PEND"
      while (( $PEND > 0 )) ;do
        PEND=`ls -l $OUTDIR/sigfit/SignalFitJobs/sub* | grep -v "\.run" | grep -v "\.done" | grep -v "\.fail" | grep -v "\.out" | grep -v "\.err" |grep -v "\.sub" |grep -v "\.sh" |wc -l`
        RUN=`ls -l $OUTDIR/sigfit/SignalFitJobs/sub* | grep "\.run" |wc -l`
        FAIL=`ls -l $OUTDIR/sigfit/SignalFitJobs/sub* | grep "\.fail" |wc -l`
        DONE=`ls -l $OUTDIR/sigfit/SignalFitJobs/sub* | grep "\.done" |wc -l`
        (( PEND=$PEND-$RUN-$FAIL-$DONE ))
        echo " PEND $PEND - RUN $RUN - DONE $DONE - FAIL $FAIL"
        if (( $RUN > 0 )) ; then PEND=1 ; fi
        if (( $FAIL > 0 )) ; then 
          echo "ERROR at least one job failed :"
          ls -l $OUTDIR/sigfit/SignalFitJobs/sub* | grep "\.fail"
          exit 1
        fi
        sleep 10
    
      done
    fi
    ls $PWD/$OUTDIR/CMS-HGG_sigfit_${EXT}_*.root > out.txt

    echo "ls ../Signal/$OUTDIR/CMS-HGG_sigfit_${EXT}_*.root > out.txt"
    counter=0
    while read p ; do
      if (($counter==0)); then
        SIGFILES="$p"
      else
        SIGFILES="$SIGFILES,$p"
      fi
      ((counter=$counter+1))
    done < out.txt
    echo "SIGFILES $SIGFILES"
    echo "./bin/PackageOutput -i $SIGFILES --procs $PROCS -l $INTLUMI -p $OUTDIR/sigfit -W wsig_13TeV -f $CATS -L 120 -H 130 -o $OUTDIR/CMS-HGG_sigfit_$EXT.root"
    ./bin/PackageOutput -i $SIGFILES --procs $PROCS -l $INTLUMI -p $OUTDIR/sigfit -W wsig_13TeV -f $CATS -L 120 -H 130 -o $OUTDIR/CMS-HGG_sigfit_$EXT.root > package.out

    #./makeSlides.sh $OUTDIR
    #scp fullslides.pdf lcorpe@lxplus.cern.ch:www/scratch/fullslides.pdf
    #exit 1
    # if [ $DONTPACKAGE == 0 ]; then
#       echo "./bin/PackageOutput -i $SIGFILES --procs $PROCS -l $INTLUMI -p $OUTDIR/sigfit -W wsig_13TeV -f $CATS -L 120 -H 130 -o $OUTDIR/CMS-HGG_sigfit_$EXT.root --year $YEAR"
#       ./bin/PackageOutput -i $SIGFILES --procs $PROCS -l $INTLUMI -p $OUTDIR/sigfit -W wsig_13TeV -f $CATS -L 120 -H 130 -o $OUTDIR/CMS-HGG_sigfit_$EXT.root --year $YEAR > package.out
#     fi
#   fi

# fi

# if [ $PACKAGEONLY == 1 ]; then
#   ls $OUTDIR/CMS-HGG_sigfit_${EXT}_*.root > out.txt
#   echo "ls ../Signal/$OUTDIR/CMS-HGG_sigfit_${EXT}_*.root > out.txt"
#   counter=0
#   while read p ; do
#     if (($counter==0)); then
#       #SIGFILES="$PWD/$p"
#       SIGFILES="$p"
#     else
#       #SIGFILES="$SIGFILES,$PWD/$p"
#       SIGFILES="$SIGFILES,$p"
#     fi
#     ((counter=$counter+1))
#   done < out.txt
#   echo "SIGFILES $SIGFILES"
#   echo ""
#   if [[ $BATCH == "" ]]; then
#     echo "./bin/PackageOutput -i $SIGFILES --procs $PROCS -l $INTLUMI -p $OUTDIR/sigfit -W wsig_13TeV -f $CATS -L 120 -H 130 -o $OUTDIR/CMS-HGG_sigfit_$EXT.root --year $YEAR"
#     ./bin/PackageOutput -i $SIGFILES --procs $PROCS -l $INTLUMI -p $OUTDIR/sigfit -W wsig_13TeV -f $CATS -L 120 -H 130 -o $OUTDIR/CMS-HGG_sigfit_$EXT.root --year $YEAR > package.out
#   else
#     echo "./python/submitPackager.py -i $SIGFILES --basepath $PWD --procs $PROCS -l $INTLUMI -p $OUTDIR/sigfit -W wsig_13TeV -f $CATS -L 120 -H 130 -o $OUTDIR/CMS-HGG_sigfit_$EXT.root --batch $BATCH -q $QUEUE --year $YEAR"
#     ./python/submitPackager.py -i $SIGFILES --basepath $PWD --procs $PROCS -l $INTLUMI -p $OUTDIR/sigfit -W wsig_13TeV -f $CATS -L 120 -H 130 -o $OUTDIR/CMS-HGG_sigfit_$EXT.root --batch $BATCH -q $QUEUE --year $YEAR

#     PEND=`ls -l $OUTDIR/sigfit/PackagerJobs/sub*| grep -v "\.run" | grep -v "\.done" | grep -v "\.fail" | grep -v "\.err" |grep -v "\.log" | grep -v "\.out" | grep -v "\.sub" | wc -l`
#     echo "PEND $PEND"
#     while (( $PEND > 0 )) ; do
#       PEND=`ls -l $OUTDIR/sigfit/PackagerJobs/sub* | grep -v "\.run" | grep -v "\.done" | grep -v "\.fail" | grep -v "\.err" | grep -v "\.log" | grep -v "\.out" | grep -v "\.sub" | wc -l`
#       RUN=`ls -l $OUTDIR/sigfit/PackagerJobs/sub* | grep "\.run" | wc -l`
#       FAIL=`ls -l $OUTDIR/sigfit/PackagerJobs/sub* | grep "\.fail" | wc -l`
#       DONE=`ls -l $OUTDIR/sigfit/PackagerJobs/sub* | grep "\.done" | wc -l`
#       (( PEND=$PEND-$RUN-$FAIL-$DONE ))
#       echo " PEND $PEND - RUN $RUN - DONE $DONE - FAIL $FAIL"
#       if (( $RUN > 0 )) ; then PEND=1 ; fi
#       if (( $FAIL > 0 )) ; then 
#         echo "[ERROR] at least one job failed :"
#         ls -l $OUTDIR/sigfit/PackagerJobs/sub* | grep "\.fail"
#         exit 1
#       fi
#       sleep 10
#     done

  fi
fi


#####################################################
#################### SIGNAL PLOTS ###################
#####################################################

if [ $SIGPLOTSONLY == 1 ]; then
  echo "=============================="
  echo "Make Signal Plots"
  echo "-->Create Validation plots"
  echo "=============================="
  
  if [ -z $BATCH ]; then
    echo " ./bin/makeParametricSignalModelPlots -i $OUTDIR/CMS-HGG_sigfit_$EXT.root  -o $OUTDIR -p $PROCS -f $CATS --year $YEAR"
    ./bin/makeParametricSignalModelPlots -i $OUTDIR/CMS-HGG_sigfit_$EXT.root  -o $OUTDIR/sigplots -p $PROCS -f $CATS --year $YEAR > signumbers_${EXT}.txt
    
    ./makeSlides.sh $OUTDIR
    mv fullslides.pdf $OUTDIR/fullslides_${EXT}.pdf
  else
    echo "./python/submitSignalPlots.py -i $OUTDIR/CMS-HGG_sigfit_$EXT.root -o $OUTDIR/sigplots -p $PROCS -f $CATS --batch $BATCH -q $QUEUE --year $YEAR"
    ./python/submitSignalPlots.py -i $OUTDIR/CMS-HGG_sigfit_$EXT.root -o $OUTDIR/sigplots -p $PROCS -f $CATS --batch $BATCH -q $QUEUE --year $YEAR

    PEND=`ls -l $OUTDIR/sigplots/PlottingJobs/sub*| grep -v "\.run" | grep -v "\.done" | grep -v "\.fail" | grep -v "\.err" |grep -v "\.log" | grep -v "\.out" | grep -v "\.sub" | wc -l`
    echo "PEND $PEND"
    while (( $PEND > 0 )) ; do
      PEND=`ls -l $OUTDIR/sigplots/PlottingJobs/sub* | grep -v "\.run" | grep -v "\.done" | grep -v "\.fail" | grep -v "\.err" | grep -v "\.log" | grep -v "\.out" | grep -v "\.sub" | wc -l`
      RUN=`ls -l $OUTDIR/sigplots/PlottingJobs/sub* | grep "\.run" | wc -l`
      FAIL=`ls -l $OUTDIR/sigplots/PlottingJobs/sub* | grep "\.fail" | wc -l`
      DONE=`ls -l $OUTDIR/sigplots/PlottingJobs/sub* | grep "\.done" | wc -l`
      (( PEND=$PEND-$RUN-$FAIL-$DONE ))
      echo " PEND $PEND - RUN $RUN - DONE $DONE - FAIL $FAIL"
      if (( $RUN > 0 )) ; then PEND=1 ; fi
      if (( $FAIL > 0 )) ; then 
        echo "[ERROR] at least one job failed :"
        ls -l $OUTDIR/sigplots/PlottingJobs/sub* | grep "\.fail"
        exit 1
      fi
      sleep 10
    done
    cat $OUTDIR/sigplots/PlottingJobs/sub*.log > signumbers_${EXT}.txt
  fi
fi
