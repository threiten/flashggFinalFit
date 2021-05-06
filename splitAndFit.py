import argparse
import os
import subprocess
import shutil as shl
import oyaml as yaml

cmssw_path = os.environ['CMSSW_BASE']

rootDir = {
    '16': {
        'IA': '/eos/home-t/threiten/Analysis/Differentials/2016ReReco/dev_differential_fixSystNNLOPS_2PsEoE_signal_IA_16/',
        'OA': '/eos/home-t/threiten/Analysis/Differentials/2016ReReco/dev_differential_fixSystNNLOPS_2PsEoE_signal_OA_16/',
        'Data': '/eos/home-t/threiten/Analysis/Differentials/2016ReReco/dev_differential_wJMET_reRunSum20_data_16/'
    },
    '17': {
        'IA': '/eos/home-t/threiten/Analysis/Differentials/2017ReReco/dev_differential_fixSystNNLOPS_2PsEoE_signal_IA_17/',
        'OA': '/eos/home-t/threiten/Analysis/Differentials/2017ReReco/dev_differential_fixSystNNLOPS_2PsEoE_signal_OA_17/',
        'Data': '/eos/home-t/threiten/Analysis/Differentials/2017ReReco/dev_differential_wJMET_reRunSum20_data_17/'
    },
    'UL17' : {
        'IA': '/eos/home-s/spigazzi/ntuples/Hgg/Differentials/UL2017/dev_differential_firstUL17_signal_IA_UL17/',
        'OA': '/eos/home-s/spigazzi/ntuples/Hgg/Differentials/UL2017/dev_differential_firstUL17_signal_OA_UL17/',
        'Data' : '/eos/home-s/spigazzi/ntuples/Hgg/Differentials/UL2017/dev_differential_firstUL17_data_UL17/'
    },
    '18': {
        'IA': '/eos/home-t/threiten/Analysis/Differentials/2018ReABCPromptDReco/dev_differential_fixSystNNLOPS_2PsEoE_signal_IA_18/',
        'OA': '/eos/home-t/threiten/Analysis/Differentials/2018ReABCPromptDReco/dev_differential_fixSystNNLOPS_2PsEoE_signal_OA_18/',
        'Data': '/eos/home-t/threiten/Analysis/Differentials/2018ReABCPromptDReco/dev_differential_wJMET_reRunSum20_data_18/'
    }}

replacementDic = {
    '16': yaml.load(open('{0}/src/flashggFinalFit/replacementSignalModel_2016.yaml'.format(cmssw_path))),
    '17': yaml.load(open('{0}/src/flashggFinalFit/replacementSignalModel_2017.yaml'.format(cmssw_path))),
    'UL17': yaml.load(open('{0}/src/flashggFinalFit/replacementSignalModel_UL17.yaml'.format(cmssw_path))),
    '18': yaml.load(open('{0}/src/flashggFinalFit/replacementSignalModel_2018.yaml'.format(cmssw_path)))
}

varDic = {
    'Pt': 'Pt',
    'Njets2p5': 'Njets2p5',
    'Njets2p5_testNames': 'Njets2p5',
    'CosThetaS': 'CosThetaS',
    'AbsRapidity': 'AbsRapidity',
    'AbsRapidityFine': 'AbsRapidity',
    'Jet2p5Pt0': 'Jet2p5Pt0',
    'Jet2p5AbsRapidity0': 'Jet2p5AbsRapidity0',
    'Jet2p5AbsRapidity0_testShUnc': 'Jet2p5AbsRapidity0',
    'AbsDeltaRapidityGgJet2p50': 'AbsDeltaRapidityGgJet2p50',
    'AbsDeltaPhiGgJet2p50': 'AbsDeltaPhiGgJet2p50',
    'Jet4p7Pt1': 'Jet4p7Pt1',
    'Jet4p7AbsRapidity1': 'Jet4p7AbsRapidity1',
    'AbsDeltaPhiGgJjJets4p7': 'AbsDeltaPhiGgJjJets4p7',
    'AbsDeltaPhiJ1J2Jets4p7': 'AbsDeltaPhiJ1J2Jets4p7',
    'AbsZeppenfeldEtaJets4p7': 'AbsZeppenfeldEtaJets4p7',
    'MjjJets4p7': 'MjjJets4p7',
    'MjjJets4p7NewBins': 'MjjJets4p7',
    'AbsDeltaEtaJ1J2Jets4p7': 'AbsDeltaEtaJ1J2Jets4p7',
    'Nleptons': 'Nleptons',
    'NBjets2p5': 'NBjets2p5',
    'PtMiss': 'PtMiss',
    'Jet4p7Pt1VBFlike': 'Jet4p7Pt1',
    'AbsDeltaPhiGgJjJets4p7VBFlike': 'AbsDeltaPhiGgJjJets4p7',
    'AbsDeltaPhiJ1J2Jets4p7VBFlike': 'AbsDeltaPhiJ1J2Jets4p7',
    'PtVBFlike': 'Pt',
    'TauCJets4p7': 'TauCJets4p7',
    'AbsPhiS': 'AbsPhiS',
    'PtInclusive': 'Pt',
    'PtInclusive1L1B': 'Pt',
    'PtInclusive1LHPtM': 'Pt',
    'PtInclusive1LLPtM': 'Pt',
    'PtTauCJets4p7': 'Pt',
    'PtNjets2p5': 'Pt'
}

labels = {
    '16' : '16',
    '17' : '17',
    'UL17' : '17',
    '18' : '18'
}

inpDir = '/eos/home-t/threiten/Analysis/Differentials/FinalFitsInDir'

def getIAOA(proc):

    if proc == 'IA':
        return ''
    elif proc == 'OA':
        return '_OA'
    else:
        raise NotImplementedError('Process Name not recognized!')


def main(options):

    variable = varDic[options.extension]
    commands = {}
    if options.year is None:
        if options.wUL17:
            years = ['16', 'UL17', '18']
        else:
            years = ['16', '17', '18']
    else:
        years = [options.year]
        options.noDatacard = True
        
    if not options.noSplit:
        subPs = []
        for year in years:
            yrLabel = labels[year]
            splitLabel = '20{}'.format(year) if not 'UL' in year else year
            subPs.append(subprocess.Popen('python {4}/src/flashggFinalFit/splitTree.py -c {4}/src/flashggFinalFit/splitConfig_{1}_{7}.yaml -i {2}/allData.root -o {5}/data_{1}_{0} -l {6} -p Data'.format(
                year, options.extension, rootDir[year]['Data'], variable, cmssw_path, inpDir, yrLabel, splitLabel), shell=True))
            commands['split_{}_Data'.format(year)] = subPs[-1].args
            if not options.parallel:
                subPs[-1].wait()
            # for proc in ['IA', 'OA']:
            retCodesL = []
            for mass in ['120', '125', '130']:
                subPs.append(subprocess.Popen('python {6}/src/flashggFinalFit/splitTree.py -c {6}/src/flashggFinalFit/splitConfig_{1}_{11}.yaml -i {2}/allSig_{3}{4}.root -o {9}/m{3}{4}_{1}_{0} -l {10} --infileOA {8}/allSig_{3}{7}.root --outfolderOA {9}/m{3}{7}_{1}_{0} -p SIG_{3} --cluster joblib'.format(
                    year, options.extension, rootDir[year]['IA'], mass, getIAOA('IA'), variable, cmssw_path, getIAOA('OA'), rootDir[year]['OA'], inpDir, yrLabel, splitLabel), shell=True))
                commands['split_{}_SIG_{}'.format(
                    year, mass)] = subPs[-1].args
                if not options.parallel:
                    subPs[-1].wait()
        if options.parallel:
            retCodesL = [subp.wait() for subp in subPs]
                    

        yaml.dump(commands, open(
            '{1}/src/flashggFinalFit/commands_{0}.yaml'.format(options.extension, cmssw_path), 'w'))
        retCodes = [subp.wait() for subp in subPs]

    # -------------------FTest---------------------------------------
    if options.ftest:
        sFitsP = []
        for year in years:
            ext = '{0}_{1}'.format(options.extension, year)
            yrLabel = labels[year]
            refTagWV = replacementDic[year][options.extension]['tagWV'] if 'tagWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['tag']
            refProcWV = replacementDic[year][options.extension]['procWV'] if 'procWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['proc']
            replStr = '--refTagDiff={0} --refTagWV={2} --refProcWV={3} --refProcDiff={1} --refProc={1}'.format(
                replacementDic[year][options.extension]['tag'], replacementDic[year][options.extension]['proc'], refTagWV, refProcWV)
            sFitsP.append(subprocess.Popen(r'bash runFinalFitsScripts_differential_univ_{0}.sh --obs={2} --ext={1} --procs=$(head -n1 {6}/m125_{1}/proc_cat_names_gen{2}.txt | tail -1),OutsideAcceptance --cats=$(head -n2 {6}/m125_{1}/proc_cat_names_gen{2}.txt | tail -1) {3} --runSignalOnly=1 --queue={5} --inputDir={6}'.format(
                yrLabel, ext, variable, replStr, cmssw_path, options.fTestQueue, inpDir), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True))
            commands['fTest_{}'.format(year)] = sFitsP[-1].args
            if not options.parallel:
                sFitsP[-1].wait()

        yaml.dump(commands, open(
            '{1}/src/flashggFinalFit/commands_{0}.yaml'.format(options.extension, cmssw_path), 'w'))
        retCodes = [pc.wait() for pc in sFitsP]

        for year in years:
            ext = '{0}_{1}'.format(options.extension, year)
            shl.copyfile('{1}/src/flashggFinalFit/Signal/dat/newConfig_differential_{0}_temp.dat'.format(
                ext, cmssw_path), '{1}/src/flashggFinalFit/Signal/dat/newConfig_differential_{0}.dat'.format(ext, cmssw_path))
            os.remove('{1}/src/flashggFinalFit/Signal/dat/newConfig_differential_{0}_temp.dat'.format(ext, cmssw_path))
            if os.path.exists('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}/dat/newConfig_differential_{0}.dat'.format(
                    ext, cmssw_path)):
                shl.copyfile('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}/dat/newConfig_differential_{0}.dat'.format(
                    ext, cmssw_path), '{1}/src/flashggFinalFit/Signal/outdir_differential_{0}/dat/newConfig_differential_{0}_Old.dat'.format(ext, cmssw_path))
                os.remove(
                    '{1}/src/flashggFinalFit/Signal/outdir_differential_{0}/dat/newConfig_differential_{0}.dat'.format(ext, cmssw_path))
            os.symlink('{1}/src/flashggFinalFit/Signal/dat/newConfig_differential_{0}.dat'.format(ext, cmssw_path),
                       '{1}/src/flashggFinalFit/Signal/outdir_differential_{0}/dat/newConfig_differential_{0}.dat'.format(ext, cmssw_path))

    # ----------------------------Signal-------------------------------
    if not options.noSig:
        sFitsP = []
        sigModOpt = '--sigModOpt=--skipCalcPhoSyst' if options.skipCalcPhoSyst else ''
        for year in years:
            ext = '{0}_{1}'.format(options.extension, year)
            yrLabel = labels[year]
            if not os.path.exists('{0}/src/flashggFinalFit/Signal/dat/newConfig_differential_{1}_BA.dat'.format(cmssw_path, ext)) and os.path.exists('{0}/src/flashggFinalFit/Signal/dat/newConfig_differential_{1}.dat'.format(cmssw_path, ext)) and not year == 'UL17':
                upDatFileP = subprocess.Popen('python updateDatFile.py -d Signal/dat/newConfig_differential_{0}.dat -r procCatNameReplacments.yaml'.format(ext), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True)
                commands['UpdateDatFile_{}'.format(year)] = upDatFileP.args
                upDatFileP.wait()
            refTagWV = replacementDic[year][options.extension]['tagWV'] if 'tagWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['tag']
            refProcWV = replacementDic[year][options.extension]['procWV'] if 'procWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['proc']
            replStr = '--refTagDiff={0} --refTagWV={2} --refProcWV={3} --refProcDiff={1} --refProc={1}'.format(
                replacementDic[year][options.extension]['tag'], replacementDic[year][options.extension]['proc'], refTagWV, refProcWV)
            sFitsP.append(subprocess.Popen(r'bash runFinalFitsScripts_differential_univ_{0}.sh --obs={2} --ext={1} --procs=$(head -n1 {6}/m125_{1}/proc_cat_names_gen{2}.txt | tail -1),OutsideAcceptance --cats=$(head -n2 {6}/m125_{1}/proc_cat_names_gen{2}.txt | tail -1) {3} --runSignalOnly=1 --inputDir={6} --queue={5} {7}'.format(yrLabel, ext, variable, replStr, cmssw_path, options.queue, inpDir, sigModOpt), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True))
            commands['Signal_{}'.format(year)] = sFitsP[-1].args
        if not options.parallel:
            sFitsP[-1].wait()

        yaml.dump(commands, open(
            '{1}/src/flashggFinalFit/commands_{0}.yaml'.format(options.extension, cmssw_path), 'w'))
        retCodes = [subp.wait() for subp in sFitsP]

    # ----------------------------Background-------------------------------
    if not options.noBkg:
        bkgFitsP = []
        for year in years:
            ext = '{0}_{1}'.format(options.extension, year)
            yrLabel = labels[year]
            refTagWV = replacementDic[year][options.extension]['tagWV'] if 'tagWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['tag']
            refProcWV = replacementDic[year][options.extension]['procWV'] if 'procWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['proc']
            replStr = '--refTagDiff={0} --refTagWV={2} --refProcWV={3} --refProcDiff={1} --refProc={1}'.format(
                replacementDic[year][options.extension]['tag'], replacementDic[year][options.extension]['proc'], refTagWV, refProcWV)
            bkgFitsP.append(subprocess.Popen(r"bash runFinalFitsScripts_differential_univ_{0}.sh --obs={2} --ext={1} --procs=$(head -n1 {5}/m125_{1}/proc_cat_names_gen{2}.txt | tail -1),OutsideAcceptance --cats=$(head -n2 {5}/m125_{1}/proc_cat_names_gen{2}.txt | tail -1) {3} --runBackgroundOnly=1 --DatacardOpt=--noSysts --bkgModOpt=--noSysts --bkgLabel={0} --sigModOpt=--noSysts --inputDir={5}".format(
                yrLabel, ext, variable, replStr, cmssw_path, inpDir), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True))
            commands['Background_{}'.format(year)] = bkgFitsP[-1].args

        retCodes = [subp.wait() for subp in bkgFitsP]
    
        yaml.dump(commands, open(
            '{1}/src/flashggFinalFit/commands_{0}.yaml'.format(options.extension, cmssw_path), 'w'))
        # if os.path.exists('{1}/src/flashggFinalFit/Background/CMS-HGG_multipdf_differential_{0}.root'.format(options.extension, cmssw_path)):
        #     os.remove('{1}/src/flashggFinalFit/Background/CMS-HGG_multipdf_differential_{0}.root'.format(options.extension, cmssw_path))
        # bkgMergeP = subprocess.Popen(r'./Signal/mergeWorkspaces.py ./Background/CMS-HGG_multipdf_differential_{0}.root ./Background/CMS-HGG_multipdf_differential_{0}_1*.root'.format(options.extension),
        #                              cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True)
        # bkgMergeP.wait()

    # -----------------------------Datacard----------------------------------
    # if not os.path.exists('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}'.format(options.extension, cmssw_path)):
    #     os.mkdir('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}'.format(options.extension, cmssw_path))
    # mergeP = subprocess.Popen(r'../Signal/bin/hadd_workspaces ../Signal/outdir_differential_{0}/reduced{1}_IAOA_161718.root ../Signal/m125_{0}_16/reduced{1}IA_gen{1}_nominal.root ../Signal/m125_{0}_17/reduced{1}IA_gen{1}_nominal.root ../Signal/m125_{0}_18/reduced{1}IA_gen{1}_nominal.root ../Signal/m125_OA_{0}_1*/reduced{1}OA__nominal.root ../Signal/m125_OA_{0}_17/reduced{1}OA__nominal.root ../Signal/m125_OA_{0}_18/reduced{1}OA__nominal.root'.format(options.extension, variable),
    #                           cwd='{0}/src/flashggFinalFit/Datacard'.format(cmssw_path), shell=True)
    # mergeP.wait()
    # commands['Datacard_mergeW'] = mergeP.args
    if not options.noDatacard:
        if not os.path.exists('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}/sigfit'.format(options.extension, cmssw_path)):
            os.mkdir('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}/sigfit'.format(options.extension, cmssw_path))
        if 'UL17' in years:
            if os.path.exists('{0}/src/flashggFinalFit/Signal/outdir_differential_{1}/sigfit/binsToSkipInDatacard_Addtl.txt'.format(cmssw_path, options.extension)):
                with open('{0}/src/flashggFinalFit/Signal/outdir_differential_{1}/sigfit/binsToSkipInDatacard_Addtl.txt'.format(cmssw_path, options.extension)) as skipFile:
                    linesSkipFileRR = list(skipFile.readlines())
                    skipFile.close()
            else:
                linesSkipFileRR = []
            if os.path.exists('{0}/src/flashggFinalFit/Signal/outdir_differential_{1}_UL17/sigfit/binsToSkipInDatacard_Addtl.txt'.format(cmssw_path, options.extension)):
                with open('{0}/src/flashggFinalFit/Signal/outdir_differential_{1}_UL17/sigfit/binsToSkipInDatacard_Addtl.txt'.format(cmssw_path, options.extension)) as skipFileUL17:
                    linesSkipFileUL17 = list(skipFileUL17.readlines())
                    skipFileUL17.close()
            else:
                linesSkipFileUL17 = []
            with open('{0}/src/flashggFinalFit/Signal/outdir_differential_{1}/sigfit/binsToSkipInDatacard_Addtl_wUL17.txt'.format(cmssw_path, options.extension), 'w') as skipFilewUL17:
                for ln in linesSkipFileRR:
                    if not ln[-4:-1] == '_17':
                        skipFilewUL17.write(ln)
                for ln in linesSkipFileUL17:
                    skipFilewUL17.write(ln)
                skipFilewUL17.close()
            skipF = subprocess.Popen(
                r'cat ./Signal/outdir_differential_{0}_16/sigfit/binsToSkipInDatacard_*.txt ./Signal/outdir_differential_{0}_UL17/sigfit/binsToSkipInDatacard_*.txt ./Signal/outdir_differential_{0}_18/sigfit/binsToSkipInDatacard_*.txt ./Signal/outdir_differential_{0}/sigfit/binsToSkipInDatacard_Addtl_wUL17.txt | tee ./Signal/outdir_differential_{0}/sigfit/binsToSkipInDatacard.txt'.format(options.extension), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True)
            skipF.wait()
            commands['Datacard_catSkipF'] = skipF.args
        elif os.path.exists('{0}/src/flashggFinalFit/Signal/outdir_differential_{1}/sigfit/binsToSkipInDatacard_Addtl.txt'.format(cmssw_path, options.extension)):
            skipF = subprocess.Popen(
                r'cat ./Signal/outdir_differential_{0}_16/sigfit/binsToSkipInDatacard_*.txt ./Signal/outdir_differential_{0}_17/sigfit/binsToSkipInDatacard_*.txt ./Signal/outdir_differential_{0}_18/sigfit/binsToSkipInDatacard_*.txt ./Signal/outdir_differential_{0}/sigfit/binsToSkipInDatacard_Addtl.txt | tee ./Signal/outdir_differential_{0}/sigfit/binsToSkipInDatacard.txt'.format(options.extension), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True)
            skipF.wait()
            commands['Datacard_catSkipF'] = skipF.args
        else:
            skipF = subprocess.Popen(
                r'cat ./Signal/outdir_differential_{0}_16/sigfit/binsToSkipInDatacard_*.txt ./Signal/outdir_differential_{0}_17/sigfit/binsToSkipInDatacard_*.txt ./Signal/outdir_differential_{0}_18/sigfit/binsToSkipInDatacard_*.txt | tee ./Signal/outdir_differential_{0}/sigfit/binsToSkipInDatacard.txt'.format(options.extension), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True)
            skipF.wait()
            commands['Datacard_catSkipF'] = skipF.args
        cats = []
        for year in years:
            with open('{0}/m125_{1}_{2}/proc_cat_names_gen{3}.txt'.format(inpDir, options.extension, year, variable)) as f:
                lines = list(f.readlines())
                cats += lines[1].split(',')

        if os.path.exists('{0}/src/flashggFinalFit/Signal/outdir_differential_{1}/catsToSkip.txt'.format(cmssw_path, options.extension)):
            with open('{0}/src/flashggFinalFit/Signal/outdir_differential_{1}/catsToSkip.txt'.format(cmssw_path, options.extension)) as f:
                catsToSkip = list(f.readlines())
            for cat in catsToSkip:
                cat = cat.replace('\n', '')
                cats.remove(cat)

        catStr = ''
        for cat in cats:
            catStr += '{},'.format(cat)
        catStr = catStr[:-1]
        if 'UL17' in years:
            dCardP = subprocess.Popen(r'./makeParametricModelDatacardFLASHgg.py -i {3}/m125_{0}_16/reduced{1}IA_gen{1}_nominal*.root {3}/m125_OA_{0}_16/reduced{1}OA__nominal*.root {3}/m125_{0}_UL17/reduced{1}IA_gen{1}_nominal*.root {3}/m125_OA_{0}_UL17/reduced{1}OA__nominal*.root {3}/m125_{0}_18/reduced{1}IA_gen{1}_nominal*.root {3}/m125_OA_{0}_18/reduced{1}OA__nominal*.root -o Datacard_13TeV_differential_{0}_wUL17.txt --ext differential_{0} -p $(head -n1 {3}/m125_{0}_16/proc_cat_names_gen{1}.txt | tail -1),OutsideAcceptance -c {2} --photonCatScales HighR9EB,HighR9EE,LowR9EB,LowR9EE,Gain6EB,Gain1EB --photonCatSmears HighR9EBPhi,HighR9EBRho,HighR9EEPhi,HighR9EERho,LowR9EBPhi,LowR9EBRho,LowR9EEPhi,LowR9EERho --mass 125 --intLumi16 35.9 --intLumi17 41.5 --intLumi18 59.35 --intLumi 35.9 --differential --isMultiPdf --fullRunII --rateFiles {4}/src/HiggsAnalysis/CombinedLimit/test/preApp21/theoryPred_{0}_16.pkl {4}/src/HiggsAnalysis/CombinedLimit/test/wUL17/theoryPred_{0}_UL17.pkl {4}/src/HiggsAnalysis/CombinedLimit/test/preApp21/theoryPred_{0}_18.pkl'.format(options.extension, variable, catStr, inpDir, cmssw_path), cwd='{0}/src/flashggFinalFit/Datacard'.format(cmssw_path), shell=True)
        else:
            dCardP = subprocess.Popen(r'./makeParametricModelDatacardFLASHgg.py -i {3}/m125_{0}_1*/reduced{1}IA_gen{1}_nominal*.root {3}/m125_OA_{0}_1*/reduced{1}OA__nominal*.root -o Datacard_13TeV_differential_{0}.txt --ext differential_{0} -p $(head -n1 {3}/m125_{0}_16/proc_cat_names_gen{1}.txt | tail -1),OutsideAcceptance -c {2} --photonCatScales HighR9EB,HighR9EE,LowR9EB,LowR9EE,Gain6EB,Gain1EB --photonCatSmears HighR9EBPhi,HighR9EBRho,HighR9EEPhi,HighR9EERho,LowR9EBPhi,LowR9EBRho,LowR9EEPhi,LowR9EERho --mass 125 --intLumi16 35.9 --intLumi17 41.5 --intLumi18 59.35 --intLumi 35.9 --differential --isMultiPdf --fullRunII --rateFiles {4}/src/HiggsAnalysis/CombinedLimit/test/preApp21/theoryPred_{0}_16.pkl {4}/src/HiggsAnalysis/CombinedLimit/test/preApp21/theoryPred_{0}_17.pkl {4}/src/HiggsAnalysis/CombinedLimit/test/preApp21/theoryPred_{0}_18.pkl'.format(options.extension, variable, catStr, inpDir, cmssw_path), cwd='{0}/src/flashggFinalFit/Datacard'.format(cmssw_path), shell=True)
        commands['Datacard'] = dCardP.args
        dCardP.wait()

    yaml.dump(commands, open(
        '{1}/src/flashggFinalFit/commands_{0}.yaml'.format(options.extension, cmssw_path), 'w'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    requiredAgrs = parser.add_argument_group()
    requiredAgrs.add_argument(
        '--extension', '-e', action='store', type=str, required=True)
    # requiredAgrs.add_argument(
    #     '--variable', '-v', action='store', type=str, required=True)
    optionalArgs = parser.add_argument_group()
    optionalArgs.add_argument(
        '--parallel', '-p', action='store_true', default=False)
    optionalArgs.add_argument(
        '--ftest', '-t', action='store_true', default=False)
    optionalArgs.add_argument(
        '--noSplit', '-n', action='store_true', default=False)
    optionalArgs.add_argument(
        '--noSig', action='store_true', default=False)
    optionalArgs.add_argument(
        '--noBkg', action='store_true', default=False)
    optionalArgs.add_argument(
        '--noDatacard', action='store_true', default=False)
    optionalArgs.add_argument(
        '--queue', '-q', action='store', type=str, default='longlunch')
    optionalArgs.add_argument(
        '--fTestQueue', action='store', type=str, default='espresso')
    optionalArgs.add_argument(
        '--year', '-y', action='store', type=str)
    optionalArgs.add_argument(
        '--skipCalcPhoSyst', action='store_true', default=False)
    optionalArgs.add_argument(
        '--wUL17', action='store_true', default=False)
    options = parser.parse_args()
    main(options)
