import argparse
import os
import subprocess
import shutil as shl
import oyaml as yaml

cmssw_path = os.environ['CMSSW_BASE']

rootDir = {
    '16': {
        'IA': '/eos/user/t/threiten/Analysis/Differentials/2016ReReco/dev_differential_wJMET_reRunSum20_signal_IA_16/',
        'OA': '/eos/user/t/threiten/Analysis/Differentials/2016ReReco/dev_differential_wJMET_reRunSum20_signal_OA_16/',
        'Data': '/eos/user/t/threiten/Analysis/Differentials/2016ReReco/dev_differential_wJMET_reRunSum20_data_16/'
    },
    '17': {
        'IA': '/eos/user/t/threiten/Analysis/Differentials/2017ReReco/dev_differential_wJMET_reRunSum20_signal_IA_17/',
        'OA': '/eos/user/t/threiten/Analysis/Differentials/2017ReReco/dev_differential_wJMET_reRunSum20_signal_OA_17/',
        'Data': '/eos/user/t/threiten/Analysis/Differentials/2017ReReco/dev_differential_wJMET_reRunSum20_data_17/'
    },
    '18': {
        'IA': '/eos/user/t/threiten/Analysis/Differentials/2018ReABCPromptDReco/dev_differential_wJMET_reRunSum20_signal_IA_18/',
        'OA': '/eos/user/t/threiten/Analysis/Differentials/2018ReABCPromptDReco/dev_differential_wJMET_reRunSum20_signal_OA_18/',
        'Data': '/eos/user/t/threiten/Analysis/Differentials/2018ReABCPromptDReco/dev_differential_wJMET_reRunSum20_data_18/'
    }}

replacementDic = {
    '16': yaml.load(open('{0}/src/flashggFinalFit/replacementSignalModel_2016.yaml'.format(cmssw_path))),
    '17': yaml.load(open('{0}/src/flashggFinalFit/replacementSignalModel_2017.yaml'.format(cmssw_path))),
    '18': yaml.load(open('{0}/src/flashggFinalFit/replacementSignalModel_2018.yaml'.format(cmssw_path)))
}

varDic = {
    'Pt': 'Pt',
    'Njets2p5': 'Njets2p5',
    'CosThetaS': 'CosThetaS',
    'AbsRapidity': 'AbsRapidity',
    'AbsRapidityFine': 'AbsRapidity',
    'Jet2p5Pt0': 'Jet2p5Pt0',
    'Jet2p5AbsRapidity0': 'Jet2p5AbsRapidity0',
    'AbsDeltaRapidityGgJet2p50': 'AbsDeltaRapidityGgJet2p50',
    'AbsDeltaPhiGgJet2p50': 'AbsDeltaPhiGgJet2p50',
    'Jet4p7Pt1': 'Jet4p7Pt1',
    'Jet4p7AbsRapidity1': 'Jet4p7AbsRapidity1',
    'AbsDeltaPhiGgJjJets4p7': 'AbsDeltaPhiGgJjJets4p7',
    'AbsDeltaPhiJ1J2Jets4p7': 'AbsDeltaPhiJ1J2Jets4p7',
    'AbsZeppenfeldEtaJets4p7': 'AbsZeppenfeldEtaJets4p7',
    'MjjJets4p7': 'MjjJets4p7',
    'AbsDeltaEtaJ1J2Jets4p7': 'AbsDeltaEtaJ1J2Jets4p7',
    'Nleptons': 'Nleptons',
    'NBjets2p5': 'NBjets2p5',
    'PtMiss': 'PtMiss',
    'Jet4p7Pt1VBFlike': 'Jet4p7Pt1',
    'AbsDeltaPhiGgJjJets4p7VBFlike': 'AbsDeltaPhiGgJjJets4p7',
    'AbsDeltaPhiJ1J2Jets4p7VBFlike': 'AbsDeltaPhiJ1J2Jets4p7',
    'PtVBFlike': 'Pt',
    'Pt0Jets': 'Pt',
    'Pt1Jets': 'Pt',
    'Pt1PJets': 'Pt',
    'TauCJet2p50': 'TauCJet2p50',
    'AbsPhiS': 'AbsPhiS',
    'PtTauC0': 'Pt',
    'PtTauC1': 'Pt',
    'PtTauC2': 'Pt',
    'PtTauC3': 'Pt',
    'PtInclusive': 'Pt',
    'PtInclusive1L1B': 'Pt',
    'PtInclusive1LHPtM': 'Pt',
    'PtInclusive1LLPtM': 'Pt'
}


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
    if not options.noSplit:
        subPs = []
        for year in ['16', '17', '18']:
            subPs.append(subprocess.Popen('python {4}/src/flashggFinalFit/splitTree.py -c {4}/src/flashggFinalFit/splitConfig_{1}_20{0}.yaml -i {2}/allData.root -o {4}/src/flashggFinalFit/Signal/data_{1}_{0} -l {0} -p Data'.format(
                year, options.extension, rootDir[year]['Data'], variable, cmssw_path), shell=True))
            commands['split_{}_Data'.format(year)] = subPs[-1].args
            subPs[-1].wait()
            # for proc in ['IA', 'OA']:
            retCodesL = []
            for mass in ['120', '125', '130']:
                subPs.append(subprocess.Popen('python {6}/src/flashggFinalFit/splitTree.py -c {6}/src/flashggFinalFit/splitConfig_{1}_20{0}.yaml -i {2}/allSig_{3}{4}.root -o {6}/src/flashggFinalFit/Signal/m{3}{4}_{1}_{0} -l {0} --infileOA {8}/allSig_{3}{7}.root --outfolderOA {6}/src/flashggFinalFit/Signal/m{3}{7}_{1}_{0} -p SIG_{3}'.format(
                    year, options.extension, rootDir[year]['IA'], mass, getIAOA('IA'), variable, cmssw_path, getIAOA('OA'), rootDir[year]['OA']), shell=True))
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
        for year in ['16', '17', '18']:
            ext = '{0}_{1}'.format(options.extension, year)
            refTagWV = replacementDic[year][options.extension]['tagWV'] if 'tagWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['tag']
            refProcWV = replacementDic[year][options.extension]['procWV'] if 'procWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['proc']
            replStr = '--refTagDiff={0} --refTagWV={2} --refProcWV={3} --refProcDiff={1} --refProc={1}'.format(
                replacementDic[year][options.extension]['tag'], replacementDic[year][options.extension]['proc'], refTagWV, refProcWV)
            sFitsP.append(subprocess.Popen(r'bash runFinalFitsScripts_differential_univ_{0}.sh --obs={2} --ext={1} --procs=$(head -n1 Signal/m125_{1}/proc_cat_names_gen{2}.txt | tail -1),OutsideAcceptance --cats=$(head -n2 Signal/m125_{1}/proc_cat_names_gen{2}.txt | tail -1) {3} --runSignalOnly=1 --DatacardOpt=--noSysts --bkgModOpt=--noSysts --sigModOpt=--noSysts --inputDir={4}/src/flashggFinalFit/Signal'.format(
                year, ext, variable, replStr, cmssw_path, options.queue), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True))
            commands['fTest_{}'.format(year)] = sFitsP[-1].args
            if not options.parallel:
                sFitsP[-1].wait()

        yaml.dump(commands, open(
            '{1}/src/flashggFinalFit/commands_{0}.yaml'.format(options.extension, cmssw_path), 'w'))
        retCodes = [pc.wait() for pc in sFitsP]

        for year in ['16', '17', '18']:
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
        for year in ['16', '17', '18']:
            ext = '{0}_{1}'.format(options.extension, year)
            refTagWV = replacementDic[year][options.extension]['tagWV'] if 'tagWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['tag']
            refProcWV = replacementDic[year][options.extension]['procWV'] if 'procWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['proc']
            replStr = '--refTagDiff={0} --refTagWV={2} --refProcWV={3} --refProcDiff={1} --refProc={1}'.format(
                replacementDic[year][options.extension]['tag'], replacementDic[year][options.extension]['proc'], refTagWV, refProcWV)
            sFitsP.append(subprocess.Popen(r'bash runFinalFitsScripts_differential_univ_{0}.sh --obs={2} --ext={1} --procs=$(head -n1 Signal/m125_{1}/proc_cat_names_gen{2}.txt | tail -1),OutsideAcceptance --cats=$(head -n2 Signal/m125_{1}/proc_cat_names_gen{2}.txt | tail -1) {3} --runSignalOnly=1 --DatacardOpt=--noSysts --bkgModOpt=--noSysts --sigModOpt=--noSysts --inputDir={4}/src/flashggFinalFit/Signal --queue={5}'.format(year, ext, variable, replStr, cmssw_path, options.queue), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True))
            commands['Signal_{}'.format(year)] = sFitsP[-1].args
        if not options.parallel:
            sFitsP[-1].wait()

        yaml.dump(commands, open(
            '{1}/src/flashggFinalFit/commands_{0}.yaml'.format(options.extension, cmssw_path), 'w'))
        retCodes = [subp.wait() for subp in sFitsP]

    # ----------------------------Background-------------------------------
    if not options.noBkg:
        bkgFitsP = []
        for year in ['16', '17', '18']:
            ext = '{0}_{1}'.format(options.extension, year)
            refTagWV = replacementDic[year][options.extension]['tagWV'] if 'tagWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['tag']
            refProcWV = replacementDic[year][options.extension]['procWV'] if 'procWV' in replacementDic[year][options.extension].keys() else replacementDic[year][options.extension]['proc']
            replStr = '--refTagDiff={0} --refTagWV={2} --refProcWV={3} --refProcDiff={1} --refProc={1}'.format(
                replacementDic[year][options.extension]['tag'], replacementDic[year][options.extension]['proc'], refTagWV, refProcWV)
            bkgFitsP.append(subprocess.Popen(r'bash runFinalFitsScripts_differential_univ_{0}.sh --obs={2} --ext={1} --procs=$(head -n1 Signal/m125_{1}/proc_cat_names_gen{2}.txt | tail -1),OutsideAcceptance --cats=$(head -n2 Signal/m125_{1}/proc_cat_names_gen{2}.txt | tail -1) {3} --runBackgroundOnly=1 --DatacardOpt=--noSysts --bkgModOpt=--noSysts --sigModOpt=--noSysts --inputDir={4}/src/flashggFinalFit/Signal'.format(
            year, ext, variable, replStr, cmssw_path), cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True))
            commands['Background_{}'.format(year)] = bkgFitsP[-1].args

        retCodes = [subp.wait() for subp in bkgFitsP]
    
        yaml.dump(commands, open(
            '{1}/src/flashggFinalFit/commands_{0}.yaml'.format(options.extension, cmssw_path), 'w'))
        if os.path.exists('{1}/src/flashggFinalFit/Background/CMS-HGG_multipdf_differential_{0}.root'.format(options.extension, cmssw_path)):
            os.remove('{1}/src/flashggFinalFit/Background/CMS-HGG_multipdf_differential_{0}.root'.format(options.extension, cmssw_path))
        bkgMergeP = subprocess.Popen(r'./Signal/mergeWorkspaces.py ./Background/CMS-HGG_multipdf_differential_{0}.root ./Background/CMS-HGG_multipdf_differential_{0}_1*.root'.format(options.extension),
                                     cwd='{0}/src/flashggFinalFit'.format(cmssw_path), shell=True)
        bkgMergeP.wait()

    # -----------------------------Datacard----------------------------------
    if not os.path.exists('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}'.format(options.extension, cmssw_path)):
        os.mkdir('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}'.format(options.extension, cmssw_path))
    mergeP = subprocess.Popen(r'../Signal/bin/hadd_workspaces ../Signal/outdir_differential_{0}/reduced{1}_IAOA_161718.root ../Signal/m125_{0}_16/reduced{1}IA_gen{1}_nominal.root ../Signal/m125_{0}_17/reduced{1}IA_gen{1}_nominal.root ../Signal/m125_{0}_18/reduced{1}IA_gen{1}_nominal.root ../Signal/m125_OA_{0}_1*/reduced{1}OA__nominal.root ../Signal/m125_OA_{0}_17/reduced{1}OA__nominal.root ../Signal/m125_OA_{0}_18/reduced{1}OA__nominal.root'.format(options.extension, variable),
                              cwd='{0}/src/flashggFinalFit/Datacard'.format(cmssw_path), shell=True)
    mergeP.wait()
    if not os.path.exists('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}/sigfit'.format(options.extension, cmssw_path)):
        os.mkdir('{1}/src/flashggFinalFit/Signal/outdir_differential_{0}/sigfit'.format(options.extension, cmssw_path))
    commands['Datacard_mergeW'] = mergeP.args
    if os.path.exists('{0}/src/flashggFinalFit/Signal/outdir_differential_{1}/sigfit/binsToSkipInDatacard_Addtl.txt'.format(cmssw_path, options.extension)):
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
    for year in ['16', '17', '18']:
        with open('{0}/src/flashggFinalFit/Signal/m125_{1}_{2}/proc_cat_names_gen{3}.txt'.format(cmssw_path, options.extension, year, variable)) as f:
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
    dCardP = subprocess.Popen(r'./makeParametricModelDatacardFLASHgg.py -i ../Signal/outdir_differential_{0}/reduced{1}_IAOA_161718.root -o Datacard_13TeV_differential_{0}.txt --ext differential_{0} -p $(head -n1 ../Signal/m125_{0}_16/proc_cat_names_gen{1}.txt | tail -1),OutsideAcceptance -c {2} --photonCatScales HighR9EB,HighR9EE,LowR9EB,LowR9EE,Gain6EB,Gain1EB --photonCatSmears HighR9EBPhi,HighR9EBRho,HighR9EEPhi,HighR9EERho,LowR9EBPhi,LowR9EBRho,LowR9EEPhi,LowR9EERho --mass 125 --intLumi16 35.9 --intLumi17 41.5 --intLumi18 59.35 --intLumi 35.9 --differential --statonly --isMultiPdf'.format(
        options.extension, variable, catStr), cwd='{0}/src/flashggFinalFit/Datacard'.format(cmssw_path), shell=True)
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
        '--queue', '-q', action='store', type=str, default='longlunch')
    options = parser.parse_args()
    main(options)
