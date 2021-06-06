import tree2dataset as t2d
import ROOT
import uproot
import root_pandas
from collections import OrderedDict
import argparse
import os
import copy
import random
import fnmatch
import oyaml as yaml
import numpy as np
import pandas as pd
import scipy.interpolate as itr
import pickle as pkl
from splitTree import parseVariablesFromFormula,parseVariablesFromFormulas,parse_variables

lastBins = {
    'AbsRapidity': 2.5,
    'Jet2p5AbsRapidity0': 2.5,
    'AbsDeltaPhiGgJet2p50': np.pi,
    'Jet4p7AbsRapidity1': 4.7,
    'AbsDeltaPhiJ1J2Jets4p7': np.pi,
    'AbsDeltaPhiGgJjJets4p7': np.pi
}

def evaluate_formulas(df, form_dic):
    for form in form_dic.keys():
        df[form] = df.eval(form_dic[form], engine='python')


def loadRFiles(path, treepath, branches, cutoff=None, shuffle=False, fmatch=None):
    if os.path.isdir(path):
        flist = [f for f in os.listdir(path) if (os.path.isfile(os.path.join(path, f)) and '.root' in f)]
        if fmatch is not None:
            flist = fnmatch.filter(flist, fmatch)
        if shuffle:
            random.shuffle(flist)
        path = [os.path.join(path, f) for f in flist]
    it = uproot.pandas.iterate(path, treepath=treepath, branches=branches)
    ret = pd.DataFrame(columns=branches)
    for df in it:
        ret = ret.append(df, ignore_index=True)
        print('{:,}'.format(ret.index.size), end='\r')
        if cutoff is not None:
            if ret.index.size>=cutoff:
                break

    return ret

def getBins(bins, var, lastBins):
    if bins[-1] > 5 * bins[-2]:
        bins[-1] = 2 * bins[-2] - bins[-3] if len(bins)>2 else bins[-1]
    if var.replace('gen','') in lastBins.keys():
        bins[-1] = lastBins[var.replace('gen','')]
    binw = bins[1:] - bins[:-1]

    return bins, binw

def getNNLOPSweight(ptnjets, nnlopsweights):
    pt = ptnjets[0]
    njets = ptnjets[1]
    if njets==0:
        return nnlopsweights[0].Eval(min(pt, 125.))
    elif njets==1:
        return nnlopsweights[1].Eval(min(pt, 625.))
    elif njets==2:
        return nnlopsweights[2].Eval(min(pt, 800.))
    elif njets>=3:
        return nnlopsweights[3].Eval(min(pt, 925.))

def main(options):

    config = yaml.load(open(options.config))

    tags = ['NoTag_0', 'SigmaMpTTag_0']
    dumpers = {
        'SigmaMpTTag_0' : 'tagsDumper',
        'NoTag_0' : 'genDiphotonDumper'
    }
    masss = ['120', '125', '130']
    if options.proc is None:
        procs = ['GluGluHToGG', 'VBFHToGG', 'VHToGG', 'ttHJetToGG']
    else:
        procs = options.proc
        
    NNLOPSfile = ROOT.TFile("/eos/cms/store/group/phys_higgs/cmshgg/flashgg-data/Taggers/data/NNLOPS_reweight.root")
    nnlopsweights = []
    for i in range(4):
        nnlopsweights.append(NNLOPSfile.Get("gr_NNLOPSratio_pt_mcatnlo_{}jet".format(i)))
    NNLOPSfile.Close()
    
    splitDic = OrderedDict({})
    for key in config['splits'].keys():
        if 'gen' in key:
            if isinstance(config['splits'][key], list):
                splitDic[key] = config['splits'][key]
            elif isinstance(config['splits'][key], dict):
                splitDic[key] = OrderedDict(config['splits'][key])

    delKeys = []
    if 'formulas' in config.keys():
        for key in config['formulas'].keys():
            if 'reco' in key:
                delKeys.append(key)
    for key in delKeys:
        del config['formulas'][key]
        
    genPSCut = None
    if 'phasespace' in config.keys():
        genPSCut = config['phasespace']['gen']

    varDic = {
        'weight': ['-inf', 'inf'],
        'puweight': ['-inf', 'inf']
        # 'genWeight': ['-inf', 'inf'],
    }
    variables = parse_variables(varDic)

    varList = [var[0] for var in variables]
    addRep = list(config['functions'].keys()) if ('functions' in config.keys()) else []
    
    if genPSCut is not None:
        varList += parseVariablesFromFormula(genPSCut, addRep)
    if 'formulas' in config.keys():
        varList += parseVariablesFromFormulas(config['formulas'], addRep)

    # splitCols = list(splitDic.keys())
    print(splitDic)
    if not isinstance(splitDic[list(splitDic.keys())[0]], dict):
        splitCols = list(splitDic.keys())
    else:
        splitCols = []
        for key, item in splitDic.items():
            splitCols += list(item.keys())
    columns = splitCols + varList
    print(columns)
    
    # if 'formulas' in config.keys():
    #     config['formulas']['genWeight'] = 'weight/puweight'
    # else:
    #     config['formulas'] = {}
    #     config['formulas']['genWeight'] = 'weight/puweight'
        
    for key in config['formulas'].keys():
        while key in columns:
            columns.remove(key)
    # for wName in ['weight', 'puweight']:
    #     if wName not in columns:
    #         columns += [wName]

    if 'functions' in config.keys():
        for key in config['functions']:
            exec(config['functions'][key], globals())

    # infiles = {}
    # for i, infile in enumerate(options.infiles):
    #     infiles[masss[i]] = infile

    dfs = {}
    
    columns = list(set(columns))
    for tag in tags:
        dfs[tag] = {}
        dpr = dumpers[tag]
        if 'NoTag' in tag:
            addCols = ['genPt', 'genNjets4p7']
        else:
            addCols = ['NNLOPSweight', 'centralObjectWeight']
        for mass in masss:
            dfs[tag][mass] = pd.DataFrame(columns=columns)
            for proc in procs:
                dfLoad = loadRFiles(options.inpath, '{}/trees/InsideAcceptance_{}_13TeV_{}'.format(dpr, mass, tag), list(set(columns+addCols)), fmatch='output_{}_M{}*.root'.format(proc, mass))
                print('Proc: ', proc)
                if 'NoTag' in tag:
                    dfLoad['genWeight'] = dfLoad.eval('weight/puweight')
                    if 'GluGlu' in proc and options.applyNNLOPSweights:
                        print('Applying NNLOPSweight')
                        dfLoad['NNLOPSweight'] = np.apply_along_axis(getNNLOPSweight, 1, dfLoad.loc[:,['genPt', 'genNjets4p7']].values, nnlopsweights=nnlopsweights)
                        dfLoad['genWeight'] = dfLoad.eval('genWeight*NNLOPSweight')
                else:
                    dfLoad['genWeight'] = dfLoad.eval('weight/(puweight*centralObjectWeight)')
                    if 'GluGlu' in proc and options.applyNNLOPSweights:
                        print('Applying NNLOPSweight')
                        dfLoad['genWeight'] = dfLoad.eval('genWeight*(NNLOPSweight/centralObjectWeight)')
                dfs[tag][mass] = dfs[tag][mass].append(dfLoad.loc[:, columns], ignore_index=True)

    gbs = {}
    for tag in tags:
        gbs[tag] = {}
        for mass in masss:
            print(dfs[tag][mass])
            if 'formulas' in config.keys():
                evaluate_formulas(dfs[tag][mass], config['formulas'])
            if genPSCut is not None:
                for key in splitDic.keys():
                    if 'gen' in key:
                        if isinstance(splitDic[key], OrderedDict):
                            for keyIn in splitDic[key].keys():
                                if not isinstance(splitDic[key][keyIn][0], list):
                                    genLabel = str(keyIn)
                        else:
                            genLabel = str(key)
                        # genLabel = str(key)
                dfs[tag][mass].loc[dfs[tag][mass].eval('not ({})'.format(genPSCut), engine='python'), genLabel] = -999.
                # dfs[tag][mass].query(genPSCut, engine='python', inplace=True)

            t2w = t2d.RooWorkspaceFromDataframe(dfs[tag][mass], splitDic, variables, 'weight', "cms_hgg_13TeV")
            gbs[tag][mass] = copy.deepcopy(t2w.gb)

    if isinstance(splitDic[list(splitDic.keys())[0]], OrderedDict):
        t2w.makeCategories()
        labels = t2w.categories
        var = list(splitDic['gen'].keys())[1]
        bins = splitDic['gen'][var]
        binsTmp = []
        binwTmp = []
        for binss in bins:
            tpl = getBins(np.array(binss), var, lastBins)
            binsTmp.append(tpl[0])
            binwTmp.append(tpl[1])
        bins = np.concatenate(binsTmp)
        binw = np.concatenate(binwTmp)
    else:
        var = list(splitDic.keys())[0]
        labels = t2w.makeBinLabels(var, splitDic[var])
        bins = np.array(splitDic[var])
        bins, binw = getBins(bins, var, lastBins)

    
    print(bins, binw, labels)
    xss = np.zeros((len(binw),3))
    errs = np.zeros((len(binw),3))
    for i in range(len(binw)):
        for j, mass in enumerate(masss):
            xss[i, j] = (gbs[tags[0]][mass].get_group(labels[i])['genWeight'].sum() + gbs[tags[1]][mass].get_group(labels[i])['genWeight'].sum())/binw[i]
            errs[i,j] = np.sqrt(gbs[tags[0]][mass].get_group(labels[i]).eval('genWeight**2').sum() + gbs[tags[1]][mass].get_group(labels[i]).eval('genWeight**2').sum())/binw[i]
    
    if options.ofile is not None:
        outf = options.ofile.replace('.{}'.format(options.ofile.split('.')[-1]),'')
        if options.ofile.split('.')[-1] in ['txt', 'all'] :
            with open('{}.txt'.format(outf), 'w') as f:
                f.write('{}'.format(xss))
            if options.dumpErrors:
                with open('{}_errs.txt'.format(outf), 'w') as f:
                    f.write('{}'.format(errs))
        if options.ofile.split('.')[-1] in ['pkl', 'all']:
            with open('{}.pkl'.format(outf), 'wb') as f:
                pkl.dump(xss, f, protocol=2)
            if options.dumpErrors:
                with open('{}_errs.pkl'.format(outf), 'wb') as f:
                    pkl.dump(errs, f, protocol=2)
        if options.ofile.split('.')[-1] not in ['txt', 'pkl', 'all']:
            print(xss)
            raise NotImplementedError('Cannot save file with this extension')
    else:
        print(xss)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    requiredArgs = parser.add_argument_group()
    requiredArgs.add_argument(
        '--inpath', '-i', action='store', type=str, required=True) #, nargs='+'
    requiredArgs.add_argument(
        '--config', '-c', action='store', type=str, required=True)
    optionalArgs = parser.add_argument_group()
    optionalArgs.add_argument(
        '--ofile', '-o', action='store', type=str)
    optionalArgs.add_argument(
        '--dumpErrors', '-e', action='store_true', default=False)
    optionalArgs.add_argument(
        '--proc', '-p', nargs='+', action='store', type=str)
    optionalArgs.add_argument(
        '--applyNNLOPSweights', action='store_true', default=False)
    options = parser.parse_args()
    main(options)    
