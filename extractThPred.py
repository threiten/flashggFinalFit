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

def main(options):

    config = yaml.load(open(options.config))

    tags = ['SigmaMpTTag_0', 'NoTag_0']
    masss = ['120', '125', '130']
    
    splitDic = OrderedDict({})
    for key in config['splits'].keys():
        if 'gen' in key:
            splitDic[key] = config['splits'][key]

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
        'puweight': ['-inf', 'inf'],
        'genWeight': ['-inf', 'inf']
    }
    variables = parse_variables(varDic)

    varList = [var[0] for var in variables]
    addRep = list(config['functions'].keys()) if ('functions' in config.keys()) else []
    
    if genPSCut is not None:
        varList += parseVariablesFromFormula(genPSCut, addRep)
    if 'formulas' in config.keys():
        varList += parseVariablesFromFormulas(config['formulas'], addRep)

    splitCols = list(splitDic.keys())
    columns = splitCols + varList
    
    if 'formulas' in config.keys():
        config['formulas']['genWeight'] = 'weight/puweight'
    else:
        config['formulas'] = {}
        config['formulas']['genWeight'] = 'weight/puweight'
        
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
        for mass in masss:
            dfs[tag][mass] = loadRFiles(options.inpath, 'genDiphotonDumper/trees/InsideAcceptance_{}_13TeV_{}'.format(mass, tag), columns, fmatch='output_*_M{}*.root'.format(mass))
            dfs[tag][mass]['genWeight'] = dfs[tag][mass].eval('weight/puweight')

    gbs = {}
    for tag in tags:
        gbs[tag] = {}
        for mass in masss:
            if 'formulas' in config.keys():
                evaluate_formulas(dfs[tag][mass], config['formulas'])
            if genPSCut is not None:
                dfs[tag][mass].query(genPSCut, engine='python', inplace=True)

            t2w = t2d.RooWorkspaceFromDataframe(dfs[tag][mass], splitDic, variables, 'weight', "cms_hgg_13TeV")
            gbs[tag][mass] = copy.deepcopy(t2w.gb)

    var = list(splitDic.keys())[0]
    labels = t2w.makeBinLabels(var, splitDic[var])

    bins = np.array(splitDic[var])
    if bins[-1] > 5 * bins[-2]:
        bins[-1] = 2 * bins[-2] - bins[-3] if len(bins)>2 else bins[-1]
    if var.replace('gen','') in lastBins.keys():
        bins[-1] = lastBins[var.replace('gen','')]
    binw = bins[1:] - bins[:-1]
    print(bins, binw)
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
    options = parser.parse_args()
    main(options)    
