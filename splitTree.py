import tree2dataset as t2d
import ROOT
# import root_pandas
import uproot
from collections import OrderedDict
import argparse
import os
import copy
import oyaml as yaml
import numpy as np
import pandas as pd
from dask.distributed import Client, LocalCluster, progress
from dask_jobqueue import HTCondorCluster
from joblib import delayed, Parallel

def parse_variables(variables):
    return [(key, float(variables[key][0]), float(variables[key][1])) for key in variables.keys()]


def write_cat_procs_file(path, procs, cats):
    proc_str = ''
    for proc in procs:
        proc_str += '{},'.format(proc)
    proc_str = proc_str[:-1] + '\n'
    cat_str = ''
    for cat in cats:
        cat_str += '{},'.format(cat)
    cat_str = cat_str[:-1]
    with open(path, 'w') as fil:
        fil.write(proc_str)
        fil.write(cat_str)


# def evaluate_formulas(df, form_dic):
#     for form in form_dic.keys():
#         df[form] = df.eval(form_dic[form], engine='python')


def parseVariablesFromFormulas(formulas, addRep):
    ret = []
    for key in formulas.keys():
        ret += parseVariablesFromFormula(formulas[key], addRep)

    return ret


def parseVariablesFromFormula(formula, addRep=[]):
    reps = ['(', ')', '@', ',', '+', '-', '*', '/', ' and ', ' or ', ' not ', '=', '<', '>', 'cosh', 'sinh', 'sqrt', 'cos', 'sin', 'abs']
    reps += addRep

    for rep in reps:
        while rep in formula:
            formula = formula.replace(rep, ' ')

    ret = []
    for part in formula.split():
        try:
            float(part)
        except ValueError:
            ret.append(part)

    return ret

def findNonContVars(lst, addlist):

    ret = []
    for tvar in addlist:
        if tvar not in lst:
            ret.append(tvar)
            
    return ret

# def labelSystVars(df, systVars, label):
#     for var in systVars:
#         for dirr in ['Up01sigma', 'Down01sigma']:
#             df['{}_{}{}'.format(var, label, dirr)] = df['{}{}'.format(var, dirr)]

#     return df


def unpackTheoryUnc(df, theoryUncName, n):
    for i in range(n):
        df['{}_{}'.format(theoryUncName, n)] = df[theoryUncName][i]

    return df


# def getProcCatOut(proc, cat):
#     with open(os.path.join(os.path.dirname(__file__), 'procCatNameReplacments.yaml')) as repF:
#         procCatDic = yaml.load(repF)

#     print(procCatDic)
#     for key, item in procCatDic['proc']['replace'].items():
#         proc = proc.replace(key, item)

#     for key, item in procCatDic['cat']['replace'].items():
#         cat = cat.replace(key, item)

#     if 'prefix' in procCatDic['proc'].keys():
#         proc = procCatDic['proc']['prefix'] + proc

#     if 'prefix' in procCatDic['cat'].keys():
#         cat = procCatDic['cat']['prefix'] + cat

#     replacements = procCatDic['proc']['replace']
#     replacements.update(procCatDic['cat']['replace'])
    
#     return proc, cat, replacements

# def getFilename(outfolder, outfile, catLabel=None, label=None):

#     outfilenRT = outfile.split('.')[0]

#     ret = '{}/{}'.format(outfolder, outfilenRT)
    
#     if catLabel is not None:
#         ret += '_{}'.format(catLabel)

#     if label is not None:
#         ret += '_{}'.format(label)

#     ret += '.root'
    
#     return ret


def getCluster():

    cluster = HTCondorCluster(cores=1, memory='6GB', disk='1GB', scheduler_options={'dashboard_address': '8010'})
    cluster.scale(jobs=10)
    # cluster = LocalCluster()
    return cluster

# def parseVariablesFromCut(cut):
#     reps = ['(', ')', 'and', 'or', 'not', '=', '+', '-', '<', '>', 'cosh', 'abs', 'sqrt']

#     for rep in reps:
#         cut = cut.replace(rep, ' ')

#     ret = []
#     for part in cut.split():
#         try:
#             float(part)
#         except ValueError:
#             ret.append(part)

#     return ret


def runOneCat(catR, config, options, variables, systematicVariables, nomVariables, splitDic, varList, nomVarList, systVarList, systVars, label, process, outfolder, outfile, replace, weight, cut, genPSCut=None, outfolderOA=None, outfileOA=None, processOA=None, splitDicOA=None, extCat=None):

    if options.cluster is not None:
        import ROOT as rt
        import uproot as ur
        import copy as cp
        import tree2dataset as t2din
        import pandas as pdin
        # import numpy as npin
    else:
        rt = ROOT
        ur = uproot
        cp = copy
        t2din = t2d
        pdin = pd
        # npin = np
        
    def labelSystVarsIn(df, systVars, label):
        for var in systVars:
            for dirr in ['Up01sigma', 'Down01sigma']:
                df['{}_{}{}'.format(var, label, dirr)] = df['{}{}'.format(var, dirr)]

        return df

    def getFilenameIn(outfolder, outfile, catLabel=None, label=None):

        outfilenRT = outfile.split('.')[0]

        ret = '{}/{}'.format(outfolder, outfilenRT)
            
        if catLabel is not None:
            ret += '_{}'.format(catLabel)

        if label is not None:
            ret += '_{}'.format(label)

        ret += '.root'
        
        return ret

    def getProcCatOutIn(proc, cat):
        with open('/afs/cern.ch/work/t/threiten/Hgg/Differentials/CMSSW_10_2_13/src/flashggFinalFit/procCatNameReplacments.yaml') as repF:
            procCatDic = yaml.load(repF)

        print(procCatDic)
        for key, item in procCatDic['proc']['replace'].items():
            proc = proc.replace(key, item)

        for key, item in procCatDic['cat']['replace'].items():
            cat = cat.replace(key, item)

        if 'prefix' in procCatDic['proc'].keys():
            proc = procCatDic['proc']['prefix'] + proc

        if 'prefix' in procCatDic['cat'].keys():
            cat = procCatDic['cat']['prefix'] + cat

        replacements = procCatDic['proc']['replace']
        replacements.update(procCatDic['cat']['replace'])
            
        return proc, cat, replacements

    def evaluate_formulasIn(df, form_dic):
        for form in form_dic.keys():
            df[form] = df.eval(form_dic[form], engine='python')

    print('extCat: {}'.format(extCat))
    print('catR: {}'.format(catR))
    if extCat is not None:
        extended = True if catR == extCat else False
    else:
        extended = False
    print('Out extended: {}'.format(extended))
    
    rFile = ur.open(options.infile)
    if 'SIG' in options.process:
        rFileOA = ur.open(options.infileOA)
        
    cat = catR
    print('-------------------------------------------------------------')
    print(cat)
    print('-------------------------------------------------------------')

    ws = rt.RooWorkspace("cms_hgg_13TeV")

    if 'SIG' in options.process:
        wsOA = rt.RooWorkspace("cms_hgg_13TeV")

    currVariables = cp.deepcopy(variables)
    if 'SIG_125' in options.process and extended:
        currVariables += systematicVariables + nomVariables
    elif 'SIG' in options.process and extended:
        currVariables += nomVariables

    if 'SIG' in options.process and not extended:
        currVariables[0] = (currVariables[0][0], currVariables[0][1], currVariables[0][2], int(160))
        useHists = True
    else:
        useHists = False
            
    proc = config['procs'][process]
    if 'SIG' in options.process:
        procOA = config['procs'][processOA]

    splitCols = list(splitDic.keys())
    columns = splitCols + varList
    if 'SIG' in options.process:
        if extended:
            columns += nomVarList
            if 'SIG_125' in options.process:
                columns += systVarList
    if 'formulas' in config.keys():
        for key in config['formulas'].keys():
            while key in columns:
                columns.remove(key)

    columns = list(set(columns))
    print(columns)
    # df = root_pandas.read_root(
    #     options.infile, '{}/{}_{}'.format(config['treepath'], proc, cat), columns=columns)
    print("Reading the ntuples!")
    df = rFile['{}/{}_{}'.format(config['treepath'], proc, cat)].pandas.df(columns)
    if label is not None and 'SIG_125' in options.process and extended:
        df = labelSystVarsIn(df, systVars, label)

    if 'SIG' in options.process:
        # dfOA = root_pandas.read_root(
        # options.infileOA, '{}/{}_{}'.format(config['treepath'], procOA, cat), columns=columns)
        dfOA = rFileOA['{}/{}_{}'.format(config['treepath'], procOA, cat)].pandas.df(columns)
        if label is not None and 'SIG_125' in options.process and extended:
            dfOA = labelSystVarsIn(dfOA, systVars, label)

    print('Evaluating Functions!')
    if 'functions' in config.keys():
        for key in config['functions']:
            funcStr = config['functions'][key]
            if options.cluster is not None:
                funcStr = 'import numpy as npin\n' + funcStr.replace('np.', 'npin.')
            exec(funcStr, globals())

    print('Evaluating Formulas!')
    if 'formulas' in config.keys():
        evaluate_formulasIn(df, config['formulas'])
        if 'SIG' in options.process:
            evaluate_formulasIn(dfOA, config['formulas'])
            
    print('Making cuts!')
    df.query(cut, engine='python', inplace=True)
    if 'SIG' in options.process:
        dfOA.query(cut, engine='python', inplace=True)

    if 'SIG' in options.process and genPSCut is not None:
        # print('Shifting Events to OOA')
        dfOA = pdin.concat([dfOA, df.query('not ({})'.format(genPSCut), engine='python')], ignore_index=True)
        df.query(genPSCut, engine='python', inplace=True)

    if label is not None:
        if extended:
            cat += '_{}'.format(label)
        else:
            cat = catR[:catR.index(catR.split('_')[-1])-1] + '_{}_'.format(label) + catR.split('_')[-1].replace('Up','_{}Up'.format(label)) if 'Up' in catR.split('_')[-1] else (catR[:catR.index(catR.split('_')[-1])-1] + '_{}_'.format(label) + catR.split('_')[-1].replace('Down','_{}Down'.format(label)) if 'Down' in catR.split('_')[-1] else catR[:catR.index(catR.split('_')[-1])-1] + '_{}_'.format(label) + catR.split('_')[-1] + '_{}'.format(label))

        if any(st in cat for st in config['correlatedSyst']):
            cat = cat.replace('_{}Down'.format(label),'Down')
            cat = cat.replace('_{}Up'.format(label),'Up')
        
    if replace is not None:
        proc = proc.replace(replace[0], replace[1])

    # if os.path.exists('{}/{}'.format(outfolder, outfile)):
    #     existingWS = True
    # else:
    #     existingWS = False

    print('Getting out proc cat')
    procOut, catOut, replacements = getProcCatOutIn(proc, cat)

    catLabel = None if extended else catR
    labelHere = None if extended else label

    actLabels = []
    for procO in procOut:
        f = rt.TFile(getFilenameIn(outfolder, outfile, catLabel, labelHere), "RECREATE")
        w = t2din.RooWorkspaceFromDataframe(
            df, splitDic, currVariables, weight, "cms_hgg_13TeV", (procO, catOut), ws, useHists=useHists, replacementNames=replacements)
        w.makeCategories()
        w.makeWorkspace()
        ws.Write("cms_hgg_13TeV")
        f.Close()
        actLabels.append(w.actualLabels[0])

    if 'SIG' in options.process:
        procOAOut, _, _ = getProcCatOutIn(procOA, cat)
        for procOOA in procOAOut:
            fOA = rt.TFile(getFilenameIn(outfolderOA, outfileOA, catLabel, labelHere), "RECREATE")
        
            wOA = t2din.RooWorkspaceFromDataframe(
                dfOA, splitDicOA, currVariables, weight, "cms_hgg_13TeV", (procOOA, catOut), wsOA, useHists=useHists, replacementNames=replacements)
            wOA.makeCategories()
            wOA.makeWorkspace()
            wsOA.Write("cms_hgg_13TeV")
            fOA.Close()

    # print('Actual labels: ', w.actualLabels)
    procs_temp = ['{}_{}'.format(procOut, lab) for lab in actLabels]
    cats_temp = []
    for labs in w.actualLabels[1:]:
        if len(cats_temp) == 0:
            cats_temp = ['{}'.format(lab) for lab in labs]
        else:
            cats_temp = ['{}_{}'.format(categ, lab) for lab in labs for categ in cats_temp]
    cats_temp = ['{}_{}'.format(categ, catOut) for categ in cats_temp]
    # if len(w.actualLabels) > 1 and all('gen' in la for la in w.actualLabels[0]):
    #     procs_temp = ['{}_{}'.format(procOut, lab) for lab in w.actualLabels[0]]
    #     cats_temp = [
    # else:
    #     procs_temp = ['{}'.format(procOut)]
    #     cats_temp = ['{}_{}'.format(lab, catOut) for lab in w.actualLabels[0]]

    # workS = w.getWorkspace()
    # f = rt.TFile('{}/{}'.format(outfolder, outfile), "UPDATE")
    # workS.Write("cms_hgg_13TeV")
    # f.Write()
    # f.Close()
    # if 'SIG' in options.process:
    #     workSOA = wOA.getWorkspace()
    #     fOA = rt.TFile('{}/{}'.format(outfolderOA, outfileOA), "UPDATE")
    #     workSOA.Write("cms_hgg_13TeV")
    #     fOA.Write()
    #     fOA.Close()
            
    del w, ws, df
    if 'SIG' in options.process:
        del wOA, wsOA, dfOA

    print('Done with {}!'.format(catR))
    if extended:
        return procs_temp, cats_temp

    return None, None

def main(options):

    outfolder = os.path.abspath(options.outfolder)
    if not os.path.exists(outfolder):
        os.mkdir(outfolder)
    if options.outfolderOA is not None:
        outfolderOA = os.path.abspath(options.outfolderOA)
        if not os.path.exists(outfolderOA):
            os.mkdir(outfolderOA)
    else:
        outfolderOA = options.outfolderOA

    config = yaml.load(open(options.config))

    if options.process == 'Data':
        for key in config['splits'].keys():
            if 'gen' in key:
                del config['splits'][key]
        if 'formulas' in config.keys():
            for key in config['formulas'].keys():
                if 'gen' in key:
                    del config['formulas'][key]

    processOA = None
    if 'SIG' in options.process:
        process = options.process.replace('SIG', 'IA')
        processOA = options.process.replace('SIG', 'OA')
    else:
        process = options.process
        
    print(config['splits'])
    splitDic = OrderedDict(config['splits'])

    splitDicOA = None
    if 'SIG' in options.process:
        splitDicOA = copy.deepcopy(splitDic)
        remKeys = []
        for key in splitDicOA.keys():
            if 'gen' in key:
                remKeys.append(key)
        for key in remKeys:
            del splitDicOA[key]
    
    if process == 'Data':
        varDic = {'CMS_hgg_mass': [100, 180],
                  'weight': ['-inf', 'inf'], 'lumi': ['-inf', 'inf']}
        config['categories'] = [config['categories'][0]]
    elif process in ['SIG_120', 'SIG_130']:
        config['categories'] = [config['categories'][0]]
        varDic = config['variables']
    else:
        varDic = config['variables']
        
    print(varDic)
    variables = parse_variables(varDic)
    nomVariables = []
    if 'SIG' in options.process:
        nomVariables = parse_variables(config['nominalVariables'])
        
    cut = config['cut']

    genPSCut = None
    if 'phasespace' in config.keys():
        genPSCut = config['phasespace']['gen']
        cut = '({0}) and ({1})'.format(cut, config['phasespace']['reco'])

    if process == 'Data':
        weight = 'weight'
    else:
        weight = config['weight']
    outfile = config['filenames'][process]
    outfileOA = None
    if 'SIG' in options.process:
        outfileOA = config['filenames'][processOA]
    label = options.label
    if 'replace' in config.keys():
        replace = config['replace']
    else:
        replace = None

    systVars = config['systVars']
    systematicVariables = []
    for var in systVars:
        for dirr in ['Up01sigma', 'Down01sigma']:
            if label is not None:
                systematicVariables.append(('{}_{}{}'.format(var, label, dirr), -999999., 999999.))
            else:
                systematicVariables.append(('{}{}'.format(var, dirr), -999999., 999999.))

    theoryWeights = config['theoryWeights']
    print(theoryWeights)
    for theoryWeight in theoryWeights.keys():
        for j in range(theoryWeights[theoryWeight]):
            systematicVariables.append(('{}{}'.format(theoryWeight, j), -999999., 999999.))

    varList = [var[0] for var in variables]
    nomVarList = []
    systVarList = []
    if 'SIG' in options.process:
        nomVarList = [var[0] for var in nomVariables]
        if label is not None:
            systVarList = [var[0].replace('_{}'.format(label),'') for var in systematicVariables]
        else:
            systVarList = [var[0] for var in systematicVariables]

    addRep = list(config['functions'].keys()) if ('functions' in config.keys()) else []
    # addRep += list(config['formulas'].keys()) if ('formulas' in config.keys()) else []
    
    print(addRep)
    print('varList before: {}'.format(varList))
    varList += parseVariablesFromFormula(cut, addRep)
    if genPSCut is not None and 'SIG' in options.process:
        varList += parseVariablesFromFormula(genPSCut, addRep)
    if 'formulas' in config.keys():
        varList += parseVariablesFromFormulas(config['formulas'], addRep)

    print(varList)
    procs = []
    cats = []

    # if os.path.exists('{}/{}'.format(outfolder, outfile)):
    #     os.remove('{}/{}'.format(outfolder, outfile))
    
    # if 'SIG' in options.process:
    #     if os.path.exists('{}/{}'.format(outfolderOA, outfileOA)):
    #         os.remove('{}/{}'.format(outfolderOA, outfileOA))

    # rFile = uproot.open(options.infile)
    # rFileOA = None
    # if 'SIG' in options.process:
    #     rFileOA = uproot.open(options.infileOA)

    categs = config['categories'] if options.process == 'SIG_125' else [config['categories'][0]]

    extended = True
    if options.simple:
        extended = False
    extCat = config['categories'][0] if extended else None
    wasExtd = True if extended else False

    if options.cluster is None:
        for cat in categs:
            procs_temp, cats_temp = runOneCat(cat, config, options, variables, systematicVariables, nomVariables, splitDic, varList, nomVarList, systVarList, systVars, label, process, outfolder, outfile, replace, weight, cut, genPSCut, outfolderOA, outfileOA, processOA, splitDicOA, extCat)

        if procs_temp is not None and cats_temp is not None:
            procs.extend([x for x in procs_temp if x not in procs])
            cats.extend([x for x in cats_temp if x not in cats])
    else:
        if options.cluster == 'joblib':
            res = Parallel(n_jobs=-1, verbose=20)(delayed(runOneCat)(cat, config, options, variables, systematicVariables, nomVariables, splitDic, varList, nomVarList, systVarList, systVars, label, process, outfolder, outfile, replace, weight, cut, genPSCut, outfolderOA, outfileOA, processOA, splitDicOA, extCat) for cat in categs)

        elif options.cluster == 'dask':
            print('Getting Cluster')
            client = Client(getCluster())
            print('Running in parallel')
            # procCatFutures = client.map(runOneCat, categs, **kwargs)
            procCatFutures = []
            for cat in categs:
                procCatFutures.append(client.submit(runOneCat, cat, config, options, variables, systematicVariables, nomVariables, splitDic, varList, nomVarList, systVarList, systVars, label, process, outfolder, outfile, replace, weight, cut, genPSCut, outfolderOA, outfileOA, processOA, splitDicOA, extCat))
    
            progress(procCatFutures)
            res = [future.result() for future in procCatFutures]

        for procs_temp, cats_temp in res:
            if procs_temp is not None and cats_temp is not None:
                procs.extend([x for x in procs_temp if x not in procs])
                cats.extend([x for x in cats_temp if x not in cats])



    print(procs, cats)

    if wasExtd:
        for mass in ['120', '125', '130']:
            try:
                procs[0].index('_{}_13TeV'.format(mass))
                procs = [pro.replace(mass, '') for pro in procs]
            except ValueError:
                pass
            
        procs = [pro.replace('__13TeV', '') for pro in procs]
        write_cat_procs_file(
            '{}/proc_cat_names_{}.txt'.format(outfolder, splitDic.keys()[0]), procs, cats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    requiredAgrs = parser.add_argument_group()
    requiredAgrs.add_argument(
        '--infile', '-i', action='store', type=str, required=True)
    requiredAgrs.add_argument(
        '--process', '-p', action='store', type=str, required=True)
    requiredAgrs.add_argument(
        '--outfolder', '-o', action='store', type=str, required=True)
    requiredAgrs.add_argument(
        '--config', '-c', action='store', type=str, required=True)
    optionalArgs = parser.add_argument_group()
    optionalArgs.add_argument('--label', '-l', action='store', type=str)
    optionalArgs.add_argument('--simple', '-s', action='store_true', default=False)
    optionalArgs.add_argument('--outfolderOA', action='store', type=str)
    optionalArgs.add_argument('--cluster', action='store', type=str)
    requiredAgrs.add_argument('--infileOA', action='store', type=str)
    options = parser.parse_args()
    main(options)
