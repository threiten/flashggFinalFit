import tree2dataset as t2d
import ROOT
import root_pandas
from collections import OrderedDict
import argparse
import os
import copy
import oyaml as yaml
import numpy as np
import pandas as pd


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


def evaluate_formulas(df, form_dic):
    for form in form_dic.keys():
        df[form] = df.eval(form_dic[form], engine='python')

def parseVariablesFromFormulas(formulas, addRep):
    ret = []
    for key in formulas.keys():
        ret += parseVariablesFromFormula(formulas[key], addRep)

    return ret

def parseVariablesFromFormula(formula, addRep):
    reps = ['(', ')', '@', ',', 'abs', '+', '-', '*', '/', 'cos', 'sin', 'cosh', 'sinh', 'sqrt']
    reps += addRep

    for rep in reps:
        formula = formula.replace(rep, ' ')

    ret = []
    for part in formula.split():
        try:
            float(part)
        except ValueError:
            ret.append(part)

    return ret

def parseVariablesFromCut(cut):
    reps = ['(', ')', 'and', 'or', 'not', '=', '+', '-', '<', '>', 'abs', 'sqrt']

    for rep in reps:
        cut = cut.replace(rep, ' ')

    ret = []
    for part in cut.split():
        try:
            float(part)
        except ValueError:
            ret.append(part)

    return ret


def main(options):

    outfolder = os.path.abspath(options.outfolder)
    if not os.path.exists(outfolder):
        os.mkdir(outfolder)
    if options.outfolderOA is not None:
        outfolderOA = os.path.abspath(options.outfolderOA)
        if not os.path.exists(outfolderOA):
            os.mkdir(outfolderOA)        

    config = yaml.load(open(options.config))

    if options.process == 'Data':
        for key in config['splits'].keys():
            if 'gen' in key:
                del config['splits'][key]
        if 'formulas' in config.keys():
            for key in config['formulas'].keys():
                if 'gen' in key:
                    del config['formulas'][key]

    if 'SIG' in options.process:
        process = options.process.replace('SIG', 'IA')
        processOA = options.process.replace('SIG', 'OA')
    else:
        process = options.process
        
    print(config['splits'])
    splitDic = OrderedDict(config['splits'])

    if 'SIG' in options.process:
        splitDicOA = copy.deepcopy(splitDic)
        for key in splitDicOA.keys():
            if 'gen' in key:
                del splitDicOA[key]
    
    if process == 'Data':
        varDic = {'CMS_hgg_mass': [100, 180],
                  'weight': ['-inf', 'inf'], 'lumi': ['-inf', 'inf']}
    else:
        varDic = config['variables']
    print(varDic)
    variables = parse_variables(varDic)
        
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
    if 'SIG' in options.process:
        outfileOA = config['filenames'][processOA]
    label = options.label
    if 'replace' in config.keys():
        replace = config['replace']
    else:
        replace = None

    systVars = config['systVars']
    for var in systVars:
        for dirr in ['Up01sigma', 'Down01sigma']:
            variables.append(('{}{}'.format(var, dirr), -1000, 1000))

    varList = [var[0] for var in variables]
    varList += parseVariablesFromCut(cut)
    if genPSCut is not None and 'SIG' in options.process:
        varList += parseVariablesFromCut(genPSCut)
    if 'formulas' in config.keys():
        addRep = list(config['functions'].keys()) if ('functions' in config.keys()) else []
        print(addRep)
        print(config['formulas'])
        varList += parseVariablesFromFormulas(config['formulas'], addRep)

    print(varList)
    procs = []
    cats = []
    ws = ROOT.RooWorkspace('cms_hgg_13TeV')

    if 'SIG' in options.process:
        wsOA = ROOT.RooWorkspace('cms_hgg_13TeV')
    
    for cat in config['categories']:

        proc = config['procs'][process]
        if 'SIG' in options.process:
            procOA = config['procs'][processOA]
            
        splitCols = list(splitDic.keys())
        if 'formulas' in config.keys():
            for key in config['formulas'].keys():
                splitCols.remove(key)
        columns = splitCols + varList

        df = root_pandas.read_root(
            options.infile, '{}/{}_{}'.format(config['treepath'], proc, cat), columns=columns)

        if 'SIG' in options.process:
            dfOA = root_pandas.read_root(
            options.infileOA, '{}/{}_{}'.format(config['treepath'], procOA, cat), columns=columns)

        if 'functions' in config.keys():
            for key in config['functions']:
                exec(config['functions'][key], globals())

        if 'formulas' in config.keys():
            evaluate_formulas(df, config['formulas'])
            if 'SIG' in options.process:
                evaluate_formulas(dfOA, config['formulas'])

        df.query(cut, engine='python', inplace=True)
        if 'SIG' in options.process:
            dfOA.query(cut, engine='python', inplace=True)

        if 'SIG' in options.process and genPSCut is not None:
            print('Shifting Events to OOA')
            dfOA = pd.concat([dfOA, df.query('not ({})'.format(genPSCut), engine='python')], ignore_index=True)
            df.query(genPSCut, engine='python', inplace=True)
        
        if label is not None:
            cat += '_{}'.format(label)
        if replace is not None:
            proc = proc.replace(replace[0], replace[1])

        w = t2d.RooWorkspaceFromDataframe(
            df, splitDic, variables, weight, "cms_hgg_13TeV", (proc, cat), ws)
        w.makeCategories()
        w.makeWorkspace()

        if 'SIG' in options.process:
            wOA = t2d.RooWorkspaceFromDataframe(
                dfOA, splitDicOA, variables, weight, "cms_hgg_13TeV", (procOA, cat), wsOA)
            wOA.makeCategories()
            wOA.makeWorkspace()
            
        if len(w.labels) > 1 and all('gen' in la for la in w.labels[0]):
            procs_temp = ['{}_{}'.format(proc, lab) for lab in w.labels[0]]
            cats_temp = []
            for labs in w.labels[1:]:
                if len(cats_temp) == 0:
                    cats_temp = ['{}'.format(lab) for lab in labs]
                else:
                    cats_temp = ['{}_{}'.format(categ, lab)
                                 for lab in labs for categ in cats_temp]
            cats_temp = ['{}_{}'.format(categ, cat) for categ in cats_temp]
        else:
            procs_temp = ['{}'.format(proc)]
            cats_temp = ['{}_{}'.format(lab, cat) for lab in w.labels[0]]

        procs.extend([x for x in procs_temp if x not in procs])
        cats.extend([x for x in cats_temp if x not in cats])

        del w
        if 'SIG' in options.process:
            del wOA

    for mass in ['120', '125', '130']:
        try:
            procs[0].index(mass)
            procs = [pro.replace(mass, '') for pro in procs]
        except ValueError:
            pass

    procs = [pro.replace('__13TeV', '') for pro in procs]
    write_cat_procs_file(
        '{}/proc_cat_names_{}.txt'.format(outfolder, splitDic.keys()[0]), procs, cats)

    f = ROOT.TFile('{}/{}'.format(outfolder, outfile), "RECREATE")
    ws.Write("cms_hgg_13TeV")
    f.Write()
    f.Close()
    
    if 'SIG' in options.process:
        fOA = ROOT.TFile('{}/{}'.format(outfolderOA, outfileOA), "RECREATE")
        wsOA.Write("cms_hgg_13TeV")
        fOA.Write()
        fOA.Close()


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
    optionalArgs.add_argument('--outfolderOA', action='store', type=str)
    requiredAgrs.add_argument('--infileOA', action='store', type=str)
    options = parser.parse_args()
    main(options)
