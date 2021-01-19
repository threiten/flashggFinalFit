import pandas as pd
import numpy as np
import root_numpy
import ROOT
import itertools
import copy
from collections import OrderedDict


class RooDatasetFromDataframe(object):

    def __init__(self, df, variables, weight, dsetName):

        ROOT.RooMsgService.instance().setGlobalKillBelow(ROOT.RooFit.WARNING)
        self.df = df
        self.weight = weight
        self.parseVariables(variables)
        self.variables = [var[0] for var in variables]
        self.name = dsetName

        if 'weight' not in self.variables:
            self.variables.append('weight')

    def __del__(self):

        del self.df
        del self.dataSet

    def prepareTTree(self):

        self.df['weight'] = self.df.eval(self.weight)
        rec = self.df.loc[:, self.variables].to_records(index=False)
        self.tree = root_numpy.array2tree(rec)

    def parseVariables(self, variables):

        self.rooRealVars = []
        for tpl in variables:
            self.rooRealVars.append(ROOT.RooRealVar(tpl[0], tpl[0], tpl[1], tpl[2]))

    def prepareDataset(self):

        argSet = ROOT.RooArgSet()
        for var in self.rooRealVars:
            argSet.add(var)

        if not hasattr(self, 'tree'):
            self.prepareTTree()

        self.dataSet = ROOT.RooDataSet(self.name, self.name, self.tree, argSet, '1', 'weight')

    def getDataset(self):

        self.prepareDataset()
        return self.dataSet


class RooHistogramFromDataframe(RooDatasetFromDataframe):

    def __init__(self, df, variable, weight, histName):

        ROOT.RooMsgService.instance().setGlobalKillBelow(ROOT.RooFit.WARNING)
        if len(variable) > 2:
            raise TypeError('Maximally two variables accepted')
        elif len(variable[0]) < 4:
            raise ValueError('First variable needs nBins as last entry')

        super(RooHistogramFromDataframe, self).__init__(df, variable, weight, histName)
        self.nBins = variable[0][3]
        self.xLow = variable[0][1]
        self.xHigh = variable[0][2]

    def __del__(self):

        super(RooHistogramFromDataframe, self).__del__()
        del self.RooHistogram

    def prepareHistogram(self):
            
        hist = ROOT.TH1F(self.name, self.name, self.nBins, self.xLow, self.xHigh)
        self.prepareDataset()
        self.dataSet.fillHistogram(hist, ROOT.RooArgList(self.rooRealVars[0]))
        self.RooHistogram = ROOT.RooDataHist(self.name, self.name, ROOT.RooArgList(self.rooRealVars[0]), hist)

    def getRooHist(self):

        self.prepareHistogram()

        return self.RooHistogram


class RooWorkspaceFromDataframe(object):

    def __init__(self, df, splitDic, variables, weight, wsName, dsetPreSuffix=None, workspace=None, useHists=False, replacementNames=None, splitByProc=False):

        ROOT.RooMsgService.instance().setGlobalKillBelow(ROOT.RooFit.WARNING)
        self.splitDic = splitDic
        self.variables = variables
        self.weight = weight
        self.useHists = useHists
        self.name = wsName
        self.splitByProc = splitByProc
        if dsetPreSuffix is not None:
            self.dsetPreSuffix = dsetPreSuffix

        if replacementNames is not None:
            self.replacementNames = replacementNames
            
        if workspace is not None and isinstance(workspace, ROOT.RooWorkspace):
            self.workspace = workspace
            self.createdDS = False
        elif workspace is not None:
            print("Object passed to workspace variable has to be a RooWorkspace. Object you passed is not and will not be read, a new RooWorkspace will be created")
            self.workspace = ROOT.RooWorkspace(self.name)
            self.createdDS = True
        elif workspace is None:
            self.workspace = ROOT.RooWorkspace(self.name)
            self.createdDS = True

        if self.splitByProc:
            self.workspace = [self.workspace]

        if not isinstance(splitDic[list(splitDic.keys())[0]], OrderedDict):
            self.splitCols = list(splitDic.keys())
        else:
            self.splitCols = []
            for key, item in splitDic.items():
                self.splitCols += list(item.keys())
        self.splitTree(df.loc[:, [var[0] for var in variables] + self.splitCols])

    def __del__(self):

        del self.workspace
        del self.gb

    def splitTree(self, df):

        binvars = []
        for key, item in self.splitDic.items():
            if isinstance(item, OrderedDict):
                lowerLev = {}
                upperLev = {}
                uppNames = []
                uppLabels = []
                for ky, itte in item.items():
                    if isinstance(itte[0], list):
                        lowerLev[ky] = itte
                    else:
                        upperLev[ky] = itte
                for ke, itt in upperLev.items():
                    uppLabels.append(self.makeBinLabels(ke, itt))
                    uppNames.append('{}_bin'.format(ke))
                    df['{}_bin'.format(ke)] = pd.cut(df[ke], bins=itt, labels=self.makeBinLabels(ke, itt), include_lowest=True, right=False)
                    binvars.append('{}_bin'.format(ke))
                uppLevCats = list(itertools.product(*uppLabels))
                uppLevNames = []
                for ka, ite in lowerLev.items():
                    llSplDic = {}
                    for k, llSpl in enumerate(ite):
                        nameStr = ''
                        for st in uppLevCats[k]:
                            nameStr += '{}_'.format(st)
                        nameStr = nameStr[:-1]
                        uppLevNames.append(nameStr)
                        llSplDic[nameStr] = llSpl
                    llGb = df.groupby(uppNames)
                    df['{}_bin'.format(ka)] = -999. * np.ones_like(df[ka].values)
                    for l, cat in enumerate(uppLevNames):
                        df.loc[llGb.get_group(cat).index, '{}_bin'.format(ka)] = pd.cut(llGb.get_group(cat)[ka], bins=llSplDic[cat], labels=self.makeBinLabels(ka, llSplDic[cat]))
                    binvars.append('{}_bin'.format(ka))
                        
            else:
                df['{}_bin'.format(key)] = pd.cut(df[key], bins=item, labels=self.makeBinLabels(key, item), include_lowest=True, right=False)
                binvars.append('{}_bin'.format(key))
                
        self.gb = df.groupby(binvars)

    @staticmethod
    def makeBinLabels(var, bins):
        return ['{}_{}_{}'.format(var, bins[i], bins[i+1]).replace('.', 'p').replace('-', 'm') for i in range(len(bins) - 1)]

    def updateDsetlabel(self, dsetlabel):
        if hasattr(self, 'replacementNames'):
            for key, item in self.replacementNames.items():
                dsetlabel = dsetlabel.replace(key, item)

        return dsetlabel

    def makeCategories(self):

        if 'reco' in self.splitDic.keys() and 'expmtl' in self.splitDic.keys():
            self.labels = []
            regCats = {}
            regLabels = {}
            catVec = []
            regList = list(self.splitDic.keys())
            regList.remove('expmtl')
            for reg in regList:
                regLabels[reg] = []
                for key, item in self.splitDic[reg].items():
                    if isinstance(item[0], list):
                        inlabels = []
                        for it in item:
                            inlabels.append(self.makeBinLabels(key, it))
                        regLabels[reg].append(inlabels)
                    else:
                        regLabels[reg].append(self.makeBinLabels(key, item))
                for i in range(len(regLabels[reg])):
                    for k in range(len(regLabels[reg][i])):
                        if not isinstance(regLabels[reg][i][k], list):
                            regLabels[reg][i][k] = [regLabels[reg][i][k]]
                regLabels[reg] = np.array(regLabels[reg]).T
                regCats[reg] = []
                for lbl in regLabels[reg]:
                    regCats[reg] += list(itertools.product(*lbl))
                catVec.append(regCats[reg])
                labString = []
                for cat in regCats[reg]:
                    labl = ''
                    for pa in cat:
                        labl += '{}_'.format(pa)
                    labString.append(labl[:-1])
                self.labels.append(labString)
            labsExp = [self.makeBinLabels(key, item) for key, item in self.splitDic['expmtl'].items()]
            for j in range(len(labsExp)):
                self.labels.append(copy.deepcopy(labsExp[j]))            
                for i, lab in enumerate(labsExp[j]):
                    labsExp[j][i] = tuple([lab])
            catVec += labsExp
            self.categories = list(itertools.product(*catVec))
            for i in range(len(self.categories)):
                flat = ()
                for tpl in self.categories[i]:
                    flat += tpl
                self.categories[i] = flat
        else:
            self.labels = [self.makeBinLabels(key, item) for key, item in self.splitDic.items()]
            self.categories = list(itertools.product(*self.labels))

    def makeWorkspace(self):

        dummyDf = pd.DataFrame(columns=[var[0] for var in self.variables], dtype=np.float32)

        prevProc = self.categories[0][0]
        for cat in self.categories:
            currProc = cat[0]
            dsetlabel = ''
            for varl in cat:
                dsetlabel += '{}_'.format(varl)
            dsetlabel = dsetlabel[:-1]

            if hasattr(self, 'dsetPreSuffix'):
                dsetlabel = '{}_{}_{}'.format(self.dsetPreSuffix[0], dsetlabel, self.dsetPreSuffix[1])

            dsetlabel = self.updateDsetlabel(dsetlabel)

            if cat in self.gb.indices.keys():
                if self.useHists:
                    dsetFrDf = RooHistogramFromDataframe(self.gb.get_group(cat), self.variables, self.weight, dsetlabel)
                else:
                    dsetFrDf = RooDatasetFromDataframe(self.gb.get_group(cat), self.variables, self.weight, dsetlabel)
            else:
                if self.useHists:
                    dsetFrDf = RooHistogramFromDataframe(dummyDf, self.variables, self.weight, dsetlabel)
                else:
                    dsetFrDf = RooDatasetFromDataframe(dummyDf, self.variables, self.weight, dsetlabel)

            if self.useHists:
                dset = dsetFrDf.getRooHist()
            else:
                dset = dsetFrDf.getDataset()
            if not self.createdDS:
                if self.splitByProc:
                    if currProc != prevProc:
                        self.workspace.append(ROOT.RooWorkspace(self.name))
                    getattr(self.workspace[-1], 'import')(dset, ROOT.RooFit.Rename(dset.GetName()))
                else:
                    getattr(self.workspace, 'import')(dset, ROOT.RooFit.Rename(dset.GetName()))

            prevProc = cat[0]
            self.actualLabels = []
            for i, lls in enumerate(self.labels):
                self.actualLabels.append([])
                for label in lls:
                    self.actualLabels[i].append(self.updateDsetlabel(label))

    def getWorkspace(self):
        
        return self.workspace
