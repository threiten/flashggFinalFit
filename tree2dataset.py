import pandas as pd
import numpy as np
import root_numpy
import ROOT
import itertools


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

    def __init__(self, df, splitDic, variables, weight, wsName, dsetPreSuffix=None, workspace=None, useHists=False, replacementNames=None):

        ROOT.RooMsgService.instance().setGlobalKillBelow(ROOT.RooFit.WARNING)
        self.splitDic = splitDic
        self.variables = variables
        self.weight = weight
        self.useHists = useHists
        self.name = wsName
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

        self.splitTree(df.loc[:, [var[0] for var in variables] + list(splitDic.keys())])

    def __del__(self):

        del self.workspace
        del self.gb

    def splitTree(self, df):

        binvars = []
        for key in self.splitDic.keys():
            df['{}_bin'.format(key)] = pd.cut(df[key], bins=self.splitDic[key], labels=self.makeBinLabels(key, self.splitDic[key]), include_lowest=True, right=False)
            
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

        self.labels = []
        for key in self.splitDic.keys():
            self.labels.append(self.makeBinLabels(key, self.splitDic[key]))
        self.categories = list(itertools.product(*self.labels))

    def makeWorkspace(self):
            
        dummyDf = pd.DataFrame(columns=[var[0] for var in self.variables], dtype=np.float32)
        
        for cat in self.categories:
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
                getattr(self.workspace, 'import')(dset, ROOT.RooFit.Rename(dset.GetName()))

            self.actualLabels = []
            for i, lls in enumerate(self.labels):
                self.actualLabels.append([])
                for label in lls:
                    self.actualLabels[i].append(self.updateDsetlabel(label))

    def getWorkspace(self):

        self.makeCategories()
        self.makeWorkspace()
        
        return self.workspace
