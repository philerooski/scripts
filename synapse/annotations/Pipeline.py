import pandas as pd
import synapseclient as sc
import json
from . import utils
from copy import deepcopy

class Pipeline:
    """ Annotations pipeline object. """

    BACKUP_LENGTH = 50

    def __init__(self, syn, view=None, activeCols=None, meta=None):
        self.syn = syn
        self.df = utils.synread(
                self.syn, view) if isinstance(view, str) else deepcopy(view)
        self.activeCols = []
        if activeCols: self.addActiveCols(activeCols)
        self.meta = self.parseMetadata(meta)
        self.backup = []

    def head(self):
        print(self.df.head())

    def columns(self):
        self._prettyPrintColumns(self.df.columns)

    def activeColumns(self):
        self._prettyPrintColumns(self.activeCols)

    def addActiveCols(self, activeCols, path=False):
        # activeCols can be a str, list, dict, or DataFrame
        if isinstance(activeCols, str) and not path:
            self.activeCols.append(activeCols)
        elif isinstance(activeCols, (list, dict)) and not path:
            for c in activeCols: self.activeCols.append(c)
        elif isinstance(activeCols, pd.core.frame.DataFrame):
            # assumes column names are in first column
            for c in activeCols[activeCols.columns[0]]:
                self.activeCols.append(c)
        elif path:
            pass

    def removeActiveCols(self, activeCols):
        if isinstance(activeCols, str):
            self.activeCols.remove(activeCols)
        else: # is list-like
            for c in activeCols:
                self.activeCols.remove(c)

    def parseMetadata(self, metadata):
        # metadata can be a str, list, or DataFrame
        if isinstance(metadata, str):
            self.meta = utils.synread(self.syn, metadata)
        elif isinstance(metadata, list):
            self.meta = utils.combineSynapseTabulars(self.syn, metadata)
        else:
            self.meta = deepcopy(metadata)

    def undo(self):
        if self.backup:
            backup, message = self.backup.pop()
            self.syn = backup.syn
            self.df = backup.df
            self.activeCols = backup.activeCols
            print("Undo: {}".format(message))
        else:
            print("At last available change.")

    def _backup(self, message):
        self.backup.append((Pipeline(
            self.syn, self.df, self.activeCols, self.meta), message))
        if len(self.backup) > self.BACKUP_LENGTH:
            self.backup = self.backup[1:]

    def _dropDuplicateCols(self, cols, preexistingCols):
        preexistingColNames = [c['name'] for c in preexistingCols]
        uniqueCols = [c for c in cols if not c['name'] in preexistingCols]
        return uniqueCols

    def valueCounts(self):
        for c in self.activeCols:
            print(self.df[c].value_counts(dropna=False))

    def _prettyPrintColumns(self, cols):
        for i in range(len(cols)):
            if 65 + i > 90:
                i_ = i % 26
                print("A{}".format(chr(65 + i_)), "|", cols[i])
            else:
                print(chr(65 + i), "|", cols[i])

    def addDefaultValues(self, colVals, backup=True):
        if backup: self._backup("addDefaultValues")
        for k in colVals:
            self.df[k] = colVals[k]

    def addFileFormatCol(self, referenceCol='name', fileFormatColName='fileFormat'):
        self._backup("addFiletypeCol")
        filetypeCol = utils.makeColFromRegex(self.df[referenceCol].values, "extension")
        self.df[fileFormatColName] = filetypeCol

    def newFileView(self, name, parent, scope, addCols=None):
        self._backup("newFileView")
        if isinstance(scope, str): scope = [scope]
        params = {'scope': scope, 'viewType': 'file'}
        cols = self.syn.restPOST('/column/view/scope',
                json.dumps(params))['results']
        if self.activeCols:
            activeCols = utils.makeColumns(self.activeCols, asSynapseCols=False)
            cols += self._dropDuplicateCols(activeCols, cols)
        if addCols:
            newCols = utils.makeColumns(addCols, asSynapseCols=False)
            cols += self._dropDuplicateCols(newCols, cols)
        cols = [sc.Column(**c) for c in cols]
        schema = sc.EntityViewSchema(name=name, columns=cols,
                parent=parent, scopes=scope)
        self.schema = self.syn.store(schema)
        self.df = utils.synread(self.syn, self.schema.id)
        self.index = self.df.index
        if isinstance(addCols, dict): self.addDefaultValues(addCols, False)
        print("File view created here: {}".format(self.schema.id))

    def inferValues(self, col, referenceCols):
        self._backup("inferValues")
        groups = self.df.groupby(referenceCols)
        values = groups[col].unique()
        for k, v in values.items():
            v = v[pd.notnull(v)] # filter out na values
            if len(v) == 1:
                df_.loc[df_[referenceCols] == k, col] = v[0]
            else:
                print("Unable to infer value when {} = {}".format(
                    referenceCols, k))
