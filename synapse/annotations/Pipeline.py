import pandas as pd
import synapseclient as sc
import readline
import json
from . import utils
from copy import deepcopy

class Pipeline:
    """ Annotations pipeline object. """

    BACKUP_LENGTH = 50

    def __init__(self, syn, view=None, meta=None, activeCols=[],
            metaActiveCols=[], link=None, sortCols=True):
        self.syn = syn
        self.df = view if view is None else self._parseView(view, sortCols)
        self.schema = self.syn.get(view, downloadFile=False) if isinstance(
                view, str) else None
        self.index = self.df.index if isinstance(
                self.df, pd.core.frame.DataFrame) else None
        self.activeCols = []
        if activeCols: self.addActiveCols(activeCols)
        self.meta = meta if meta is None else self._parseView(meta, sortCols)
        self.metaActiveCols = []
        if metaActiveCols: self.addActiveCols(metaActiveCols, meta=True)
        self.sortCols = sortCols
        self.keyCol = None
        self.link = link
        self.index = None
        self.schema = None
        self.backup = []

    def _backup(self, message):
        self.backup.append((Pipeline(
            self.syn, self.df, self.meta, self.activeCols, self.metaActiveCols,
            self.link, self.sortCols), message))
        if len(self.backup) > self.BACKUP_LENGTH:
            self.backup = self.backup[1:]

    def undo(self):
        if self.backup:
            backup, message = self.backup.pop()
            self.syn = backup.syn
            self.df = backup.df
            self.activeCols = backup.activeCols
            print("Undo: {}".format(message))
        else:
            print("At last available change.")

    def head(self):
        print(self.df.head())

    def columns(self, style="letters"):
        if hasattr(self.df, 'columns'):
            self._prettyPrintColumns(self.df.columns, style)
        else:
            print("No columns.")

    def metaColumns(self, style="letters"):
        if hasattr(self.meta, 'columns'):
            self._prettyPrintColumns(self.meta.columns, style)
        else:
            print("No columns.")

    def activeColumns(self, style="letters"):
        if self.activeCols:
            self._prettyPrintColumns(self.activeCols, style)
        else:
            print("No active columns.")

    def metaActiveColumns(self, style="letters"):
        if self.metaActiveCols:
            self._prettyPrintColumns(self.metaActiveColumns, style)
        else:
            print("No active columns.")

    def addActiveCols(self, activeCols, path=False, meta=False):
        # activeCols can be a str, list, dict, or DataFrame
        if isinstance(activeCols, str) and not path:
            if meta:
                self.metaActiveCols.append(activeCols)
            else:
                self.activeCols.append(activeCols)
        elif isinstance(activeCols, (list, dict)) and not path:
            if meta:
                for c in activeCols: self.metaActiveCols.append(c)
            else:
                for c in activeCols: self.activeCols.append(c)
        elif isinstance(activeCols, pd.core.frame.DataFrame):
            # assumes column names are in first column
            for c in activeCols[activeCols.columns[0]]:
                if meta:
                    self.metaActiveCols.append(c)
                else:
                    self.activeCols.append(c)
        elif path:
            pass

    def addDefaultValues(self, colVals, backup=True):
        if backup: self._backup("addDefaultValues")
        for k in colVals:
            self.df[k] = colVals[k]

    def addKeyCol(self):
        link = self._linkData(1)
        dataKey, metaKey = link.popitem()
        regex = ''
        print("Data", "\n\n")
        print("head")
        print(self.df[dataKey].head(), "\n\n")
        print()
        print("tail")
        print(self.df[dataKey].tail(), "\n\n")
        print("Metadata", "\n\n")
        print(self.meta[metaKey].head(), "\n\n")
        while True:
            regex = self._inputDefault("regex: ", regex)
            newCol = utils.makeColFromRegex(self.df[dataKey].values, regex)
            missingVals = [not v in self.meta[metaKey].values.astype(str) for v in newCol]
            if any(missingVals):
                before_regex = self.df[dataKey][missingVals]
                after_regex = [newCol[i] for i in range(len(newCol)) if missingVals[i]]
                print("The following values were not found in the metadata:")
                for i in range(len(before_regex)):
                    print(after_regex[i], "<-", before_regex[i])
                print()
                proceedAnyways = self._getUserConfirmation()
                if proceedAnyways:
                    break
                else:
                    continue
            else:
                break
        self.keyCol = metaKey
        self.df[metaKey] = newCol

    def _inputDefault(self, prompt, prefill=''):
        readline.set_startup_hook(lambda: readline.insert_text(prefill))
        try:
            return input(prompt)
        finally:
           readline.set_startup_hook()

    def addFileFormatCol(self, referenceCol='name', fileFormatColName='fileFormat'):
        self._backup("addFiletypeCol")
        filetypeCol = utils.makeColFromRegex(self.df[referenceCol].values, "extension")
        self.df[fileFormatColName] = filetypeCol

    def addLinks(self, links):
        if not isinstance(links, dict):
            raise TypeError("`links` must be a dictionary-like object")
        if not self.link:
            self.link = links
        else:
            for l in links:
                self.link[l] = links[l]
        return links

    def isValidKeyPair(self, dataCol=None, metaCol=None):
        if dataCol is None and metaCol is None:
            dataCol, metaCol = self._linkData(1).popitem()
        if set(self.df[dataCol]).difference(self.meta[metaCol]):
            print("The following values are missing:", end="\n")
            for i in set(self.df[dataCol]).difference(self.meta[metaCol]):
                print(i)
            return False
        return True

    def linkMetadata(self, links=None):
        self._backup("linkMetadata")
        if links is None:
            links = self._linkData(-1)
        for v in links.values():
            if not v in self.metaActiveCols:
                self.metaActiveCols.append(v)
        self.addLinks(links)
        return links

    def modifyColumn(self, col, mod):
        oldCol = self.df[col].values
        if isinstance(mod, dict):
            newCol = [mod[v] for v in oldCol]
        self.df[col] = newCol

    def _parseView(self, view, sortCols):
        if isinstance(view, str):
            return utils.synread(self.syn, view, sortCols)
        elif isinstance(view, list):
            return utils.combineSynapseTabulars(self.syn, view)
        elif isinstance(view, pd.core.frame.DataFrame):
            if sortCols:
                view = view.sort_index(1)
            return deepcopy(view)
        else:
            raise TypeError("{} is not a supported data input type".format(type(view)))

    def parseMetadata(self, metadata, sortCols):
        # metadata can be a str, list, or DataFrame
        if isinstance(metadata, str):
            return utils.synread(self.syn, metadata)
        elif isinstance(metadata, list):
            return utils.combineSynapseTabulars(self.syn, metadata)
        else:
             return deepcopy(metadata)

    def publish(self, verify = True):
        warnings = self._validate()
        if len(warnings):
            for w in warnings:
                print(w)
            print()
            continueAnyways = self._getUserConfirmation()
            if not continueAnyways:
                print("Publish canceled.")
                return
        t = sc.Table(self.schema.id, self.df)
        print("Storing to Synapse...")
        t_online = self.syn.store(t)
        print("Fetching new table index...")
        self.df = utils.synread(self.syn, self.schema.id)
        self.index = self.df.index
        print("You're good to go :~)")
        return self.schema.id

    def _getUserConfirmation(self):
        print("Proceed anyways? (y) or (n): ", end='')
        proceed = ''
        while not len(proceed):
            proceed = input()
            if len(proceed) and not proceed[0] in ['Y', 'y', 'N', 'n']:
                proceed = ''
                print("Please enter 'y' or 'n': ", end='')
            elif len(proceed) and proceed[0].lower() == 'y':
                return True
            elif len(proceed) and proceed[0].lower() == 'n':
                return False

    def onweb(self):
        self.syn.onweb(self.schema.id)

    def _validate(self):
        warnings = []
        # check that no columns have null values
        null_cols = self.df[self.activeCols].isnull().any()
        for i in null_cols.iteritems():
            col, hasna = i
            if hasna:
                warnings.append("{} has null values.".format(col))
        return warnings

    def removeActiveCols(self, activeCols):
        if isinstance(activeCols, str):
            self.activeCols.remove(activeCols)
        else: # is list-like
            for c in activeCols:
                self.activeCols.remove(c)

    def _getUniqueCols(self, newCols, preexistingCols):
        preexistingColNames = [c['name'] for c in preexistingCols]
        uniqueCols = []
        for c in newCols:
            if c['name'] in preexistingColNames:
                # default behavior is to replace the older column with the newer.
                isCol = [c_['name'] == c['name'] for c_ in preexistingCols]
                preexistingCols.pop(isCol.index(True))
            uniqueCols.append(c)
        uniqueCols += preexistingCols
        return uniqueCols

    def valueCounts(self):
        for c in self.activeCols:
            print(self.df[c].value_counts(dropna=False))
            print()

    def _prettyPrintColumns(self, cols, style):
        if style == 'letters':
            for i in range(len(cols)):
                padding = " " if (len(cols) > 26 and (65 + i <= 90)) else ""
                if 65 + i > 90:
                    i_ = i % 26
                    print("A{}".format(chr(65 + i_)), "{}|".format(padding), cols[i])
                else:
                    print(chr(65 + i), "{}|".format(padding), cols[i])
        elif style == 'numbers':
            for i in range(len(cols)):
                padding = " " if (len(cols) > 10 and i < 10) else ""
                print(str(i), "{}|".format(padding), cols[i])

    def newFileView(self, name, parent, scope, addCols=None):
        self._backup("newFileView")
        if isinstance(scope, str): scope = [scope]
        params = {'scope': scope, 'viewType': 'file'}
        cols = self.syn.restPOST('/column/view/scope',
                json.dumps(params))['results']
        if self.activeCols:
            activeCols = utils.makeColumns(self.activeCols, asSynapseCols=False)
            cols = self._getUniqueCols(activeCols, cols)
        if addCols:
            for k in addCols:
                if addCols[k] is None and not k in self.activeCols:
                    self.activeCols.append(k)
            newCols = utils.makeColumns(addCols, asSynapseCols=False)
            cols = self._getUniqueCols(newCols, cols)
        cols = [sc.Column(**c) for c in cols]
        schema = sc.EntityViewSchema(name=name, columns=cols,
                parent=parent, scopes=scope)
        self.schema = self.syn.store(schema)
        self.df = utils.synread(self.syn, self.schema.id)
        self.index = self.df.index
        if isinstance(addCols, dict): self.addDefaultValues(addCols, False)
        return self.schema.id

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

    def transferMetadata(self, cols=None, on=None, how='left', dropOn=True):
        if on is None: on = self.keyCol
        if not self.link: raise RuntimeError("Need to link metadata values first.")
        self._backup("transferMetadata")
        if not cols:
            cols = list(self.link.keys())
            if on in cols:
                cols.pop(cols.index(on))
        relevant_meta = self.meta[list(set(self.link.values()))]
        merged = self.df.merge(relevant_meta, on=on, how=how)
        merged = merged.drop_duplicates()
        print("original", self.df.shape)
        print("merged", merged.shape)
        for c in cols:
            self.df[c] = merged[self.link[c]].values
        if dropOn:
            self.df.drop(on, 1, inplace=True)

    def _linkData(self, iters):
        links = {}
        def _verifyInputIntegrity(i):
            if i is '': return -1
            try:
                i = int(i)
                assert i < len(self.df.columns) and i >= 0
            except:
                print("Please enter an integer corresponding to "
                        "one of the columns above.", "\n")
                return
            return i
        while iters != 0:
            print("Data:", "\n")
            self.columns("numbers")
            print()
            data_col = None
            while data_col is None:
                data_col = input("Select a data column: ")
                data_col = _verifyInputIntegrity(data_col)
            if data_col == -1: return links
            print("\n", "Metadata", "\n")
            self.metaColumns("numbers")
            print()
            metadata_col = None
            while metadata_col is None:
                metadata_col = input("Select a metadata column: ")
                metadata_col = _verifyInputIntegrity(metadata_col)
            if metadata_col == -1: return links
            data_val = self.df.columns[data_col]
            metadata_val = self.meta.columns[metadata_col]
            links[data_val] = metadata_val
            iters -= 1
        return links
