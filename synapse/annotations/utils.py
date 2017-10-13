import pandas as pd
import synapseclient as sc
import re

def synread(syn_, synId):
    #if "syn" in globals(): syn_ = syn
    if isinstance(synId, str):
        f = syn_.get(synId)
        d = _synread(synId, f, syn_)
    else: # is list-like
        files = list(map(syn_.get, synId))
        d = [_synread(synId_, f, syn_) for synId_, f in zip(synId, files)]
    return d

def _synread(synId, f, syn_):
    if isinstance(f, sc.entity.File):
        d = pd.read_csv(f.path, header="infer", sep=None, engine="python")
    elif isinstance(f, sc.table.EntityViewSchema):
        q = syn_.tableQuery("select * from %s" % synId)
        d = q.asDataFrame();
    return d

def clipboardToDict(sep=" "):
    df = pd.read_clipboard(sep, header=None)
    d = {k: v for k, v in zip(df[0], df[1])}
    return d

def _keyValCols(keys, values, asSynapseCols):
    val_length = map(lambda v : len(v) if v else 50, values)
    cols = [{'name': k, 'maximumSize': l,
        'columnType': "STRING", "defaultValue": v}
            for k, v, l in zip(keys, values, val_length)]
    if asSynapseCols: cols = list(map(sc.Column, cols))
    return cols

def _colsFromFile(fromFile, asSynapseCols):
    f = pd.read_csv(fromFile, header=None)
    return _keyValCols(f[0].values, f[1].values, asSynapseCols)

def _colsFromDict(d, asSynapseCols):
    keys = [i[0] for i in d.items()]
    values = [i[1] for i in d.items()]
    return _keyValCols(keys, values, asSynapseCols)

def _colsFromList(l, asSynapseCols):
    keys = l
    values = [None for i in l]
    return _keyValCols(keys, values, asSynapseCols)

def makeColumns(obj, asSynapseCols=True):
    if isinstance(obj, str): return _colsFromFile(obj, asSynapseCols)
    elif isinstance(obj, dict): return _colsFromDict(obj, asSynapseCols)
    elif isinstance(obj, list): return _colsFromList(obj, asSynapseCols)

def combineSynapseTabulars(syn, tabulars):
    tabulars = synread(syn, tabulars)
    return pd.concat(tabulars, axis=1, ignore_index=True)

def makeColFromRegex(df, col, regex):
    p = re.compile(regex)
    newCol = []
    for s in df[col].values:
        m = p.search(s)
        if not m: print("{} does not match regex.".format(s))
        newCol.append(m.group()) if m else newCol.append(None)
    return newCol
