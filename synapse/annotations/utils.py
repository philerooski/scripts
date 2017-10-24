import pandas as pd
import synapseclient as sc
import re

def synread(syn_, synId, sortCols=True):
    #if "syn" in globals(): syn_ = syn
    if isinstance(synId, str):
        f = syn_.get(synId)
        d = _synread(synId, f, syn_, sortCols)
    else: # is list-like
        files = list(map(syn_.get, synId))
        d = [_synread(synId_, f, syn_, sortCols) for synId_, f in zip(synId, files)]
    return d

def _synread(synId, f, syn_, sortCols):
    if isinstance(f, sc.entity.File):
        d = pd.read_csv(f.path, header="infer", sep=None, engine="python")
    elif isinstance(f, sc.table.EntityViewSchema):
        q = syn_.tableQuery("select * from %s" % synId)
        d = q.asDataFrame();
    if sortCols:
        return d.sort_index(1)
    else:
        return d

def convertClipboardToDict(sep=" "):
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
    return pd.concat(tabulars, axis=1, ignore_index=True).sort_index(1)

def makeColFromRegex(referenceList, regex):
    """ Return a list created by mapping a regular expression to another list.
    The regular expression must contain at least one capture group.

    Params
    ------
    referenceList : list-like
        A list to derive new values from.
    regex : str
        A regular expression to be applied.

    Returns
    -------
    newCol, list
        The list resulting from mapping `regex` to `referenceList`.
    """
    if regex == 'extension': regex = r"\.(\w+)(?:\.gz)?$"
    p = re.compile(regex)
    if not p.groups:
        raise RuntimeError("`regex` must have at least one capture group.")
    newCol = []
    for s in referenceList:
        m = p.search(s)
        if not m: print("{} does not match regex.".format(s))
        newCol.append(m.group(1)) if m else newCol.append(None)
    return newCol
