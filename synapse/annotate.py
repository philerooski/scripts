import pandas as pd
import synapseclient as sc
import json
import re

def synread(syn, synId):
    f = syn.get(synId)
    try:
       d = pd.read_csv(f.path, header="infer", sep=None, engine="python")
    except AttributeError: # f is probably a table
        q = syn.tableQuery("select * from %s" % synId)
        d = q.asDataFrame();
    return d

def newColFromRegex(df, col, regex, name='colToMatchOn'):
    df = df.copy(deep=True)
    p = re.compile(regex)
    newCol = []
    for s in df[col].values:
        m = p.search(s)
        assert m, "{} does not match regex.".format(s)
        newCol.append(m.group())
    return newCol

def addFromFile(toDf, fromFile):
    fromList = pd.read_csv(fromFile, header=None).values
    toDf = toDf.copy(deep=True)
    for k, v in fromList:
        toDf[k] = v
    return toDf

def _fileViewColsFromFile(fromFile):
    f = pd.read_csv(fromFile, header=None)
    keys = f[0].values
    values = f[1].values
    val_length = map(len, values)
    cols = [{'name':k, 'maximumSize':l, 'columnType':"STRING", "defaultValue":v}
            for k, v, l in zip(keys, values, val_length)]
    return cols

def newFileView(name, parent, scope, additionalCols=[], colsFromFile=None):
    syn = sc.login()
    params = {'scope': scope, 'viewType': 'file'}
    cols = syn.restPOST('/column/view/scope', json.dumps(params))['results']
    if colsFromFile: cols += _fileViewColsFromFile(colsFromFile)
    cols += [{'name':n,'maximumSize':50,'columnType':'STRING'}
            for n in additionalCols]
    cols = [sc.Column(**c) for c in cols]
    schema = sc.EntityViewSchema(name=name, columns=cols, parent=parent, scopes=scope)
    schema = syn.store(schema)
    df = synread(syn, schema.id)
    if colsFromFile:
        df = addFromFile(df, colsFromFile)
        syn.store(sc.Table(schema.id, df))
    return df, schema
