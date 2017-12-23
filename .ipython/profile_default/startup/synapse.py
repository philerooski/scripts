import synapseclient as sc
import pandas as pd

SANDBOX = 'syn11611056' # where to stash file views

def synread(query, syn_=None, header='infer', silent=False):
    """ Instantly read a variety of Synapse objects as a pandas DataFrame.

    Parameters
    ----------
    query : str, list
        A Synapse ID or query such as you would pass to Synapse.tableQuery.
        Can also be a list of Synapse IDs, which will return a list
        of DataFrames.
    syn_ : synapseclient.Synapse
        If there is a variable named `syn` in your global namespace,
        then `synread` will assume it is a Synapse object it can use
        to interact with Synapse.
    header : str
        Line number of header file, if any. Specify `None` to indicate there
        is no header. Only applicable when passing a Synapse ID of a delimited
        file, not a table, file view, or query.
    silent : bool
        Whether to print output to the console.

    Returns
    -------
    pandas.DataFrame
    """
    if 'syn' in globals() and syn_ is None: syn_ = syn
    elif syn_ is None:
        raise NameError("syn object not found. "
        "Please establish a Synapse session.")
    if isinstance(query, str):
        if query.lower().startswith("select"):
            q = syn_.tableQuery(query)
            d = q.asDataFrame()
        else:
            f = syn_.get(query)
            d = _synread(query, f, syn_, header, silent)
        _preview(d, silent)
    elif isinstance(query, list): # is list-like
        files = list(map(syn_.get, query))
        if all([isinstance(f, sc.entity.Folder) for f in files]):
            d = _synread(query, files, syn_, header, silent)
            _preview(d, silent)
        else:
            d = [_synread(query, f, syn_, header, silent) for f in files]
            if not silent:
                print("Read in {} files.".format(len(d)))
    else:
        raise TypeError("synread cannot read type of {}".format(query))
    return d

def _synread(query, f, syn_, header, silent):
    """ Helper function. See `synread`. """
    if isinstance(f, sc.entity.File):
        if f.path is None:
            d = None
        else:
            d = pd.read_csv(f.path, header=header, sep=None, engine="python")
    elif isinstance(f, (sc.table.EntityViewSchema, sc.table.Schema)):
        q = syn_.tableQuery("select * from %s" % query)
        d = q.asDataFrame();
    elif isinstance(f, (sc.entity.Folder, sc.entity.Project)) or isinstance(
            f, list):
        scope = [_.id for _ in f] if isinstance(f, list) else [f.id]
        preexisting = syn.getChildren(
                SANDBOX,
                includeTypes=['entityview'],
                sortBy='CREATED_ON',
                sortDirection='DESC')
        d = None
        scopeSet = set(scope)
        for ev in preexisting:
            names = set(ev['name'].split("+"))
            if scopeSet.issubset(names) and scopeSet.issuperset(names):
                d = synread(ev['id'], syn_, header, silent=True)
        if d is None:
            import json
            name = "+".join(scope)
            if not silent:
                print("Creating file view...")
            params = {'scope': scope, 'viewType': 'file'}
            cols = syn_.restPOST('/column/view/scope',
                                     json.dumps(params))['results']
            cols = [sc.Column(**c) for c in cols]
            schema = sc.EntityViewSchema(name=name, columns=cols,
                    parent=SANDBOX, scopes=scope)
            schema = syn_.store(schema)
            d = synread(schema.id, syn_, header, silent=True)
    else:
        raise TypeError("synread cannot read type of {}: {}".format(
            query, type(f)))
    return d

def _preview(d, silent):
        if not silent:
            if hasattr(d, 'head'): print(d.head())
            if hasattr(d, 'shape'): print("Full size:", d.shape)
