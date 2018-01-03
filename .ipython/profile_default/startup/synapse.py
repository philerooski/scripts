import synapseclient as sc
import pandas as pd

SANDBOX = 'syn11611056' # where to stash file views

def synread(query=None, syn_=None, silent=False, **kwargs):
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
    silent : bool
        Whether to print output to the console.
    kwargs :
        Other arguments accepted by pandas.read_csv. Only applies when
        reading in .csv, .tsv, or other tabular data from a single file.
        If you pass in a list of Synapse IDs, the kwargs will be applied
        to each of the individual IDs. Does nothing when working with
        EntityView objects.

    Returns
    -------
    pandas.DataFrame
    """
    if 'syn' in globals() and syn_ is None: syn_ = syn
    elif syn_ is None:
        raise NameError("syn object not found. "
        "Please establish a Synapse session.")
    if query is None:
        query = pd.io.clipboard.clipboard_get()
    if isinstance(query, str):
        if query.lower().startswith("select"):
            try:
                q = syn_.tableQuery(query)
                d = q.asDataFrame()
            except sc.exceptions.SynapseHTTPError: # is actually a Folder
                # bit of a hack to avoid importing re
                startIndex = query.find("from syn")
                restOfQuery = query[startIndex+8:]
                endIndex = 0
                for i in range(1, len(restOfQuery)+1):
                    if restOfQuery[:i].isdigit():
                        endIndex += 1
                    else:
                        break
                scope = ["syn{}".format(restOfQuery[:endIndex])]
                actualId = _getCorrespondingEntityView(scope, syn_)
                if actualId is None: # We need to create an EntityView first
                    schema = _createEntityView(scope, syn_, silent)
                    actualId = schema.id
                actualQuery = query[:startIndex+5] + actualId + \
                        " " + restOfQuery[endIndex+1:]
                d = synread(actualQuery, syn_, silent=True, **kwargs)
        else:
            f = syn_.get(query)
            d = _synread(query, f, syn_, silent, **kwargs)
        _preview(d, silent)
    elif isinstance(query, list): # is list-like
        files = list(map(syn_.get, query))
        if all([isinstance(f, sc.entity.Folder) for f in files]):
            d = _synread(query, files, syn_, silent, **kwargs)
            _preview(d, silent)
        else:
            d = [_synread(query, f, syn_, silent, **kwargs) for f in files]
            if not silent:
                print("Read in {} files.".format(len(d)))
    else:
        raise TypeError("synread cannot read type of {}".format(query))
    return d


def _synread(query, f, syn_, silent, **kwargs):
    """ Helper function. See `synread`. """
    if isinstance(f, sc.entity.File):
        if f.path is None:
            d = None
        else:
            d = pd.read_csv(f.path, sep=None, engine="python", **kwargs)
    elif isinstance(f, (sc.table.EntityViewSchema, sc.table.Schema)):
        q = syn_.tableQuery("select * from %s" % query)
        d = q.asDataFrame();
    elif isinstance(f,
            (sc.entity.Folder, sc.entity.Project)) or isinstance(f, list):
        scope = [_.id for _ in f] if isinstance(f, list) else [f.id]
        actualId = _getCorrespondingEntityView(scope, syn_)
        if actualId: # there is already a preexisting entityView
            d = synread(actualId, syn_, silent=True, **kwargs)
        else: # we need to create an EntityView before return a DataFrame
            schema = _createEntityView(scope, syn_, silent)
            d = synread(schema.id, syn_, silent=True, **kwargs)
    else:
        raise TypeError("synread cannot read type of {}: {}".format(
            query, type(f)))
    return d


def _createEntityView(scope, syn_, silent):
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
    return schema


def _getCorrespondingEntityView(scope, syn_):
    preexisting = syn_.getChildren(
            SANDBOX,
            includeTypes=['entityview'],
            sortBy='CREATED_ON',
            sortDirection='DESC')
    actualId = None
    scopeSet = set(scope)
    for ev in preexisting:
        names = set(ev['name'].split("+"))
        if scopeSet.issubset(names) and scopeSet.issuperset(names):
            actualId = ev['id']
    return actualId


def _preview(d, silent):
        if not silent:
            if hasattr(d, 'head'): print(d.head())
            if hasattr(d, 'shape'): print("Full size:", d.shape)
