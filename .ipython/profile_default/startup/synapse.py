import synapseclient as sc
import pandas as pd

SANDBOX = 'syn11611056' # where to stash file views

def synread(synId, syn_=None, header='infer', silent=False):
    if 'syn' in globals() and syn_ is None: syn_ = syn
    elif syn_ is None:
        raise NameError("syn object not found. "
        "Please establish a Synapse session.")
    if isinstance(synId, str):
        f = syn_.get(synId)
        d = _synread(synId, f, syn_, header, silent)
        if not silent:
            if hasattr(d, 'head'): print(d.head())
            if hasattr(d, 'shape'): print("Full size:", d.shape)
    else: # is list-like
        files = map(syn_.get, synId)
        if all([isinstance(f, sc.entity.Folder) for f in files]):
            d = _synread(synId, files, syn_, header, silent)
        else:
            d = [_synread(synId, f, syn_, header, silent) for f in files]
            if not silent:
                print("Read in {} files.".format(len(d)))
    return d

def _synread(synId, f, syn_, header, silent):
    if isinstance(f, sc.entity.File):
        if f.path is None:
            d = None
        else:
            d = pd.read_csv(f.path, header=header, sep=None, engine="python")
    elif isinstance(f, (sc.table.EntityViewSchema, sc.table.Schema)):
        q = syn_.tableQuery("select * from %s" % synId)
        d = q.asDataFrame();
    elif isinstance(f, (sc.entity.Folder, sc.entity.Project)) or isinstance(
            f, list):
        import datetime, json
        if not silent:
            print("Creating file view...")
        scope = [_.id for _ in f] if isinstance(f, list) else [f.id]
        params = {'scope': scope, 'viewType': 'file'}
        cols = syn_.restPOST('/column/view/scope',
                                 json.dumps(params))['results']
        cols = [sc.Column(**c) for c in cols]
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
        schema = sc.EntityViewSchema(name=now, columns=cols,
                parent=SANDBOX, scopes=scope)
        schema = syn_.store(schema)
        d = synread(schema.id, syn_, header, silent=True)
    else:
        raise Exception("synread cannot read type of {}: {}".format(
            synId, type(f)))
    return d
