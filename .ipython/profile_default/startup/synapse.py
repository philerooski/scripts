import synapseclient as sc
import pandas as pd

SANDBOX = 'syn11611056' # where to stash file views

def synread(synId, syn_=None):
    if 'syn' in globals() and syn_ is None: syn_ = syn
    elif syn_ is None:
        raise NameError("syn object not found. "
        "Please establish a Synapse session.")
    if isinstance(synId, str):
        f = syn_.get(synId)
        d = _synread(synId, f, syn_)
        if hasattr(d, 'head'): print(d.head())
        if hasattr(d, 'shape'): print("Full size:", d.shape)
    else: # is list-like
        files = map(syn_.get, synId)
        if all([isinstance(f, sc.entity.Folder) for f in files]):
            d = _synread(synId, files, syn_)
        else:
            d = [_synread(synId, f, syn_) for f in files]
            print("Read in {} files.".format(len(d)))
    return d

def _synread(synId, f, syn_):
    if isinstance(f, sc.entity.File):
        if f.path is None:
            d = None
        else:
            d = pd.read_csv(f.path, header="infer", sep=None, engine="python")
    elif isinstance(f, (sc.table.EntityViewSchema, sc.table.Schema)):
        q = syn_.tableQuery("select * from %s" % synId)
        d = q.asDataFrame();
    elif isinstance(f, sc.entity.Folder) or isinstance(f, list):
        import datetime, json
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
        d = synread(schema.id, syn_)
    else:
        raise Exception("_synread cannot read type of {}".format(synId))
    return d
