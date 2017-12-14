def pyread(path):
    d = pd.read_csv(path, header='infer', sep=None, engine='python')
    print(d.head())
    print("Full size:",d.shape)
    return d
