import argparse
import pandas as pd

def read_args():
    parser = argparse.ArgumentParser(description="merge csv, table")
    parser.add_argument('outputFile', help='name of output')
    parser.add_argument('filetype', help='csv, table, etc')
    parser.add_argument('files', nargs='+')
    args = parser.parse_args()
    return args.outputFile, args.filetype, args.files

def merge_files(filetype, files):
    if filetype == "csv":
        dfs = [pd.read_csv(f, header=0) for f in files]
    elif filetype == "table":
        dfs = [pd.read_table(f, header=0) for f in files]
    else:
        raise ValueError("Unrecognized file type: %s" % filetype)
    return pd.DataFrame().append(dfs).reset_index(drop=True)

def main():
    output_file, filetype, files = read_args()
    merged_dfs = merge_files(filetype, files)
    merged_dfs.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()
