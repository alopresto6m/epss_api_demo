import time
from typing import TextIO, Optional

import numpy as np
import pandas as pd


def read_file(f: TextIO, rows: Optional[int] = None):
    cpes_csv = pd.read_csv(f, header=0, nrows=rows)

    print(f"Count of elements: {len(cpes_csv['cpe_id'])}")
    return cpes_csv


def main():
    filename = 'nistCPEFormatted.csv'
    rows = 10 #None

    with open(f"/Users/andylopresto/Downloads/{filename}") as csv_file:
        df = read_file(csv_file, rows)
        print('Finished loading file')
        print(df.count())

        # Clean up the exploded column data (nan -> '-')
        # version_cols = ['version_start_including', 'version_start_excluding', 'version_end_including', 'version_end_excluding']
        df.exploded_version = df.exploded_version.replace({np.nan: '-'})
        df.exploded_update = df.exploded_update.replace({np.nan: '*'})

        # Clean the timestamp data
        print(f"Empty last_modified values: {pd.isna(df.last_modified)}")
        print(f"Empty published values: {pd.isna(df.published)}")

        # Ensure there are no null LM values (and sub now if there are)
        if pd.isna(df.last_modified).sum() > 0:
            df.last_modified = df.last_modified.fillna(time.time())

        # Fill any null published with the match LM
        if pd.isna(df.published).sum() > 0:
            df.published = df.published.fillna(df.last_modified)

        assert pd.isna(df.last_modified).sum() == 0
        assert pd.isna(df.published).sum() == 0

        # Drop the unnecessary columns
        # df = df.drop(['criteria', 'matches'], axis=1)

        # Modify the status column to active and remove status
        # df['active'] = df['status'].apply(lambda s: s == 'Active')
        # df = df.drop('status', axis=1)

        with pd.option_context('display.max_rows', 2, 'display.max_columns', 15):
            print(df)

    output_rows = rows if rows is not None else len(df.index)
    tag = '_' + str(output_rows) if rows is not None else ''
    output_filename = f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/cpes_formatted{tag}.csv"
    df.to_csv(output_filename, index=False)
    print(f"Wrote {output_rows} rows to {output_filename}")


if __name__ == '__main__':
    main()
