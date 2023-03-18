import time
from typing import TextIO, Optional

import numpy as np
import pandas as pd


def read_file(f: TextIO, rows: Optional[int] = None):
    match_criteria_csv = pd.read_csv(f, header=0, nrows=rows)

    print(f"Count of elements: {len(match_criteria_csv['match_criteria_id'])}")
    return match_criteria_csv


def main():
    filename = 'nistMATCHFormatted.csv'
    rows = None

    with open(f"/Users/andylopresto/Downloads/{filename}") as csv_file:
        df = read_file(csv_file, rows)
        print('Finished loading file')
        print(df.count())

        # Clean up the version data (nan -> NULL)
        # df = df.where(pd.notnull(df), None)
        version_cols = ['version_start_including', 'version_start_excluding', 'version_end_including', 'version_end_excluding']
        df[version_cols] = df[version_cols].replace({np.nan: 'NULL'})

        # Clean the timestamp data
        print(f"Empty last_modified values: {pd.isna(df.last_modified)}")
        print(f"Empty cpe_last_modified values: {pd.isna(df.cpe_last_modified)}")
        print(f"Empty created values: {pd.isna(df.created)}")

        # Ensure there are no null LM values (and sub now if there are)
        if pd.isna(df.last_modified).sum() > 0:
            df.last_modified = df.last_modified.fillna(time.time())

        # Fill any null CPE LM with the match LM
        if pd.isna(df.cpe_last_modified).sum() > 0:
            df.cpe_last_modified = df.cpe_last_modified.fillna(df.last_modified)

        # Fill any null created with the match LM
        if pd.isna(df.created).sum() > 0:
            df.created = df.created.fillna(df.last_modified)

        assert pd.isna(df.last_modified).sum() == 0
        assert pd.isna(df.cpe_last_modified).sum() == 0
        assert pd.isna(df.created).sum() == 0

        # Drop the unnecessary columns
        df = df.drop(['criteria', 'matches'], axis=1)

        # Modify the status column to active and remove status
        df['active'] = df['status'].apply(lambda s: s == 'Active')
        df = df.drop('status', axis=1)

        with pd.option_context('display.max_rows', 100, 'display.max_columns', 2):
            print(df)

        # sql_inserts = []
        # for i, r in df.iterrows():
        #     sql_inserts.append(
        #         'INSERT INTO match_criteria (' + str(', '.join(df.columns)) + ') VALUES ' + str(tuple(r.values)) + ';')

    # print('\n'.join(sql_inserts))

    output_rows = rows if rows is not None else len(df.index)
    tag = '_' + output_rows if rows is not None else ''
    output_filename = f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/match_criteria_formatted{tag}.csv"
    df.to_csv(output_filename, index=False)
    print(f"Wrote {output_rows} rows to {output_filename}")


if __name__ == '__main__':
    main()
