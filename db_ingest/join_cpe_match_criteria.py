import time
from typing import TextIO, Optional

import numpy as np
import pandas as pd


def read_file(f: TextIO, rows: Optional[int] = None):
    match_criteria_csv = pd.read_csv(f, header=0, nrows=rows)

    print(f"Count of elements: {len(match_criteria_csv['match_criteria_id'])}")
    return match_criteria_csv


def main():
    pd.options.display.max_columns = 5
    pd.options.display.width = 1000
    rows = None

    # Read the cpe file
    with open(f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/cpes_formatted.csv") as cpe_csv_file:
        cpe_df = pd.read_csv(cpe_csv_file, header=0)
        cpe_df.columns = cpe_df.columns.str.replace(' ', '')
        print('CPE:\n', cpe_df, '\n')
        print('Finished loading CPE file: ', len(cpe_df.index), '\n\n')

    # Read the mc file
    with open(
            f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/match_criteria_formatted.csv") as mc_csv_file:
        mc_df = pd.read_csv(mc_csv_file, header=0)
        mc_df.columns = mc_df.columns.str.replace(' ', '')
        print('MC:\n', mc_df, '\n')
        print('Finished loading MC file: ', len(mc_df.index), '\n\n')
        print('MC rows where active == False:\n', mc_df[mc_df['active']==False], '\n\n')
        mc_df = mc_df[mc_df['active']]
        print('MC (refined):\n', mc_df, '\n\n')

    # Read the join file
    with open(
            f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/cpe_match_criteria_formatted.csv") as join_csv_file:
        join_df = pd.read_csv(join_csv_file, header=0)
        join_df.columns = join_df.columns.str.replace(' ', '')
        print('Join:\n', join_df, '\n\n')
        print('Finished loading join file: ', len(join_df.index), '\n\n')

    # Check that every cpe_id from the join file is present in the cpe file
    validate_cpe_df = join_df.merge(cpe_df.drop_duplicates(), on=['cpe_id'], how='left', indicator=True)
    missing_cpe_id_df = validate_cpe_df[validate_cpe_df['_merge'] != 'both']
    print('Join has cpe_id missing from cpes:\n', missing_cpe_id_df, '\n\n')
    print('Unique mc_ids missing cpe_id:', missing_cpe_id_df.match_criteria_id.unique())
    if len(missing_cpe_id_df.match_criteria_id.unique()) > 0:
        join_df = join_df[~join_df.match_criteria_id.isin(missing_cpe_id_df.match_criteria_id.unique())]
        print(f"Dropped {len(missing_cpe_id_df.match_criteria_id.unique())} bad mc_ids from join table")

    # Check that every mc_id from the join file is present in the mc file
    validate_mc_df = join_df.merge(mc_df.drop_duplicates(), on=['match_criteria_id'], how='left', indicator=True)
    missing_mc_id_df = validate_mc_df[validate_mc_df['_merge'] != 'both']
    print('Join has mc_id missing from mc:\n', missing_mc_id_df, '\n\n')
    print('Unique cpe_ids missing mc_id:', missing_mc_id_df.cpe_id.unique())
    if len(missing_mc_id_df.cpe_id.unique()) > 0:
        join_df = join_df[~join_df.cpe_id.isin(missing_mc_id_df.cpe_id.unique())]
        print(f"Dropped {len(missing_mc_id_df.cpe_id.unique())} bad cpe_ids from join table")

    # Verify none of the values in the join table are NaN
    assert pd.isna(join_df).any().sum() == 0
    # exit(0)

    # with pd.option_context('display.max_rows', 100, 'display.max_columns', 2):
    #     print(df)

        # sql_inserts = []
        # for i, r in df.iterrows():
        #     sql_inserts.append(
        #         'INSERT INTO match_criteria (' + str(', '.join(df.columns)) + ') VALUES ' + str(tuple(r.values)) + ';')

    # print('\n'.join(sql_inserts))

    output_rows = rows if rows is not None else len(join_df.index)
    tag = '_' + output_rows if rows is not None else ''
    output_filename = f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/cpe_match_criteria_formatted{tag}.csv"
    join_df.to_csv(output_filename, index=False)
    print(f"Wrote {output_rows} rows to {output_filename}")


if __name__ == '__main__':
    main()
