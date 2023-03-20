import re
import time
import uuid
from typing import TextIO, Optional

import numpy as np
import pandas as pd


def read_file(f: TextIO, rows: Optional[int] = None):
    match_criteria_csv = pd.read_csv(f, header=0, nrows=rows)

    print(f"Count of elements: {len(match_criteria_csv['match_criteria_id'])}")
    return match_criteria_csv


def gen_cve_id(df) -> str:
    (year, c) = re.findall(r'^CVE-(\d{4})-(\d{4,})$', df.cve_descriptor)[0]
    return uuid.UUID('cfe'.ljust(8, '0') + '-0000-4000-a000-' + year + c.rjust(8, 'a')).hex


def main():
    pd.options.display.max_columns = 5
    pd.options.display.width = 1000
    rows = None

    # Read the cve file
    with open(
            f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/cves_formatted.csv") as cve_csv_file:
        cve_df = pd.read_csv(cve_csv_file, header=0)
        cve_df.columns = cve_df.columns.str.replace(' ', '')
        print('CVE:\n', cve_df, '\n')
        print('Finished loading CVE file: ', len(cve_df.index), '\n\n')

    # Read the mc file
    with open(
            f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/match_criteria_formatted.csv") as mc_csv_file:
        mc_df = pd.read_csv(mc_csv_file, header=0)
        mc_df.columns = mc_df.columns.str.replace(' ', '')
        print('MC:\n', mc_df, '\n')
        initial_mc_rows = len(mc_df.index)
        print('Finished loading MC file: ', initial_mc_rows, '\n\n')
        print('MC rows where active == False:\n', mc_df[mc_df['active'] == False], '\n\n')
        dropped_inactive_rows = len(mc_df[mc_df['active'] == False].index)
        mc_df = mc_df[mc_df['active']]
        print('MC (refined):\n', mc_df, '\n\n')
        print(
            f"Total {initial_mc_rows} rows from mc table ({dropped_inactive_rows} total rows dropped; {len(mc_df.index)} remaining)")

    # Read the join file
    with open(
            f"/Users/andylopresto/Downloads/nistCVEFormattedJoin.csv") as join_csv_file:
        join_df = pd.read_csv(join_csv_file, header=0)
        join_df.columns = join_df.columns.str.replace(' ', '')
        print('Join:\n', join_df, '\n\n')
        print('Finished loading join file: ', len(join_df.index), '\n\n')

    initial_join_rows = len(join_df.index)

    # Check that every cve_descriptor from the join file is present in the cve file
    validate_cve_df = join_df.merge(cve_df.drop_duplicates(), on=['cve_descriptor'], how='left', indicator=True)
    missing_cve_descriptor_df = validate_cve_df[validate_cve_df['_merge'] != 'both']
    print('Join has cve_descriptor missing from cves:\n', missing_cve_descriptor_df, '\n\n')
    if len(missing_cve_descriptor_df.index) > 0:
        joins_without_cd_df = missing_cve_descriptor_df[missing_cve_descriptor_df['_merge'] != 'both']
        print('Missing records df:\n', joins_without_cd_df)
        mcs_and_cds_to_remove_df = joins_without_cd_df.filter(['match_criteria_id', 'cve_descriptor'], axis=1)
        print('Missing records df (2 columns):\n', mcs_and_cds_to_remove_df)
        dropped_mc_rows = len(mcs_and_cds_to_remove_df.index)
        print('Row count to drop:\n', dropped_mc_rows)

        # Merge the join and to_drop dataframes on the two columns and keep only the good ones
        join_df = pd.merge(join_df, cve_df, on=['cve_descriptor'], how='left', indicator=True).query(
            "_merge != 'left_only'").drop('_merge', axis=1).reset_index(drop=True)
        # Keep only the important columns
        join_df = join_df[['match_criteria_id', 'cve_descriptor']]
        print('Filtered join DF:\n', join_df)
        join_rows_after_cve_drop = len(join_df.index)
        print(
            f"Total rows in join table {initial_join_rows} ({dropped_mc_rows} total rows dropped; {join_rows_after_cve_drop} remaining)")

    # Check that every mc_id from the join file is present in the mc file
    validate_mc_df = join_df.merge(mc_df.drop_duplicates(), on=['match_criteria_id'], how='left', indicator=True)
    missing_mc_id_df = validate_mc_df[validate_mc_df['_merge'] != 'both']
    print('Join has match_criteria_id missing from mc:\n', missing_mc_id_df, '\n\n')
    if len(missing_mc_id_df.index) > 0:
        joins_without_mc_df = missing_mc_id_df[missing_mc_id_df['_merge'] != 'both']
        print('Missing records df:\n', joins_without_mc_df)
        mcs_and_cds_to_remove_df = joins_without_mc_df.filter(['match_criteria_id', 'cve_descriptor'], axis=1)
        print('Missing records df (2 columns):\n', mcs_and_cds_to_remove_df)
        dropped_mc_rows = len(mcs_and_cds_to_remove_df.index)
        print('Row count to drop:\n', dropped_mc_rows)

        # Merge the join and to_drop dataframes on the two columns and keep only the good ones
        join_df = pd.merge(join_df, mc_df, on=['match_criteria_id'], how='left', indicator=True).query(
            "_merge != 'left_only'").drop('_merge', axis=1).reset_index(drop=True)
        # Keep only the important columns
        join_df = join_df[['match_criteria_id', 'cve_descriptor']]
        print('Filtered join DF:\n', join_df)
        print(
            f"Total rows in join table {join_rows_after_cve_drop} ({dropped_mc_rows} total rows dropped; {len(join_df.index)} remaining)")

    # Add a constructed CVE ID
    join_df['cve_id'] = join_df.apply(gen_cve_id, axis=1)
    print('Join with constructed cve_id:\n', join_df, '\n\n')

    # Keep only the important columns
    join_df = join_df[['match_criteria_id', 'cve_id']]

    # Verify none of the values in the join table are NaN
    print('Join rows with null values:\n', pd.isna(join_df).any())
    assert pd.isna(join_df).any().sum() == 0

    # with pd.option_context('display.max_rows', 100, 'display.max_columns', 2):
    #     print(df)

    # sql_inserts = []
    # for i, r in df.iterrows():
    #     sql_inserts.append(
    #         'INSERT INTO match_criteria (' + str(', '.join(df.columns)) + ') VALUES ' + str(tuple(r.values)) + ';')

    # print('\n'.join(sql_inserts))

    output_rows = rows if rows is not None else len(join_df.index)
    tag = '_' + output_rows if rows is not None else ''
    output_filename = f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/cve_match_criteria_formatted{tag}.csv"
    join_df.to_csv(output_filename, index=False)
    print(f"Wrote {output_rows} rows to {output_filename}")


if __name__ == '__main__':
    main()
