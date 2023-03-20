import re
import time
import uuid
from typing import TextIO, Optional

import numpy as np
import pandas as pd


def read_file(f: TextIO, rows: Optional[int] = None):
    cves_csv = pd.read_csv(f, header=0, nrows=rows)

    print(f"Count of elements: {len(cves_csv['cve_id'])}")
    return cves_csv


def gen_cve_id(df) -> str:
    (year, c) = re.findall(r'^CVE-(\d{4})-(\d{4,})$', df.cve_descriptor)[0]
    return uuid.UUID('cfe'.ljust(8, '0') + '-0000-4000-a000-' + year + c.rjust(8, 'a')).hex


def main():
    filename = 'nistCVEFormatted.csv'
    rows = None  # 1000

    with open(f"/Users/andylopresto/Downloads/{filename}") as csv_file:
        df = read_file(csv_file, rows)
        print('Finished loading file')
        print(df.count())

        # Drop any rows with vuln_status is bad
        print(f"All statuses: {df.vuln_status.unique()}")
        bad_statuses = ['Rejected', 'Awaiting Analysis', 'Undergoing Analysis', 'Deferred', 'Received']
        bad_status_df = df.vuln_status.str.contains('|'.join(bad_statuses))
        print('Bad statuses:\n', bad_status_df, '\n\n')
        df = df[~bad_status_df]
        # df = df[df.vuln_status != 'Rejected']

        with pd.option_context('display.max_rows', 10, 'expand_frame_repr', False):
            print(df)

        # Add a constructed CVE ID
        df.cve_id = df.apply(gen_cve_id, axis=1)

        # Ensure the score is present
        with pd.option_context('expand_frame_repr', False):
            print(df[(df.cvss_score.isna())])
        print(pd.isnull(df.cvss_score).sum())
        print(pd.isnull(df.cvss_score_level_id).sum())

        # Convert the CVSS score level ID from decimal to int
        df.cvss_score_level_id = df.cvss_score_level_id.apply(lambda x: int(x))

        # Ensure the CVSS vector is capitalized
        df.cvss_vector = df.cvss_vector.apply(lambda x: str(x).upper())

        # Clean up the CISA data
        cisa_cols = ['cisa_exploit_added_at', 'cisa_action_due_at', 'cisa_required_action', 'cisa_vulnerability_name']
        df[cisa_cols] = df[cisa_cols].replace({np.nan: None})

        # Scope values are CHANGED or UNCHANGED
        print("Unique scope values: ", df.scope.unique())
        df.scope = df.scope.replace({np.nan: 'UNCHANGED'})

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
        df = df.drop(['cisa_most_wanted'], axis=1)

        with pd.option_context('display.max_rows', 2, 'display.max_columns', 40):
            print(df)

    # exit(0)
    output_rows = rows if rows is not None else len(df.index)
    tag = '_' + str(output_rows) if rows is not None else ''
    output_filename = f"/Users/andylopresto/DataGripProjects/SixMap PGSQL System Design/ingest_data/cves_formatted{tag}.csv"
    df.to_csv(output_filename, index=False)
    print(f"Wrote {output_rows} rows to {output_filename}")


if __name__ == '__main__':
    main()
