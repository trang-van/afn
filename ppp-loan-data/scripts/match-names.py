'''
Match Business Names in Two Datasets - Orbis and PPP Loan

Author: Trang Van

Instructions:

1. Modify file paths to match where the following files are on your computer:
    Orbis database_six cities only.csv
    bay_bus_from_ppp.csv
    bay_bus_from_ppp_minority.csv
Note: PATH will get the parent directory,
        if files are in parent directory, don't need to append another path (make string empty)

2. In terminal, run python match-names.py -v 1 for matching of all Bay Area businesses
                run python match-names.py -v 2 for matching of only minority Bay Area businesses

'''
import os
from os.path import dirname, abspath
import argparse
import pandas as pd
import numpy as np
from cleanco import cleanco

# Get Current Working Directory and Parent Path (for reading files in different folders)
PATH = dirname(dirname(abspath(os.getcwd())))

# TODO: Change files by modifying the string to match path to file.
# Business Databases
orbis_file = PATH + '/ppp-loan-data/Orbis database_six cities only.csv'
red_rich_file = PATH + '/data-to-match/Business_database_Redwood_Richmond.csv'
south_sf_file = PATH + '/data-to-match/Business_database_South_SF.csv'
sj_file = PATH + '/data-to-match/Business_database_San_Jose.csv'
oakland_file = PATH + '/data-to-match/Business_database_Oakland_extended.csv'

# PPP Loan Files
ppp_file = PATH + '/ppp-loan-data/out/bay_bus_from_ppp.csv'
ppp_minority_file = PATH + '/ppp-loan-data/out/bay_bus_from_ppp_minority.csv'


def cleanco_check():
    '''
    Run sample of cleanco's functions to make sure everything is installed and running
    as expected
    '''
    bus_name = 'RISTORANTE BUON GUSTO, INC'
    bus_name1 = 'INDELICATO INC.'
    bus_name2 = 'INDELICATO INC'

    # Pass string into cleanco object
    x = cleanco(bus_name)
    y = cleanco(bus_name1)
    z = cleanco(bus_name2)

    # Use function .clean_name() for the business name without company abbreviations
    assert x.clean_name() == 'RISTORANTE BUON GUSTO'
    assert y.clean_name() == 'INDELICATO'
    assert z.clean_name() == 'INDELICATO'

    print('all tests passed.')


def read_files(files, encoding1='utf-8', encoding2='utf-8'):
    df1 = pd.read_csv(files[0], encoding=encoding1, engine='python')
    df2 = pd.read_csv(files[1], encoding=encoding2, engine='python')
    return df1, df2


def clean_names(df, col, new_col):
    '''
    Use cleanco's clean_name function to remove company related abbreviations in names.
    :param df:
    :param col: column to clean
    :param new_col: cleaned column
    :return: dataframe with new column
    '''
    df[new_col] = df[col].apply(str).apply(lambda x: cleanco(x).clean_name())
    return df


def match_names(df1, df2, col1, col2):
    '''
    Get the PPP Loan Data of businesses that appear in the main database.
    :param df1:
    :param df2:
    :param col1:
    :param col2:
    :return:
    '''
    return df2[df2[col2].isin(df1[col1])]


def extract_columns(df, columns):
    '''
    Get columns from dataframe (eg. PPP Loan datasets)
    :param df:
    :param columns:
    :return:
    '''
    df.columns = df.columns.str.replace(' ', '')
    return df[columns].reset_index()


def merge_df(df1, df2, on_col1, on_col2):
    '''
    Merge two data frames on two different columns, if same column, pass in same parameter twice
    :param df1:
    :param df2:
    :param on_col1:
    :param on_col2:
    :return:
    '''
    return df1.merge(df2, how='left', left_on=on_col1, right_on=on_col2)


def change_headers(df, header_row_idx=2):
    '''
    Additional processing on Business database from team
    :param df:
    :param header_row_idx:
    :return:
    '''
    df.columns = df.iloc[header_row_idx]
    df.drop(np.arange(header_row_idx + 1), inplace=True)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", "-v", type=str, default='orbis-all',
                        help="Specify which files you want to run. Options: orbis-all, orbis-minoirty, oakland, red-rich, south-sf, sj")
    args = parser.parse_args()

    # TODO: Change `columns` as needed to extract necessary PPP Loan information
    PPP_COLS = ['BorrowerName-Clean', 'CurrentApprovalAmount', 'Race', 'Ethnicity', 'Minority', 'NAICS_4', 'IndustrySubsector']

    if args.version == 'orbis-all':
        '''
        Outputs file containing all PPP loan information (same fields as PPP file) and
        only gets the rows that are in the Orbis database.
        '''
        print("running Orbis and PPP Data -All...")
        files = [orbis_file, ppp_file]
        orbis_df, ppp_df = read_files(files)
        cleanco_check()
        orbis_df = clean_names(orbis_df, 'Company name Latin alphabet', 'Company Name - Clean')
        ppp_df = clean_names(ppp_df, 'BorrowerName', 'BorrowerName - Clean')
        print("finished cleaning names.")
        print("getting matched names...")

        matched_df = match_names(orbis_df, ppp_df, 'Company Name - Clean', 'BorrowerName - Clean')
        matched_df.to_csv(PATH + '/ppp-loan-data/out/orbis_ppp_all_bay.csv')
        print("done.")
    elif args.version == 'orbis-minority':
        '''
        Outputs file containing all PPP loan information (same fields as PPP file) that are from minority-owned businesses and
        only gets the rows that are in the Orbis database.
        '''
        print("running Orbis and PPP Data -Minority...")
        files = [orbis_file, ppp_minority_file]
        orbis_df, ppp_minority_df = read_files(files)
        cleanco_check()
        orbis_df = clean_names(orbis_df, 'Company name Latin alphabet', 'Company Name - Clean')
        ppp_minority_df = clean_names(ppp_minority_df, 'BorrowerName', 'BorrowerName - Clean')
        print("finished cleaning names.")
        print("getting matched names...")

        matched_df = match_names(orbis_df, ppp_minority_df, 'Company Name - Clean', 'BorrowerName - Clean')
        matched_df.to_csv(PATH + '/ppp-loan-data/out/orbis_ppp_bay_minority.csv')
        print("done.")
    elif args.version == 'merge-orbis-ppp':
        '''
        Output file that merges Orbis database with PPP Loan Data with specific columns.
        Businesses in the database with no PPP Loan Information will have NaN values in those fields.
        '''
        PPP_COLS_ORBIS = ['BorrowerName-Clean', 'CurrentApprovalAmount', 'Race', 'Ethnicity', 'Minority', 'NAICS_4',
                    'IndustrySubsector']

        files = [orbis_file, PATH + '/ppp-loan-data/out/orbis_ppp_all_bay.csv']
        orbis_df, ppp_from_orbis = read_files(files)
        ppp_from_orbis.columns = ppp_from_orbis.columns.str.strip()
        ppp_from_orbis_extracted = extract_columns(ppp_from_orbis, PPP_COLS_ORBIS)
        orbis_df = clean_names(orbis_df, 'Company name Latin alphabet', 'Company Name - Clean')

        merge_df_orbis = merge_df(orbis_df, ppp_from_orbis_extracted, 'Company Name - Clean', 'BorrowerName-Clean')
        merge_df_orbis.drop(columns=['index'], inplace=True)
        merge_df_orbis.to_csv(PATH + '/ppp-loan-data/out/merged_cities/orbis_ppp_merge.csv')
    elif args.version == 'merge-oakland-ppp':
        '''
        Output file that merges Oakland database with PPP Loan information
        
        File was
        '''
        # Source-Encoding: https://stackoverflow.com/questions/21504319/python-3-csv-file-giving-unicodedecodeerror-utf-8-codec-cant-decode-byte-err
        print("running Oakland and PPP Data Merge...")
        files = [oakland_file, ppp_file]
        oakland_df, ppp_df = read_files(files, 'mac_roman', 'utf-8')
        cleanco_check()

        oakland_df = clean_names(oakland_df, 'Company name Latin alphabet', 'Company Name - Clean')
        ppp_df = clean_names(ppp_df, 'BorrowerName', 'BorrowerName - Clean')
        ppp_extracted = extract_columns(ppp_df, PPP_COLS)
        print("finished cleaning names.")

        print("merging...")

        merge_df_oakland = merge_df(oakland_df, ppp_extracted, 'Company Name - Clean', 'BorrowerName-Clean')
        merge_df_oakland.to_csv(PATH + '/ppp-loan-data/out/merged_cities/oakland_ppp_merge.csv')
        print("done.")
    elif args.version == 'merge-red-rich':
        '''
        Output file that merges Redwood City and Richmond database with PPP Loan information
        '''

        print("running Redwood City + Richmond and PPP Data Merge...")

        files = [red_rich_file, ppp_file]
        red_rich_df, ppp_df = read_files(files)

        red_rich_df = change_headers(red_rich_df)
        red_rich_df.dropna(axis='columns', how='all', inplace=True)
        red_rich_df.dropna(axis='rows', how='all', inplace=True)

        cleanco_check()
        red_rich_df = clean_names(red_rich_df, 'Company name Latin alphabet', 'Company Name - Clean')
        ppp_df = clean_names(ppp_df, 'BorrowerName', 'BorrowerName - Clean')
        print("finished cleaning names.")
        ppp_extracted = extract_columns(ppp_df, PPP_COLS)

        print("merging...")

        merge_df_red_rich = merge_df(red_rich_df, ppp_extracted, 'Company Name - Clean', 'BorrowerName-Clean')
        merge_df_red_rich.to_csv(PATH + '/ppp-loan-data/out/merged_cities/redwood_richmond_ppp_merge.csv')
        print("done.")
    elif args.version == 'merge-south-sf':
        '''
        Output file that merges South SF database with PPP Loan information
        '''
        print("running South SF and PPP Data Merge...")

        files = [south_sf_file, ppp_file]
        south_sf_df, ppp_df = read_files(files)
        south_sf_df = change_headers(south_sf_df)
        south_sf_df.dropna(axis='columns', how='all', inplace=True)
        south_sf_df.dropna(axis='rows', how='all', inplace=True)
        cleanco_check()
        south_sf_df = clean_names(south_sf_df, 'Company name Latin alphabet', 'Company Name - Clean')
        ppp_df = clean_names(ppp_df, 'BorrowerName', 'BorrowerName - Clean')
        print("finished cleaning names.")
        print("merging...")
        ppp_extracted = extract_columns(ppp_df, PPP_COLS)
        merge_df_south_sf = merge_df(south_sf_df, ppp_extracted, 'Company Name - Clean', 'BorrowerName-Clean')
        merge_df_south_sf.to_csv(PATH + '/ppp-loan-data/out/merged_cities/south_sf_ppp_merge.csv')
        print("done.")
    elif args.version == 'merge-sj':
        '''
        Output file that merges San Jose database with PPP Loan information
        '''
        print("running SJ and PPP Data Merge...")
        files = [sj_file, ppp_file]
        sj_df, ppp_df = read_files(files)

        sj_df = change_headers(sj_df)
        sj_df.dropna(axis='columns', how='all', inplace=True)
        sj_df.dropna(axis='rows', how='all', inplace=True)

        cleanco_check()
        sj_df = clean_names(sj_df, 'Company name Latin alphabet', 'Company Name - Clean')
        ppp_df = clean_names(ppp_df, 'BorrowerName', 'BorrowerName - Clean')
        print("finished cleaning names.")

        print("merging...")
        ppp_extracted = extract_columns(ppp_df, PPP_COLS)

        merge_df_sj = merge_df(sj_df, ppp_extracted, 'Company Name - Clean', 'BorrowerName-Clean')
        merge_df_sj.to_csv(PATH + '/ppp-loan-data/out/merged_cities/sj_ppp_merge.csv')
        print("done.")
else:
    raise ValueError("Enter valid option. See help for options.")
