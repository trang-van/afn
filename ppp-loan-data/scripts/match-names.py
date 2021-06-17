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
from cleanco import cleanco

# Get Current Working Directory and Parent Path (for reading files in different folders)
PATH = dirname(dirname(abspath(os.getcwd())))

# TODO: Change files by modifying the string to match path to file.
orbis_file = PATH + '/ppp-loan-data/Orbis database_six cities only.csv'
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


def read_files(files):
    df1 = pd.read_csv(files[0])
    df2 = pd.read_csv(files[1])
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
    Get the PPP Loan Data of businesses that appear in the Orbis database.
    :param df1:
    :param df2:
    :param col1:
    :param col2:
    :return:
    '''
    return df2[df2[col2].isin(df1[col1])]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", "-v", type=int, default=1, help="Specify which files you want to run.")
    args = parser.parse_args()

    if args.version == 1:
        print("running version 1 (Orbis and PPP Data -All)...")
        files = [orbis_file, ppp_file]
        orbis_df, ppp_df = read_files(files)
        cleanco_check()
        orbis_df = clean_names(orbis_df, 'Company name Latin alphabet', 'Company Name - Clean')
        ppp_df = clean_names(ppp_df, 'BorrowerName', 'BorrowerName - Clean')
        print("finished cleaning names.")
        print("getting matched names...")

        matched_df = match_names(orbis_df, ppp_df, 'Company Name - Clean', 'BorrowerName - Clean')
        matched_df.to_csv(PATH+'/ppp-loan-data/out/orbis_ppp_all_bay.csv')
        print("done.")
    elif args.version == 2:
        print("running version 2 (Orbis and PPP Data -Minority)...")
        files = [orbis_file, ppp_minority_file]
        orbis_df, ppp_minority_df = read_files(files)
        cleanco_check()
        orbis_df = clean_names(orbis_df, 'Company name Latin alphabet', 'Company Name - Clean')
        ppp_minority_df = clean_names(ppp_minority_df, 'BorrowerName', 'BorrowerName - Clean')
        print("finished cleaning names.")
        print("getting matched names...")

        matched_df = match_names(orbis_df, ppp_minority_df, 'Company Name - Clean', 'BorrowerName - Clean')
        matched_df.to_csv(PATH+'/ppp-loan-data/out/orbis_ppp_bay_minority.csv')
        print("done.")

else:
    raise ValueError("Enter valid version (1 or 2)")
