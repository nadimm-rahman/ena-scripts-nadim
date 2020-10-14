#!/usr/bin/python3

# Script to obtain metadata

# INSTALLATION AND SETUP:
# Requires installation of cx_Oracle: https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html
# Along with this, the Oracle Instant Client is also required. Download this and then save the directory as an environment variable.
# Alternatively, provide the full path in the initialisation object in this script (edit the line with the following code) --> cx_Oracle.init_oracle_client(lib_dir=PATH_TO_CLIENT)
# Note for Mac: Workaround for security issues --> http://oraontap.blogspot.com/2020/01/mac-os-x-catalina-and-oracle-instant.html#:~:text=Developer%20Cannot%20be%20Verified&text=You%20can%20go%20to%20the,also%20has%20to%20be%20approved.

# Oracle Client EXAMPLE = '/Users/rahman/Downloads/instantclient_19_3'
import pandas as pd
import argparse, cx_Oracle, os, sys
from datetime import date
from getpass import getpass


def get_args():
    """
    Get arguments that are passed to the script
    :return: Arguments
    """
    parser = argparse.ArgumentParser(description="Script to obtain metadata for systematic assembly processing", formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-i', '--instrument_platform', type=str.upper, help='Instrument platform to obtain data on', required=True, choices=['OXFORD_NANOPORE', 'ILLUMINA'], nargs='?')
    parser.add_argument('-r', '--processed_runs', type=str, help='File containing run IDs for processed runs', required=True)
    args = parser.parse_args()
    return args


class MetadataFromDatabase:
    # Class object which handles obtaining metadata from ERAPRO database
    def __init__(self, sql_query):
        self.query = sql_query  # SQL query to obtain metadata

    def get_oracle_usr_pwd(self):
        """
        Obtain credentials to create an SQL connection
        :return: Username and password for a valid SQL database account
        """
        self.usr = input("Username: ")  # Ask for username
        self.pwd = getpass()  # Ask for password and handle appropriately

    def setup_connection(self):
        """
        Set up the database connection
        :return: Database connection object
        """
        # client_lib_dir = os.getenv('ORACLE_CLIENT_LIB')
        client_lib_dir = '/Users/rahman/Downloads/instantclient_19_3'
        if not client_lib_dir or not os.path.isdir(client_lib_dir):
            sys.stderr.write("ERROR: Environment variable $ORACLE_CLIENT_LIB must point at a valid directory\n")
            exit(1)
        cx_Oracle.init_oracle_client(lib_dir=client_lib_dir)
        self.connection = None
        try:
            dsn = cx_Oracle.makedsn("ora-vm-009.ebi.ac.uk", 1541,
                                    service_name="ERAPRO")  # Try connection to ERAPRO with credentials
            self.connection = cx_Oracle.connect(self.usr, self.pwd, dsn, encoding="UTF-8")
        except cx_Oracle.Error as error:
            print(error)

    def fetch_metadata(self):
        """
        Obtain metadata from ERAPRO database
        :return: Dataframe of metadata
        """
        self.get_oracle_usr_pwd()  # Obtain credentials from script operator
        self.setup_connection()  # Set up the database connection using the credentials
        if self.connection is not None:
            cursor = self.connection.cursor()
            search_query = cursor.execute(self.query)  # Query the database with the SQL query
            df = pd.DataFrame(search_query.fetchall())  # Fetch all results and save to dataframe
            return df


if __name__ == '__main__':
    args = get_args()
    today = date.today()
    date = today.strftime('%d%m%Y')

    sql_query = "SELECT proj.project_id, samp.sample_id, samp.biosample_id, exp.experiment_id, ru.run_id, proj.project_title, proj.project_name, samp.sample_title, exp.instrument_model, exp.library_layout, exp.library_name, exp.library_strategy, exp.library_source, extractValue(exp.experiment_xml,'//EXPERIMENT_SET/EXPERIMENT/DESIGN/DESIGN_DESCRIPTION'), extractValue(exp.experiment_xml, '//EXPERIMENT_SET/EXPERIMENT/DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_CONSTRUCTION_PROTOCOL') FROM project proj \
                        JOIN study stu ON (proj.project_id = stu.project_id) \
                        JOIN experiment exp ON (stu.study_id = exp.study_id) \
                        JOIN experiment_sample expsamp ON (exp.experiment_id = expsamp.experiment_id) \
                        JOIN sample samp ON (expsamp.sample_id = samp.sample_id) \
                        JOIN run ru ON (exp.experiment_id = ru.experiment_id) \
                        JOIN dcc_meta_key dmk on (proj.project_id = dmk.project_id) \
                            WHERE samp.tax_id=2697049 and exp.instrument_platform='{}' and ru.status_id=4 and dmk.meta_key='dcc_grusin'".format(args.instrument_platform)

    # Get project metadata
    print('> Obtaining all project metadata...')
    metadata = MetadataFromDatabase(sql_query)
    project_data = metadata.fetch_metadata()
    project_data.columns = ['project_id', 'sample_id', 'biosample_id', 'experiment_id', 'run_id', 'project_title', 'project_name', 'sample_title', 'instrument_model', 'library_layout', 'library_name', 'library_strategy', 'library_source', 'library_design_description', 'library_construction_protocol']
    print('> Obtaining all project metadata... COMPLETE.')

    # Get processed data
    print('> Reading in processed run data and obtaining full metadata...')
    processed = pd.read_csv(args.processed_runs, sep="\t", header=None, names=['run_id'])

    # Merge to obtain full metadata information on processed run data
    merged = pd.merge(project_data, processed, on='run_id', how='right')
    projects = merged.project_id.unique()
    print('-' * 100)
    print('Processed data projects: {}'.format(projects))
    print('-' * 100)
    merged.to_csv('data/{}_Metadata_{}.tsv'.format(args.instrument_platform, date), sep="\t", index=False)
    print('> {} runs have been processed. Metadata retrieved for {} runs.'.format(len(processed), len(merged)))
    print('> Reading in processed run data and obtaining full metadata... COMPLETE.')

