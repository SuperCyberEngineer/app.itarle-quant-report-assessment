#!/usr/bin/env python3

# from rx import create 
# from csv import DictReader
import pandas as pd
import hydra
import os

DEBUG = False
##############
# clean data #
##############

# load path from hydra config
DEBUG and print(__name__)
DEBUG and print(__file__)
DEBUG and print(os.getcwd())

# relative paths
def clean_csv(input_file, output_file):
    df = pd.read_csv(input_file, sep=',', header=71, na_values= '')
    df = df[df.columns[0:15]]
    df = df.replace('@1', '')
    #df = df.replace('', '')

    df.to_csv(output_file, index=False, header=False)
    pass

@hydra.main(version_base=None, config_path=os.getcwd(), config_name="env")
def app(config):
    print('{} = {}, {} = {}'.format('config', config, 'current dir.', os.getcwd()))
    input_file = './inputs/scandi.csv'
    print('start to clean the csv data')
    clean_csv(input_file, config['CLEANED_DATA_FILE']);
    # os.system('touch ./inputs/finished-cleaning; sleep 3; rm ./inputs/finished-cleaning;')
    os.system('touch ./inputs/finished-cleaning; sleep 3');
    pass

if __name__ == '__main__':
    print('start app ...')
    app()
