import argparse
from p_acquisitions import m_acquisitions
from p_analysis import m_analysis
from p_reporting import m_reporting
from p_wrangling import m_wrangling

def argument_parser():

    parser = argparse.ArgumentParser(description='specify input file and country...')
    parser.add_argument("-p", "--path", type=str, help='specify poll list ...')
    parser.add_argument("-c", "--country", type=str, help='specify a country ...')
    args = parser.parse_args()

    return args

def main(args):
    print('Starting pipeline...')

    m_acquisitions.get_dataframes(args.path)
    m_acquisitions.get_jobs(args.path)
    m_acquisitions.country_names()

    m_wrangling.wrangle()

    m_analysis.analyze_ch_1()

    print('Job done!!')
    print('You will find the result of challenge one on final_data folder')

if __name__ == '__main__':
    arguments = argument_parser()

    main(arguments)