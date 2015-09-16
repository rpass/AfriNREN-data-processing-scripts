'''
Taking in the flow aggregated output file,
Returning a series of different json outputs.

Tutorial followed:
http://pythonprogramming.net/pandas-column-manipulation-spreadsheet-data/?completed=/pandas-saving-reading-csv-file/
'''
import pandas as pd
from pandas import DataFrame
import datetime
# import pandas.io.data

INPUT_CSV_DIR = '/Users/rob/dev/AfriNREN-NetFLOW-Scripts'
INPUT_CSV_FILE = 'flow-aggregated-data.csv'
INPUT_FILE_PATH = '%s/%s' % (INPUT_CSV_DIR, INPUT_CSV_FILE)
OUTPUT_FILE_PATH = '/Users/rob/dev/AfriNREN-NetFLOW-Scripts/%s'


def changeToNum(numstr):
    if(numstr[-1] == 'M'):
        numstr = float(numstr[:-2]) * 1000000
    elif(numstr[-1] == 'G'):
        numstr = float(numstr[:-2]) * 1000000000
    return int(numstr)


def convertStrBytesToIntBytes():
    df = pd.read_csv(INPUT_FILE_PATH, parse_dates=[1])
    df['Bytes'] = [changeToNum(numstr) for numstr in df['Bytes']]
    df['Bytes'] = df['Bytes'].astype('int')
    # df['Dst Pt'] = df['Dst Pt'].astype('int')
    print(df.info())
    df.to_csv(OUTPUT_FILE_PATH % 'flow-aggregated-data.csv')
    print('Changed text byte format to int format')


def produceASaggregatedData():
    print('Aggregating by src and then dst ASN')
    df = pd.read_csv(INPUT_FILE_PATH, parse_dates=[1])
    df = df[['src_ASN', 'dst_ASN', 'Bytes']]
    df_grouped = df.groupby(['src_ASN', 'dst_ASN']).sum()
    print(df_grouped[0:10])
    df_grouped.to_csv(OUTPUT_FILE_PATH % 'data.csv')


def produceGeneralAggregatedData():
    print('Aggregating by date, src and then protocol and counting by bytes')
    df = pd.read_csv(INPUT_FILE_PATH, parse_dates=[1])
    df = df[['Date first seen', 'src_ASN', 'Src Pt', 'Bytes']]
    print(df.info())
    # get date to simpler date format
    df['Date first seen'] = [time.date() for time in df['Date first seen']]
    df_grouped = df.groupby(['Date first seen', 'src_ASN', 'Src Pt']).sum()
    print(df_grouped[0:10])
    df_grouped.to_csv(OUTPUT_FILE_PATH % 'gen-data.csv')


def getASstats():
    print('.... Statistics ....')
    df = pd.read_csv(INPUT_FILE_PATH, parse_dates=[1])
    unique_src_as = len(pd.unique(df.src_ASN.ravel()))
    unique_dst_as = len(pd.unique(df.dst_ASN.ravel()))
    bytes_count = df.Bytes.sum()
    print('''No. of unique:\nSrc ASes: %d,
Dst ASes: %d,
Total Bytes: %d''' % (unique_src_as, unique_dst_as, bytes_count))


def getTopStats():
    top_src_ip = ''
    top_dst_ip = ''
    top_src_prt = ''
    top_dst_prt = ''
    top_dst_AS = ''
    top_src_AS = ''
    headers = ['Date first seen', 'Duration']
    print('Fetching top stats...')
    df = pd.read_csv(INPUT_FILE_PATH, parse_dates=True)
    df_grouped = df[['Src IP Addr', 'Bytes']]

if __name__ == '__main__':
    print('Processing data file...')
    # ----------- ----------------------
    # df = pd.read_csv(INPUT_FILE_PATH, parse_dates=True)
    # # print(df.head())
    # AS_df = df[['src_ASN', 'dst_ASN', 'Bytes']]
    # AS_df['Date first seen'] = df['Date first seen'].astype('datetime64[ns]')
    # AS_df['Date first seen']= [time.date() for time in df['Date first seen']]
    # # changing the bytes to numbers
    # AS_df['Bytes'] = [changeToNum(numstr) for numstr in df['Bytes']]
    # df_out = AS_df[0:300]
    # df_out.to_csv(OUTPUT_FILE_PATH % 'data.csv')
    # ----------- ----------------------
    if False:
        convertStrBytesToIntBytes()
        produceASaggregatedData()
    produceGeneralAggregatedData()
