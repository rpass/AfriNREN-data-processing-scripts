import csv
import subprocess

FLOW_AGG_DIR = '/Users/rob/dev/AfriNREN-NetFLOW-Scripts'
FLOW_AGG_FILE = 'flow-aggregate-kenet.out'
ORIGINAL_FLOW_AGG_PATH = FLOW_AGG_DIR + '/' + FLOW_AGG_FILE
EDITED_FLOW_AGG_PATH = ORIGINAL_FLOW_AGG_PATH[:-4] + '-edited.csv'
OUTPUT_DATA_FILE = FLOW_AGG_DIR + '/flow-aggregated-data.csv'
UNIQUE_IPS_FILE = ORIGINAL_FLOW_AGG_PATH[:-4] + '-unique-IPs'
ASN_LIST_FILE = 'ASNs.txt'
ASN_LIST_CLEANED_FILE = 'ASNs-cleaned.txt'
asn_dictionary = {}
unique_ips = []


def prepFlowAggFile():
    print('prepping flow-aggregate (%s) for further processing' % FLOW_AGG_FILE)
    with open(ORIGINAL_FLOW_AGG_PATH, 'r') as infile:
        with open(EDITED_FLOW_AGG_PATH, 'w') as outfile:
            # write field-names to header of csv file
            outfile.write('Date first seen,Duration,Src IP Addr,Src Pt,Dst IP Addr,Dst Pt,Proto,Flows,Bytes\n')
            # write each flow to output csv file
            for line in infile:
                if line[0:4] == '2015':
                    line = ','.join(map(lambda x: x.strip(), line.split(','))) + '\n'
                    outfile.write(line)
            print('Flow aggregate file edited successfully.')


def getUniqueIPs():
    # https://docs.python.org/3/library/csv.html
    # f = open('%s/%s' % (FLOW_AGG_DIR, filename))
    print('Getting unique ips from %s' % EDITED_FLOW_AGG_PATH[len(FLOW_AGG_DIR):])
    ips = []
    with open(EDITED_FLOW_AGG_PATH) as csvfile:
        reader = csv.DictReader(csvfile)
        # print(reader.keys())
        for row in reader:
            ips.append(row['Src IP Addr'])
            ips.append(row['Dst IP Addr'])
    unique_ips = list(set(ips))
    with open(UNIQUE_IPS_FILE, 'w') as fout:
        for ip in unique_ips:
            fout.write(ip.strip() + '\n')
    print('unique IP addresses found.')


def asnLookup():
    print('Looking up ASNs for ip addresses provided...')
    subprocess.call('sh geoip.sh %s/flow-aggregate-kenet-unique-ips > ASNs.txt'
                    % FLOW_AGG_DIR, shell=True)
    print('ASN lookup complete!')


def asnListCleanup():
    with open(ASN_LIST_FILE, 'r') as ASN_file:
        with open(ASN_LIST_CLEANED_FILE, 'w') as fout:
            # do something
            print('cleaning up ASN list.')
            for row in ASN_file:
                ip_and_asn = row.split(', GeoIP ASNum Edition: ')
                if len(ip_and_asn) > 1:
                    ip = ip_and_asn[0]
                    asn = ip_and_asn[1]
                    if asn[:2] != 'IP':
                        asn = ', '.join(asn.split(' ', 1))
                    fout.write('%s, %s' % (ip, asn))
                else:
                    fout.write('%s' % ip_and_asn)
    print('ASN list cleaning complete!')


def populateASNdictionary():
    print('Populating ASN Dictionary from %s' % ASN_LIST_CLEANED_FILE)
    with open(ASN_LIST_CLEANED_FILE, 'r') as asnlist:
        for line in asnlist:
            try:
                ip, asn = line.split(',')[0:2]
            except:
                print(line)
            asn_dictionary[ip] = asn
            # print('ip: %s\nasn: %s' % (ip, asn))
    print('Dictionary population complete!')


def produceOutput():
    print('Creating output...')
    with open(EDITED_FLOW_AGG_PATH, 'r') as src_file:
        with open(OUTPUT_DATA_FILE, 'w') as outfile:
            header = src_file.readline().strip() + ',src_ASN,dst_ASN\n'
            outfile.write(header)
            # print(header)
            for line in src_file:
                # print(line)
                line_arr = line.split(',')
                src_ip = line_arr[2]
                dst_ip = line_arr[4]
                outfile.write(line.strip() + ',' + asn_dictionary[src_ip].strip() +
                              ',' + asn_dictionary[dst_ip].strip() + '\n')
    print('Finished!')

if __name__ == '__main__':
    print('beginning processing...')
    # check if the edited csv exists or not
    if False:
        prepFlowAggFile()
    # check if unique ip list exists or not
    if False:
        getUniqueIPs()
    # check if ASN lookup has been completed
    if False:
        # asnLookup()
        asnListCleanup()
    if True:
        populateASNdictionary()
        produceOutput()
