import csv
import sys

def csv_to_tsv(input_file, output_file):
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile, delimiter='\t')

        for row in reader:
            writer.writerow(row)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 csv_to_tsv.py input.csv output.tsv")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_tsv = sys.argv[2]

    csv_to_tsv(input_csv, output_tsv)
