import csv
import sys

def csv_to_tsv(input_file, output_file):
    with open(input_file, mode='r', encoding='utf-8', newline='') as infile, \
         open(output_file, mode='w', encoding='utf-8', newline='') as outfile:

        # Read with semicolon as input delimiter
        reader = csv.reader(infile, delimiter=';')
        writer = csv.writer(outfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            # Strip whitespace around each field and skip empty lines
            row = [field.strip() for field in row]
            if any(row):
                writer.writerow(row)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 csv_to_tsv.py input.csv output.tsv")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_tsv = sys.argv[2]

    csv_to_tsv(input_csv, output_tsv)
