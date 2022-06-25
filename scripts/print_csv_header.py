import os
import csv

for filename in os.listdir('.'):
  if filename.endswith(".csv"):
    with open(filename, encoding='utf-8-sig') as f:
      print(filename)
      csv_reader = csv.reader(f)
      csv_header = next(csv_reader)
      print(csv_header)
    print()