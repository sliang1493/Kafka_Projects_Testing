import csv
import json

def read_csv_file_and_sum(file_path):
    ecc_sum = 0
    cit_sum = 0
    ems_sum = 0
    valid_depts = ['ECC', 'CIT', 'EMS']
    with open(file_path, mode = 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    if row.get('Department') in valid_depts and row.get('Initial Hire Date')[-4:] >= '2010':
                        row['Salary'] = int(float(row['Salary']))
                        if row.get('Department') == 'ECC':
                             ecc_sum += row['Salary']
                        elif row.get('Department') == 'CIT':
                             cit_sum += row['Salary']
                        else:
                             ems_sum += row['Salary']
                except Exception as e:
                    print(f"Skipping row due to invalid data and error: {row} and {e}")
    
    return (ecc_sum, cit_sum, ems_sum)


csv_file_loc = "Employee_Salaries.csv"
ECC_sum, CIT_sum, EMS_sum = read_csv_file_and_sum(csv_file_loc)
print("ECC Total: ", ECC_sum)
print("CIT Total: ", CIT_sum)
print("EMS Total: ", EMS_sum)
