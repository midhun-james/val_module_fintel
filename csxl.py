import pandas as pd
import openpyxl
df=pd.read_csv('companies_100k.csv')
df.to_excel('companies.xlsx', index=False)