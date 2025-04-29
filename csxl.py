import pandas as pd
# import openpyxl
# df=pd.read_csv('companies_100k.csv')
# df.to_excel('companies.xlsx', index=False)
def are_csvs_similar(csv_path1, csv_path2, ignore_index=True, ignore_column_order=False):
    df1 = pd.read_csv('new_csv.csv')
    df2 = pd.read_csv('deanonymized_data.csv')

    # Optionally ignore index
    if ignore_index:
        df1.reset_index(drop=True, inplace=True)
        df2.reset_index(drop=True, inplace=True)

    # Optionally ignore column order
    if ignore_column_order:
        df1 = df1.reindex(sorted(df1.columns), axis=1)
        df2 = df2.reindex(sorted(df2.columns), axis=1)

    # Compare shapes first (fast fail)
    if df1.shape != df2.shape:
        print(f"Shape mismatch: {df1.shape} vs {df2.shape}")
        return False

    # Compare content
    comparison = df1.equals(df2)
    if not comparison:
        print("CSV contents are different.")
    return comparison

csv1 = 'new_csv.csv'
csv2 = 'deanonymized_data.csv'

if are_csvs_similar(csv1, csv2, ignore_index=True, ignore_column_order=True):
    print("✅ The CSV files are similar.")
else:
    print("❌ The CSV files are different.")