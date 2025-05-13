
# import win32com.client
# import pdfplumber
# def xlsx_to_pdf(input_path, output_path):
#     excel = win32com.client.Dispatch("Excel.Application")
#     excel.Visible = False
#     wb = excel.Workbooks.Open(input_path)
#     wb.ExportAsFixedFormat(0, output_path)  # 0 = PDF format
#     wb.Close()
#     excel.Quit()



# # Example usage
# # xlsx_to_pdf("C:/Users/287937/Desktop/validation_database/val_module_fintel/Book1.xlsx", "C:/Users/287937/Desktop/validation_database/val_module_fintel/testing.pdf")
# pdf = pdfplumber.open("testing.pdf")
# page = pdf.pages[1]
# a=page.extract_tables()
# print(a)

import pdfplumber
import pandas as pd

with pdfplumber.open("testing.pdf") as pdf:
    for i, page in enumerate(pdf.pages):
        table = page.extract_table({
            "vertical_strategy": "text",  # or "text"
            "horizontal_strategy": "text",
            "intersection_tolerance": 5,
            "snap_tolerance": 3,
            "join_tolerance": 3
        })
        if table:
            df = pd.DataFrame(table[1:], columns=table[0])
            df.to_excel(f"output_page_{i+1}.xlsx", index=False)
