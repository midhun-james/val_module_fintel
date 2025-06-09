import pandas as pd
import json
from collections import defaultdict
import os
import time
import gzip
import sqlite3
import sqlparse
from sqlparse.sql import Token
from sqlparse.tokens import Literal,String
from datetime import datetime
import random
import re
import string
import polars as pl
from openpyxl import load_workbook
class DataMaskerCSV:
    def __init__(self,file_path):
        # self.entity_column_map={
        #                 'names': 'names',
        #                 'emails': 'emails',
        #                 'phone': 'phone',
        #                 'credit': 'credit',
        #                 'url': 'url',
        #                 'location': 'location',
        #                 'company': 'company',
        #             }
        self.file_path=file_path
        self.base_name=os.path.splitext(os.path.basename(self.file_path))[0]
        self.output_dir=self.base_name
        os.makedirs(self.output_dir, exist_ok=True)
        self.entity_column_map={
                'Name': 'names',
                'Company': 'company',
                'Location': 'location',
                }
        self.sensitive_columns = self.entity_column_map.keys()
        start=time.time()
        self.faker_data_path= 'faker_dataset_v3.json.gz'
        with gzip.open(self.faker_data_path, 'rt',encoding='utf-8') as f:
            faker_list = json.load(f)
        end=time.time()
        print(f"⏳ Faker data loaded in {end-start:.6f} seconds")
        self.faker_data = {}
        for d in faker_list:
            self.faker_data.update(d)
        self.domain_pool= self.faker_data['url']
        self.forward_mapping = defaultdict(dict)
        self.backward_mapping = defaultdict(dict)
        self.mapping= defaultdict(dict)
        self.fake_data_index = defaultdict(int)

        self.used_fakes = defaultdict(set)
        self.used_urls = set()
        self.url_extensions =  [
                                    ".com", ".net", ".org", ".edu", ".gov", ".co", ".us", ".uk", ".in", ".ru",
                                    ".jp", ".cn", ".de", ".fr", ".it", ".nl", ".es", ".br", ".au", ".ca",
                                    ".ch", ".se", ".no", ".za", ".mx", ".ar", ".be", ".kr", ".pl", ".tr",
                                    ".ua", ".ir", ".sa", ".ae", ".my", ".sg", ".hk", ".tw", ".nz", ".id",
                                    ".th", ".ph", ".vn", ".bd", ".lk", ".np", ".pk", ".cz", ".gr", ".hu",
                                    ".fi", ".dk", ".il", ".ie", ".pt", ".sk", ".si", ".ro", ".bg", ".rs",
                                    ".lt", ".lv", ".ee", ".hr", ".ba", ".md", ".ge", ".kz", ".by", ".tm",
                                    ".uz", ".af", ".qa", ".om", ".kw", ".bh", ".ye", ".jo", ".lb", ".sy",
                                    ".iq", ".ps", ".az", ".am", ".kg", ".mn", ".bt", ".mv", ".mm", ".kh",
                                    ".la", ".tl", ".sb", ".fj", ".pg", ".to", ".tv", ".ws", ".fm", ".ki"
                                ]
 

    @staticmethod
    def time_it(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            print(f'\n⏳ Execution time {func.__name__}: {end-start:.6f} seconds')
            return result
        return wrapper
    def descriptive_finder(self):   
        df =pd.read_excel(self.file_path,nrows=20)
        des=[]
        for col in df.columns:
            series = df[col].dropna().astype(str)
            if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
                continue
            if len(series)==0:
                continue
            else:
                
                unique_ratio = series.nunique() / len(series)
                avg_length = series.apply(len).mean()
                avg_word_count = series.apply(lambda x: len(x.split())).mean()
                has_punctuation = series.str.contains(r'[.,;:?!]').mean()
                if (
                    unique_ratio > 0.5 and
                    avg_length > 10 and
                    avg_word_count > 3 and
                    has_punctuation > 0.3
                ):
                    des.append(col)
        return des
    @time_it 
    def csv_extraction(self):
        
        output_csv_path=os.path.join(self.output_dir,f'new_{self.base_name}.csv')
        if self.file_path.endswith('.xlsx'):
            sheet_names = pd.ExcelFile(self.file_path).sheet_names
            df=pl.read_excel(self.file_path,engine='calamine',sheet_name=sheet_names)
            combined_df = pl.concat(df.values(), how="diagonal")
            combined_df.write_csv('intermediate.csv')
            self.file_path='intermediate.csv'

        all_data={}
        for col in self.sensitive_columns:
            entity=self.entity_column_map.get(col)
            if entity:
                all_data.setdefault(entity, [])
        df=pd.read_csv(self.file_path)
        for col in self.sensitive_columns:
            if col in df.columns:
                entity=self.entity_column_map.get(col)
                if entity:
                    values=df[col].dropna().to_list()
                    all_data[entity].extend(values)
                else:
                    entity=self.entity_column_map.get(col.lower())
                    if entity:
                        all_data[entity].extend([None]*len(df))
        max_len=max([len(v) for v in all_data.values()])
        for entity in all_data:
            all_data[entity].extend([None]*(max_len-len(all_data[entity])))
        final_df=pd.DataFrame(all_data)
        final_df.to_csv(output_csv_path,index=False)

        if self.file_path == 'intermediate.csv': os.remove(self.file_path)
        self.anonymize_csv(output_csv_path)
    
    def _get_fake_value(self, entity, original_value):
        """Return consistent fake value for an original value."""
        col_key =  entity  # default fallback if column not passed


        if original_value in self.forward_mapping[col_key]:
            return self.forward_mapping[col_key][original_value]
        if entity =='url':
            while True:
                domain1,domain2=random.sample(self.domain_pool,2)
                fake_value=f"https://{domain1.lower()}.{domain2.lower()}.co"
                if fake_value not in self.used_fakes[entity]:
                    break
            self.used_fakes[entity].add(fake_value)
            self.forward_mapping[col_key][original_value] = fake_value
            self.backward_mapping[col_key][fake_value] = original_value
            return fake_value
        
        while self.fake_data_index[entity] < len(self.faker_data[entity]):
            fake_value = self.faker_data[entity][self.fake_data_index[entity]]
            self.fake_data_index[entity] += 1

            if fake_value not in self.used_fakes[entity]:
                self.used_fakes[entity].add(fake_value)
                self.forward_mapping[col_key][original_value] = fake_value
                self.backward_mapping[col_key][fake_value] = original_value
                return fake_value
        
        counter=1
        base_fake_value=original_value
        while True:
            fallback_value= self.modify_fake_value(entity, base_fake_value,  counter=counter)
            if fallback_value not in self.used_fakes[entity]:
                self.used_fakes[entity].add(fallback_value)
                self.forward_mapping[col_key][original_value] = fallback_value
                self.backward_mapping[col_key][fallback_value] = original_value
                return fallback_value
            counter+=1

        
    def modify_fake_value(self,entity,original_value,counter=1):
        """Modify the fake value to ensure uniqueness."""
        if entity=="names":
            base=random.choice(self.faker_data['names'])
            return base+f"{string.ascii_lowercase[counter % 26]}"
        elif entity=="emails":
            base=random.choice(self.faker_data['emails'])
            name,domain=base.split('@')
            return f"{name}{counter}@{domain}"
        elif entity=="url":
            fake_value=original_value
            while fake_value in self.used_urls:
                ext=random.choice(self.url_extensions)
                if not fake_value.endswith(ext):
                    fake_value=fake_value+ext
            self.used_urls.add(fake_value)
            return fake_value
        elif entity=="phone":
            base=random.choice(self.faker_data['phone'])
            return f"{base[:-2]}{counter % 100:02d}"
        elif entity == "company":
            base=random.choice(self.faker_data['company'])
            return f"{base} Group {counter % 100_000_000 + 1}"
        elif entity == "credit":
            return f"{original_value[:-4]}{counter % 10000:04d}"
        else:
            return f"{original_value}-{counter}"

    @time_it
    def anonymize_csv(self, input_csv_path):
        df = pd.read_csv(input_csv_path)
        for entity in df.columns:

            if entity not in self.faker_data:
                print(f"Warning: No fake data available for entity type '{entity}' '.")
                continue

            df[entity] = df[entity].apply(lambda val: self._get_fake_value(entity, val) if pd.notna(val) else val)

        output_csv_path=os.path.join(self.output_dir,f'{self.base_name}_masked.csv')
        df.to_csv(output_csv_path, index=False)

        combined_mapping = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "columns_anonymized": list(self.forward_mapping.keys()),
                "total_entries": {
                    col: len(self.forward_mapping[col]) for col in self.forward_mapping
                }
            },
            "forward_mapping": self.forward_mapping,
            "backward_mapping": self.backward_mapping,
        }
        map_path =f'{self.base_name}_mapping.json'
        with open(map_path, 'w') as f: 
            json.dump(combined_mapping, f, indent=2)


        # print(f"Anonymized CSV saved to: {output_csv_path}")
        print(f" mapping saved to: {map_path}")
  
    
    @time_it
    def deanonymize_csv(self,anonymized_csv_path,map_path,deanonymized_csv_path):
        df = pd.read_csv(anonymized_csv_path)

        with open(map_path, 'r') as f:
            self.backward_mapping = json.load(f).get("backward_mapping", {})
        
        for col in self.sensitive_columns:
            entity= self.entity_column_map.get(col.lower())
            if col not in df.columns:
                continue
            backward_map = self.backward_mapping.get(entity, {})

            df[col]=df[col].apply(lambda val:backward_map.get(val,entity) if pd.notna(val) else val )
        df.to_csv(deanonymized_csv_path,index=False)
        print(f"Deanonymized CSV saved to: {deanonymized_csv_path}")
    @time_it
    def csv_to_sql(self,csv_path,_db_path,table_name):
        try:
            df=pd.read_csv(csv_path)
            conn=sqlite3.connect(_db_path)
            df.to_sql(table_name,conn,if_exists='replace',index=False)
            conn.close()
        except Exception as e:
            print(f"❌ Failed to import CSV: {e}")

file_path = 'desc.xlsx'
masker = DataMaskerCSV(file_path)
masker.csv_extraction()
