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
class DataMaskerCSV:
    def __init__(self):
        self.entity_column_map={
            'name': 'company',
            'domain': 'url',
            'locality': 'location',
            'country': 'country',
            'linkedin url': 'url',
        }
        self.faker_data_path= 'faker_data_v2.json.gz'
        with gzip.open(self.faker_data_path, 'rt',encoding='utf-8') as f:
            faker_list = json.load(f)
        self.faker_data = {}
        for d in faker_list:
            self.faker_data.update(d)
        self.domain_pool= self.faker_data['url']
        self.forward_mapping = defaultdict(dict)
        self.backward_mapping = defaultdict(dict)
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


    def _get_fake_value(self, entity, original_value, column_name=None):
        """Return consistent fake value for an original value."""
        col_key = column_name or entity  # default fallback if column not passed


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
            fallback_value= self.modify_fake_value(entity, base_fake_value, column_name=column_name, counter=counter)
            if fallback_value not in self.used_fakes[entity]:
                self.used_fakes[entity].add(fallback_value)
                self.forward_mapping[col_key][original_value] = fallback_value
                self.backward_mapping[col_key][fallback_value] = original_value
            counter+=1

        
    def modify_fake_value(self,entity,original_value,column_name=None,counter=1):
        """Modify the fake value to ensure uniqueness."""
        if entity=="name":
            return original_value+f"{string.ascii_lowercase[counter % 26]}"
        elif entity=="email":
            name,domain=original_value.split('@')
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
            return f"{original_value[:-2]}{counter % 100:02d}"
        elif entity == "company":
            return f"{original_value} Group {counter % 100_000_000 + 1}"
        elif entity == "credit":
            return f"{original_value[:-4]}{counter % 10000:04d}"
        else:
            return f"{original_value}-{counter}"

    @time_it
    def anonymize_csv(self, input_csv_path, sensitive_columns, output_csv_path, forward_map_path, backward_map_path):
        df = pd.read_csv(input_csv_path)

        for column in sensitive_columns:
            if column not in df.columns:
                print(f"Warning: Column '{column}' not found in CSV.")
                continue

            entity = self.entity_column_map.get(column.lower())
            if not entity or entity not in self.faker_data:
                print(f"Warning: No fake data available for entity type '{entity}' from column '{column}'.")
                continue

            df[column] = df[column].apply(lambda val: self._get_fake_value(entity, val, column_name=column) if pd.notna(val) else val)


        df.to_csv(output_csv_path, index=False)

        with open(forward_map_path, 'w') as f:
            json.dump(self.forward_mapping, f, indent=2)

        with open(backward_map_path, 'w') as f:
            json.dump(self.backward_mapping, f, indent=2)
        
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "columns_anonymized": list(self.forward_mapping.keys()),
            "total_entries": {
                col: len(self.forward_mapping[col]) for col in self.forward_mapping
            }
        }

        with open("anonymization_metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

        print(f"Anonymized CSV saved to: {output_csv_path}")
        print(f"Forward mapping saved to: {forward_map_path}")
        print(f"Backward mapping saved to: {backward_map_path}")
    
    @time_it
    def deanonymize_csv(self,anonymized_csv_path,sensitive_columns,backward_mapping_path,deanonymized_csv_path):
        df = pd.read_csv(anonymized_csv_path)

        with open(backward_mapping_path, 'r') as f:
            self.backward_mapping = json.load(f)
        for col in sensitive_columns:
            if col not in df.columns:
                continue
            backward_map = self.backward_mapping.get(col, {})

            df[col]=df[col].apply(lambda val:backward_map.get(val,val) if pd.notna(val) else val )
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
    
entity_column_map = {
    'name': 'company',
    'domain': 'url',
    'locality': 'location',
    'country': 'country',
    'linkedin url': 'url',
}

sensitive_columns = ['name', 'domain',]
faker_data_path = 'faker_data_v2.json.gz'

masker = DataMaskerCSV()

masker.anonymize_csv(
    input_csv_path='companies_100k.csv',
    sensitive_columns=sensitive_columns,
    output_csv_path='anonymized_data.csv',
    forward_map_path='f_mapping.json',
    backward_map_path='b_mapping.json'
)
masker.deanonymize_csv(
    anonymized_csv_path='anonymized_data.csv',
    sensitive_columns=sensitive_columns,
    backward_mapping_path='b_mapping.json',
    deanonymized_csv_path='deanonymized_data.csv'
)
masker.csv_to_sql(
    csv_path='companies_100k.csv',
    _db_path='company.db',
    table_name='companies_100k'
)
masker.csv_to_sql(
    csv_path='anonymized_data.csv',
    _db_path='company.db',
    table_name='companies_masked'
)
