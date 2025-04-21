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
class DataMaskerCSV:
    def __init__(self, faker_data_path, entity_column_map):
        self.entity_column_map = entity_column_map
        with gzip.open(faker_data_path, 'rt',encoding='utf-8') as f:
            faker_list = json.load(f)
        self.faker_data = {}
        for d in faker_list:
            self.faker_data.update(d)
        self.domain_pool= self.faker_data['url']
        self.forward_mapping = defaultdict(dict)
        self.backward_mapping = defaultdict(dict)
        self.fake_data_index = defaultdict(int)
        self.used_fakes = defaultdict(set)

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
        global ID


        if original_value in self.forward_mapping[col_key]:
            return self.forward_mapping[col_key][original_value]
        if entity =='url':
            domain1,domain2=random.sample(self.domain_pool,2)
            fake_value=f"https://{domain1.lower()}.{domain2.lower()}.co"
            if fake_value not in self.used_fakes[entity]:
                self.used_fakes[entity].add(fake_value)
                self.forward_mapping[col_key][original_value] = fake_value
                self.backward_mapping[col_key][fake_value] = original_value

            return fake_value
        else:
            while self.fake_data_index[entity] < len(self.faker_data[entity]):
                fake_value = self.faker_data[entity][self.fake_data_index[entity]]
                self.fake_data_index[entity] += 1

                if fake_value not in self.used_fakes[entity]:
                    self.used_fakes[entity].add(fake_value)
                    self.forward_mapping[col_key][original_value] = fake_value
                    self.backward_mapping[col_key][fake_value] = original_value
                    return fake_value

        raise ValueError(f"No more unique fake values available for entity: {entity}")

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
    
    @time_it
    def query_mask(self,query,forward_mapping_path):
        with open(forward_mapping_path, 'r') as f:
            self.forward_mapping = json.load(f)
        parsed = sqlparse.parse(query)
        masked_query = []
        for statement in parsed:
            tokens=list(statement.flatten())
            for token in tokens:
                if token.ttype in (String.Single, Literal.String.Single):
                    value=token.value.strip("'\'")
                    
                    replaced=False
                    for ent in self.forward_mapping.values():
                        if value in ent:
                            fake_value=ent[value]
                            token.value=f"'{fake_value}'"
                            replaced=True
                            break
                masked_query.append(token.value)
        return ''.join(masked_query)
    @time_it
    def query_unmask(self,query,reverse_mapping_path):
        with open(reverse_mapping_path, 'r') as f:
            self.forward_mapping = json.load(f)
        parsed = sqlparse.parse(query)
        masked_query = []
        for statement in parsed:
            tokens=list(statement.flatten())
            for token in tokens:
                if token.ttype in (String.Single, Literal.String.Single):
                    value=token.value.strip("'\'")
                    
                    replaced=False
                    for ent in self.backward_mapping.values():
                        if value in ent:
                            real_value=ent[value]
                            token.value=f"'{real_value}'"
                            replaced=True
                            break
                masked_query.append(token.value)
        return ''.join(masked_query)
    
    @time_it
    def demasking_results(self,results,revese_mapping_path):
        de_anonymized = []
        with open(revese_mapping_path) as f:
            reverse_mapping = json.load(f)
        for row in results:
            new_row={}
            for col,val in row.items():
                key=f"{col}"
                if key in reverse_mapping and val in reverse_mapping[key]:
                    new_row[col]=reverse_mapping[key][val]
                else:
                    new_row[col]=val
            de_anonymized.append(new_row)
        return de_anonymized
    

    @time_it



    def mask_sentence(self, sentence, forward_mapping_path):
        start = time.time()
        with open(forward_mapping_path, 'r') as f:
            self.forward_mapping = json.load(f)
        end = time.time()
        print("load:", end - start)

        flat_map = {}
        start = time.time()
        for column, value_map in self.forward_mapping.items():
            for original, fake in value_map.items():
                flat_map[original] = fake
        end = time.time()
        print("flatten:", end - start)

        # Pre-lowercased lookup for fast replacement
        flat_map_lower = {k.lower(): v for k, v in flat_map.items()}

        # Identify which keys are present in the sentence (case-insensitive)
        sentence_lower = sentence.lower()
        matched_keys = [k for k in flat_map if k.lower() in sentence_lower]

        if matched_keys:
            # Sort matched keys by length (longest first) to avoid partial replacement
            matched_keys.sort(key=len, reverse=True)

            # Build regex pattern with alternation
            pattern = re.compile(
                r'(?<!\w)(' + '|'.join(re.escape(k) for k in matched_keys) + r')(?=[\s\.,;:!?"]|$)',
                flags=re.IGNORECASE
            )

            def replace_match(match):
                word = match.group(0)
                return flat_map_lower.get(word.lower(), word)

            sentence = pattern.sub(replace_match, sentence)

        return sentence





    

entity_column_map = {
    'name': 'company',
    'domain': 'url',
    'locality': 'location',
    'country': 'country',
    'linkedin url': 'url',
}

sensitive_columns = ['name', 'domain',]
faker_data_path = 'faker_data_v2.json.gz'

masker = DataMaskerCSV(faker_data_path, entity_column_map)

# masker.anonymize_csv(
#     input_csv_path='companies_100k.csv',
#     sensitive_columns=sensitive_columns,
#     output_csv_path='anonymized_data.csv',
#     forward_map_path='f_mapping.json',
#     backward_map_path='b_mapping.json'
# )
# masker.deanonymize_csv(
#     anonymized_csv_path='anonymized_data.csv',
#     sensitive_columns=sensitive_columns,
#     backward_mapping_path='b_mapping.json',
#     deanonymized_csv_path='deanonymized_data.csv'
# )
# masker.csv_to_sql(
#     csv_path='companies_100k.csv',
#     _db_path='company.db',
#     table_name='companies_100k'
# )
# masker.csv_to_sql(
#     csv_path='anonymized_data.csv',
#     _db_path='company.db',
#     table_name='companies_masked'
# )

# sql="SELECT * FROM employees WHERE company = 'ibm'"
# masked_sql=masker.query_mask(sql,forward_mapping_path='f_mapping.json')
# print(masked_sql)
# masked_results = [
#     {'name': 'Ford-Mendez Corporation Inc.', 'domain': 'https://carrillo.harvey.co',}
# ]
# demasked=masker.demasking_results(
#     results=masked_results,
#     revese_mapping_path='b_mapping.json'
# )
# print(demasked)

# input="Show all employees from pwd , infosys where domain is ibm.com, infosys.com"
# masked=masker.mask_sentence(input,forward_mapping_path='f_mapping.json')
# print(masked)