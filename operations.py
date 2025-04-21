import json 
import re
import time
from collections import defaultdict
import sqlparse
from sqlparse.sql import Token
from sqlparse.tokens import Literal,String
class DbOperations:
    def __init__(self):
        self.forward_mapping_path= 'f_mapping.json'
        self.backward_mapping_path= 'b_mapping.json'
        with open(self.forward_mapping_path, 'r') as f:
            self.forward_mapping = json.load(f)
        with open(self.backward_mapping_path, 'r') as f:
            self.backward_mapping = json.load(f)
    @staticmethod
    def time_it(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            print(f'\n⏳ Execution time {func.__name__}: {end-start:.6f} seconds')
            return result
        return wrapper
    @time_it
    def mask_sentence(self, sentence):
        flat_map = {}
        for column, value_map in self.forward_mapping.items():
            for original, fake in value_map.items():
                flat_map[original] = fake
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
    
    @time_it
    def unmask_summary(self, sentence):
        flat_map = {}
        for column, value_map in self.backward_mapping.items():
            for original, fake in value_map.items():
                flat_map[original] = fake
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

    @time_it
    def query_mask(self,query):
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
    def query_unmask(self,query):
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
    def result_masking(self,results):
        de_anonymized = []

        for row in results:
            new_row={}
            for col,val in row.items():
                key=f"{col}"
                if key in self.forward_mapping and val in self.forward_mapping[key]:
                    new_row[col]=self.forward_mapping[key][val]
                else:
                    new_row[col]=val
            de_anonymized.append(new_row)
        return de_anonymized

    @time_it
    def result_unmasking(self,results,revese_mapping_path):
        de_anonymized = []
        for row in results:
            new_row={}
            for col,val in row.items():
                key=f"{col}"
                if key in self.backward_mapping and val in self.backward_mapping[key]:
                    new_row[col]=self.backward_mapping[key][val]
                else:
                    new_row[col]=val
            de_anonymized.append(new_row)
        return de_anonymized
    
# Example usage
op = DbOperations()
sentence = 'name is ibm and inc and domain is ibm.com'

s='''The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.
The dataset includes information on two companies. infosys, operating in the information technology and services industry, is headquartered in Bangalore, Karnataka, India. Founded in 1981, the company has a size range of 10001+ employees, with an estimated 104,752 current employees and 215,718 total employees. Its domain is infosys.com, and its LinkedIn page corresponds to Infosys. The second company, pwd, belongs to the internet industry and is located in Gresik, Jawa Timur, Indonesia, though it is registered in Bermuda. Its founding year is not specified. The company has a workforce in the range of 1001 to 5000 employees, with 1,441 current employees and a total estimate of 1,541. Its domain is pwwwd.com, and it is listed on LinkedIn under the name PWD.'''
summary='name is Ford-Mendez Corporation Inc. and Salazar, Thompson and Lawson Technologies Co. and domain is https://clark-cabrera.vargas.co'
res = op.mask_sentence(s)
print(res)
# sum=op.unmask_summary(summary)
# print(sum)
