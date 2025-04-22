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
                r'(?<!\w)([\{\(\["\'\*\_]*?)(' +
                '|'.join(re.escape(k) for k in matched_keys) +
                r')([\}\)\]"\'\*\_]*?)(?!\w)',flags=re.IGNORECASE
            )
            def replace_match(match):
                prefix = match.group(1)  # e.g., '{' or '**'
                core = match.group(2)    # e.g., 'abc'
                suffix = match.group(3)  # e.g., '}' or '**'

                replaced = flat_map_lower.get(core.lower(), core)
                print(f'{match.group(0)} => {prefix}{replaced}{suffix}')
                return f"{prefix}{replaced}{suffix}"
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
                r'(?<!\w)([\{\(\["\'\*\_]*?)(' +
                '|'.join(re.escape(k) for k in matched_keys) +
                r')([\}\)\]"\'\*\_]*?)(?!\w)',flags=re.IGNORECASE
            )

            def replace_match(match):
                prefix = match.group(1)  # e.g., '{' or '**'
                core = match.group(2)    # e.g., 'abc'
                suffix = match.group(3)  # e.g., '}' or '**'

                replaced = flat_map_lower.get(core.lower(), core)
                print(f'{match.group(0)} => {prefix}{replaced}{suffix}')
                return f"{prefix}{replaced}{suffix}"

            sentence = pattern.sub(replace_match, sentence)

        return sentence

    @time_it
    def query_mask(self, query):
        parsed = sqlparse.parse(query)
        masked_query = []

        for statement in parsed:
            tokens = list(statement.flatten())
            for token in tokens:
                original_value = token.value
                value = original_value.strip("\"'")  # remove both types of quotes
                replaced = False
                for ent in self.forward_mapping.values():
                    if value in ent:
                        fake_value = ent[value]
                        # Determine if the original was single or double quoted
                        if original_value.startswith("'") and original_value.endswith("'"):
                            token.value = f"'{fake_value}'"
                        elif original_value.startswith('"') and original_value.endswith('"'):
                            token.value = f'"{fake_value}"'
                        else:
                            token.value = fake_value

                        replaced = True
                        break

                masked_query.append(token.value)

        return ''.join(masked_query)
    @time_it
    def query_unmask(self,query,backward_mapping_path):
        self.backward_mapping_path= backward_mapping_path
        with open(self.backward_mapping_path, 'r') as f:
            self.backward_mapping = json.load(f)
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
    def unmasking_results(self,results):
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
    
    @time_it
    def masking_results(self,results):
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
    
# Example usage
op = DbOperations()
sentence = 'name is ibm and inc and domain is ibm.com'
summary= 'name is Brooks and Sons Consulting Ltd. and its domain is https://harding.mcmahon.co'

query= "SELECT * FROM employees WHERE name= infosys and domain= infosys.com"

m_qury='SELECT * FROM employees WHERE name=Vazquez, Evans and Johnson Services Corporation and domain=https://carpenter.newman.co'
aa="[{'id': 1454663, 'name': 'infosys', 'domain': 'infosys.com', 'year founded': 1981.0, 'industry': 'information technology and services', 'size range': '10001+', 'locality': 'bangalore, karnataka, india', 'country': 'india', 'linkedin url': 'linkedin.com/company/infosys', 'current employee estimate': 104752, 'total employee estimate': 215718}, {'id': 2520281, 'name': 'pwd', 'domain': 'pwwwd.com', 'year founded': None, 'industry': 'internet', 'size range': '1001 - 5000', 'locality': 'gresik, jawa timur, indonesia', 'country': 'bermuda', 'linkedin url': 'linkedin.com/company/pwd', 'current employee estimate': 1441, 'total employee estimate': 1541}]"
zz=' "infosys.com"  {{infosys}}    [infosys]    **infosys**   (infosys) "infosys" '
abc="infosys  {{infosys}}"
# masked_sentence = op.mask_sentence(aa)
# print("masked sentence:", masked_sentence)
# unmasked_sentence = op.unmask_summary(summary)
# print("unmasked summary:", unmasked_sentence)
masked_query = op.query_mask(query)
print("masked query:", masked_query)
# unmasked_query = op.query_unmask(m_qury,'b_mapping.json')
# print("unmasked query:", unmasked_query)