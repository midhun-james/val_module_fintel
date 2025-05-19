import json 
import re
import time
from collections import defaultdict
import sqlparse
from sqlparse.sql import Token
from sqlparse.tokens import Literal,String

class DbOperations:
    def __init__(self):
        self.map_path='companies_100k_mapping.json'
        self.forward_mapping = defaultdict(dict)
        self.backward_mapping = defaultdict(dict)
        # self.model=GLiNER.from_pretrained("urchade/gliner_base")
        with open(self.map_path, 'r') as f:
            data= json.load(f)
            self.forward_mapping = data.get('forward_mapping', {})
            self.backward_mapping = data.get('backward_mapping', {})
        self.entity_column_map={
        'name': 'company',
        'domain': 'url',
        }
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
        for entity, value_map in self.forward_mapping.items():
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
                # print(f'{match.group(0)} => {prefix}{replaced}{suffix}')
                return f"{prefix}{replaced}{suffix}"
            sentence = pattern.sub(replace_match, sentence)

        return sentence
    
    @time_it
    def unmask_summary(self, sentence):
        flat_map = {}
        for entity, value_map in self.backward_mapping.items():
            for original, fake in value_map.items():
                flat_map[original] = fake
        # Pre-lowercased lookup for fast replacement
        flat_map_lower = {}
        for fake,original in flat_map.items():
            fake_lower=fake.lower()
            flat_map_lower[fake_lower]=original

            core=re.sub(r'\b(co|llc|inc|group|international|corporation|ltd|)\.?$', '', fake_lower, flags=re.IGNORECASE).strip()
            if core and core!= fake_lower:
                flat_map_lower[core]=original
        # Identify which keys are present in the sentence (case-insensitive)
        sentence_lower = sentence.lower()
        matched_keys = [k for k in flat_map_lower if k in sentence_lower]

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
                # print(f'{match.group(0)} => {prefix}{replaced}{suffix}')
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
    def query_unmask(self, query):
        parsed = sqlparse.parse(query)
        masked_query = []

        for statement in parsed:
            tokens = list(statement.flatten())
            for token in tokens:
                original_value = token.value
                value = original_value.strip("\"'")  # remove both types of quotes
                replaced = False
                for ent in self.backward_mapping.values():
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
    def unmasking_results(self,results):
        de_anonymized = []
        for row in results:
            new_row={}
            for col,val in row.items():
                entity= self.entity_column_map.get(col.lower())
                key=f"{entity}"
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
                entity= self.entity_column_map.get(col.lower())
                key=f"{entity}"
                if key in self.forward_mapping and val in self.forward_mapping[key]:
                    new_row[col]=self.forward_mapping[key][val]
                else:
                    new_row[col]=val
            de_anonymized.append(new_row)
        return de_anonymized

# Example usage
def main():

    op = DbOperations()
    sentence = 'name is ibm and inc and domain is ibm.com'
    summary= 'name is Williams-Waller Co and Hall-Parker Corporation and domain is https://butler-reed.reid.co'

    query= "SELECT * FROM employees WHERE name= infosys and domain= 'infosys.com'"

    m_qury="SELECT * FROM employees WHERE name= 'Cox-Holloway International' and domain= 'https://chapman-kim.sanchez.co'"
    aa=[{'id': 1454663, 'name': 'infosys', 'domain': 'infosys.com', 'year founded': 1981.0, 'industry': 'information technology and services', 'size range': '10001+', 'locality': 'bangalore, karnataka, india', 'country': 'india', 'linkedin url': 'linkedin.com/company/infosys', 'current employee estimate': 104752, 'total employee estimate': 215718}, {'id': 2520281, 'name': 'pwd', 'domain': 'pwwwd.com', 'year founded': None, 'industry': 'internet', 'size range': '1001 - 5000', 'locality': 'gresik, jawa timur, indonesia', 'country': 'bermuda', 'linkedin url': 'linkedin.com/company/pwd', 'current employee estimate': 1441, 'total employee estimate': 1541}]
    # zz=' "infosys.com"  {{infosys}}    [infosys]    **infosys**   (infosys) "infosys" '
    # abc="infosys  {{infosys}}"
    masked_res=[{'id': 1454663, 'name': 'Cox-Holloway International', 'domain': 'https://scott-smith.gamble-nelson.co', 'year founded': 1981.0, 'industry': 'information technology and services', 'size range': '10001+', 'locality': 'bangalore, karnataka, india', 'country': 'india', 'linkedin url': 'linkedin.com/company/infosys', 'current employee estimate': 104752, 'total employee estimate': 215718}, {'id': 2520281, 'name': 'Galloway-Scott LLC', 'domain': 'https://davis.graham.co', 'year founded': None, 'industry': 'internet', 'size range': '1001 - 5000', 'locality': 'gresik, jawa timur, indonesia', 'country': 'bermuda', 'linkedin url': 'linkedin.com/company/pwd', 'current employee estimate': 1441, 'total employee estimate': 1541}]
    # masked_sentence = op.mask_sentence(sentence)
    # print("masked sentence:", masked_sentence)
    # unmasked_sentence = op.unmask_summary(summary)
    # print("unmasked summary:", unmasked_sentence)
    masked_query = op.query_mask(query)
    print("masked query:", masked_query)
    unmasked_query= op.query_unmask(m_qury)
    print("unmasked query:", unmasked_query)
    # print(op.masking_results(aa))
    # print("unmasked result: ",op.unmasking_results(masked_res))
op=DbOperations()
sentence = 'name is ibm '
print("masked is: ",op.mask_sentence(sentence))
summary='name is Williams-Waller co'
print("unmasked is: ", op.unmask_summary(summary))