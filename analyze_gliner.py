from collections import Counter
from gliner import GLiNER
model = GLiNER.from_pretrained("urchade/gliner_multi_pii-v1")
import pandas as pd
import time
import re


def get_most_common_entity(results):
    label_counter = Counter()
    for entity in results:
        label = entity.get("label")
        if label:
            label_counter[label] += 1

    if label_counter:
        print(label_counter)
        # Get label with highest count
        most_common_label, _ = label_counter.most_common(1)[0]
        # print(most_common_label)
        if label_counter[most_common_label]>=5:
            return most_common_label
    return None
# @time_it
def analyze_column(df):
    entity_columns = {}
    keywords = ["description", "remarks", "notes", "comments", "observations", "details", "summary", "explanation",
    "reviews", "feedback", "testimonials", "opinions", "assessment", "suggestions", "experience","status",
    "incident_report", "case_notes", "audit_notes", "findings", "status_update", "history", "progress_report",
    "additional_info", "clarifications", "justification", "annotations", "excerpts", "statement", "explanation_text","reason"]
    des=[col for col in df.columns if any(re.search(keyword, col, re.IGNORECASE) for keyword in keywords)]
    print("descriptive: ",des)
    for col in df.columns:
      if "id" in col.lower():
        entity_columns[col] = "ID"
      elif "date" in col.lower():
        entity_columns[col] = "date"
      elif col in des:
        entity_columns[col] = "description"
      else:
        print("colum: ",col)
        values = df[col].dropna().astype(str).tolist()[:10]
        if not values:
            continue
        combined_text = " , ".join(values)  # You could also use newline or space
        results_batch = model.predict_entities(combined_text, labels=["person","city", "phone number", "location", "email", "url", "company","country"])
        print(results_batch)
        entity_count = {}
        if not results_batch:
            continue
        else:
          most_common_entity = get_most_common_entity(results_batch)
          if most_common_entity:
              entity_columns[col] = most_common_entity

    return entity_columns
