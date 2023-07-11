import pandas as pd
import re

# Example DataFrame with 'tag_name' column
data = {'tag_name': ['report_data[22].log_data[142]', 'report_data[0].log_data[123]', 'report_data[151].log_data[0]']}
df = pd.DataFrame(data)

# Extract the last numeric value within square brackets
pattern = r'\[(\d+)\]'
last_tag_name = df['tag_name'].str.extract(pattern).astype(int).iloc[-1][0]

# Print the result
print(df)
print("-------------")
print(type(last_tag_name))
print(last_tag_name)


last_tag_names = df['tag_name'].str.extract(pattern, expand=False).astype(int)
print("----------")
print(type(last_tag_names))
print(last_tag_names)
print("----------")
for i, last_tag_name in enumerate(last_tag_names):
    print(f"Item {i+1}: {last_tag_name}")