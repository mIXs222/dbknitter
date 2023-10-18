import pandas as pd
from pathlib import Path

DEFAULT_OUTPUT_PATH = Path("./output.csv") 

df = pd.DataFrame({'name': ['Raphael', 'Donatello'],
                   'mask': ['red', 'purple'],
                   'weapon': ['sai', 'bo staff']})
df.to_csv(index=False)
df.to_csv(DEFAULT_OUTPUT_PATH) 
