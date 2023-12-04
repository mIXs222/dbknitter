# DBKnitter

Knitting DBs for cross-database queries 🧶

## Experiments

Prerequisites:
- Docker
- Bash
- Python3
- Make (Optional)

The following instruction measures accuracy of DBKnitter's generated cross-database programs across multiple query languages: SQL, official TPC-H description, manual English translation, and GPT's English translation.

Optionally, setup Python environment:
```bash
make virtualenv
source .venv/bin/activate
```

Unzip data.
```bash
unzip tpch.zip
```

Acquire an API key from OpenAI: https://platform.openai.com/api-keys. One may need to add an amount to credit balance: https://platform.openai.com/account/billing/overview.

Generate 27 table-platform mappings over 22 queries across 4 baselines.
```bash
API_KEY=YOUR_API_KEY_HERE
ALL_MAPPINGS="00000000,11111111,22222222,00001111,00002222,11110000,11112222,22220000,22221111,01010101,02020202,10101010,12121212,20202020,21212121,01201201,02102102,10210210,12012012,20120120,21021021,00011122,00022211,11100022,11122200,22200011,22211100"

python dbknitter/gpt_tpch.py batch --output_dir platforms/client/source/s01/exp_sql --db_splits ${ALL_MAPPINGS} --query_language sql --api_key ${API_KEY}
python dbknitter/gpt_tpch.py batch --output_dir platforms/client/source/s01/exp_engoff --db_splits ${ALL_MAPPINGS} --query_language eng-official --api_key ${API_KEY}
python dbknitter/gpt_tpch.py batch --output_dir platforms/client/source/s01/exp_engman --db_splits ${ALL_MAPPINGS} --query_language eng-manual --api_key ${API_KEY}
python dbknitter/gpt_tpch.py batch --output_dir platforms/client/source/s01/exp_enggpt --db_splits ${ALL_MAPPINGS} --query_language eng-gpt --api_key ${API_KEY}
```

Grade those cross-database programs.
```bash
ALL_MAPPINGS="00000000 11111111 22222222 00001111 00002222 11110000 11112222 22220000 22221111 01010101 02020202 10101010 12121212 20202020 21212121 01201201 02102102 10210210 12012012 20120120 21021021 00011122 00022211 11100022 11122200 22200011 22211100"

for m in ${ALL_MAPPINGS}; do echo ">>> ${m}"; bash cloudlab/grade_by_mapping.sh ${m} exp_sql; done
for m in ${ALL_MAPPINGS}; do echo ">>> ${m}"; bash cloudlab/grade_by_mapping.sh ${m} exp_engoff; done
for m in ${ALL_MAPPINGS}; do echo ">>> ${m}"; bash cloudlab/grade_by_mapping.sh ${m} exp_engman; done
for m in ${ALL_MAPPINGS}; do echo ">>> ${m}"; bash cloudlab/grade_by_mapping.sh ${m} exp_enggpt; done
```

Then, grading results will be `grade_output/exp_[sql|engoff|engman|enggpt]/*/*.txt`. Grab lines starting with `Score.` and insert them into `print_plots.py` accordingly. Then, print and compile the measurements in LaTeX.
```bash
python print_plots.py
```
