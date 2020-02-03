"""
02d Features (QuestNet BRC/BRI and CBS Documents)

This script extracts the BRC, BRI and CBS risk grades from the manually imputed file and defines the key features.
"""

from scripts import p1_base_preparation

import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)


# Step 1: Import the manually imputed excel file and extract the key fields.

df = pd.read_excel('/Users/alvin.yue/PycharmProjects/btl_model_development_project/data/base_table_df.xlsx')
df = df[['loan_code',
         'brc_credit_payment_grade',
         'brc_number_of_employees',
         'bri_payment_grade_1',
         'bri_payment_grade_2',
         'cbs_risk_grade_1',
         'cbs_risk_grade_2']]


# Step 2: Define all the key features.

def merge_mapping(by, main_file):
    mapping = [['STRONG', 1],
               ['GOOD', 2],
               ['FAIR', 3],
               ['MARGINAL', 4],
               ['WEAK', 5],
               ['POOR', 6],
               ['AA', 1],
               ['BB', 2],
               ['CC', 3],
               ['DD', 4],
               ['EE', 5],
               ['FF', 6],
               ['GG', 7],
               ['HH', 8],
               ['CX', 9],
               ['GX', 9],
               ['HX', 9],
               ['HZ', 9]]
    mapping = pd.DataFrame(mapping, columns=[by, by + '_score'])

    risk_grades = main_file.merge(mapping,
                                  left_on=by,
                                  right_on=by,
                                  how='left')
    return risk_grades

df = merge_mapping('brc_credit_payment_grade', df)
df = merge_mapping('bri_payment_grade_1', df)
df = merge_mapping('bri_payment_grade_2', df)
df = merge_mapping('cbs_risk_grade_1', df)
df = merge_mapping('cbs_risk_grade_2', df)

def bri_payment_grade_score(row):
    if row['bri_payment_grade_2_score'] > row['bri_payment_grade_1_score']:
        value = row['bri_payment_grade_2_score']
    else:
        value = row['bri_payment_grade_1_score']
    return value

def cbs_risk_grade_score(row):
    if row['cbs_risk_grade_2_score'] > row['cbs_risk_grade_1_score']:
        value = row['cbs_risk_grade_2_score']
    else:
        value = row['cbs_risk_grade_1_score']
    return value

df['bri_payment_grade_score'] = df.apply(bri_payment_grade_score, axis=1)
df['cbs_risk_grade_score']    = df.apply(cbs_risk_grade_score, axis=1)

df['brc_number_of_employees'] = df['brc_number_of_employees'].replace('-', '')
df['brc_number_of_employees'] = df['brc_number_of_employees'].replace(np.nan, '')
df['brc_number_of_employees'] = pd.to_numeric(df['brc_number_of_employees'])


# Step 3: Merge all the features together with the development base.

loan_docs_base = p1_base_preparation.base

def merge(field, by, main_file, to_be_merged):
    to_be_merged = to_be_merged[['loan_code',
                                 field]]

    results = main_file.merge(to_be_merged,
                              left_on=by,
                              right_on=by,
                              how='left')
    return results

loan_docs_base = merge('brc_credit_payment_grade_score', 'loan_code', loan_docs_base, df)
loan_docs_base = merge('bri_payment_grade_score', 'loan_code', loan_docs_base, df)
loan_docs_base = merge('cbs_risk_grade_score', 'loan_code', loan_docs_base, df)
loan_docs_base = merge('brc_number_of_employees', 'loan_code', loan_docs_base, df)

print(loan_docs_base)
