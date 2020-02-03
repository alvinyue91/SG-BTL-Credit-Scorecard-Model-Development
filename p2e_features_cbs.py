"""
02e Features (CBS Bureau Reports)

This script extracts the information from the borrowers' CBS bureau reports and defines the key features.
"""

from scripts import p1_base_preparation

import pandas as pd
from utils.config import get_config
from utils.database_service import DataService
from pandas.io.json import json_normalize
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

datalake_config = get_config()['datalake_write']
datalake_db = DataService(datalake_config)


# Step 1: Extract all key CBS fields from datalake.

cbs_account_status_non_s3_raw = datalake_db.query_postgres('''
select
    borrower_id,
    cbs_request_date,
    blob::JSON->>'cbs_individual_id_number' as cbs_individual_id_number,
    blob::JSON->>'cbs_risk_grade' as cbs_risk_grade,
    blob::JSON->>'cbs_number_of_accounts' as cbs_number_of_accounts,
    JSON_ARRAY_ELEMENTS(blob::JSON->'cbs_account_status') as cbs_account_status
from automated_sg_btl.cbs_enquiries_non_s3
order by borrower_id
;
''')

cbs_account_status_s3_raw = datalake_db.query_postgres('''
select
    loan_id,
    blob::JSON->>'cbs_individual_id_number' as cbs_individual_id_number,
    blob::JSON->>'cbs_risk_grade' as cbs_risk_grade,
    blob::JSON->>'cbs_number_of_accounts' as cbs_number_of_accounts,
    JSON_ARRAY_ELEMENTS(blob::JSON->'cbs_account_status') as cbs_account_status
from automated_sg_btl.cbs_enquiries
order by loan_id, created_time desc
;
''')

cbs_balances_non_s3_raw = datalake_db.query_postgres('''
select
    borrower_id,
    cbs_request_date,
    blob::JSON->>'cbs_individual_id_number' as cbs_individual_id_number,
    blob::JSON->>'cbs_risk_grade' as cbs_risk_grade,
    blob::JSON->>'cbs_number_of_accounts' as cbs_number_of_accounts,
    JSON_ARRAY_ELEMENTS(blob::JSON->'cbs_unsecured_balances_interest_bearing_l6m') as cbs_unsecured_balances_interest_bearing_l6m
from automated_sg_btl.cbs_enquiries_non_s3
order by borrower_id
;
''')

cbs_balances_s3_raw = datalake_db.query_postgres('''
select
    loan_id,
    blob::JSON->>'cbs_individual_id_number' as cbs_individual_id_number,
    blob::JSON->>'cbs_risk_grade' as cbs_risk_grade,
    blob::JSON->>'cbs_number_of_accounts' as cbs_number_of_accounts,
    JSON_ARRAY_ELEMENTS(blob::JSON->'cbs_unsecured_balances_interest_bearing_l6m') as cbs_unsecured_balances_interest_bearing_l6m
from automated_sg_btl.cbs_enquiries
order by loan_id, created_time desc
;
''')

cbs_account_status_non_s3 = pd.concat([cbs_account_status_non_s3_raw[['borrower_id',
                                                                      'cbs_request_date',
                                                                      'cbs_individual_id_number',
                                                                      'cbs_risk_grade',
                                                                      'cbs_number_of_accounts']],
                                       json_normalize(cbs_account_status_non_s3_raw['cbs_account_status'])],
                                      axis=1)

cbs_account_status_s3 = pd.concat([cbs_account_status_s3_raw[['loan_id',
                                                              'cbs_individual_id_number',
                                                              'cbs_risk_grade',
                                                              'cbs_number_of_accounts']],
                                   json_normalize(cbs_account_status_s3_raw['cbs_account_status'])],
                                  axis=1)

cbs_balances_non_s3 = pd.concat([cbs_balances_non_s3_raw[['borrower_id',
                                                          'cbs_request_date',
                                                          'cbs_individual_id_number',
                                                          'cbs_risk_grade',
                                                          'cbs_number_of_accounts']],
                                 json_normalize(cbs_balances_non_s3_raw['cbs_unsecured_balances_interest_bearing_l6m'])],
                                axis=1)

cbs_balances_s3 = pd.concat([cbs_balances_s3_raw[['loan_id',
                                                  'cbs_individual_id_number',
                                                  'cbs_risk_grade',
                                                  'cbs_number_of_accounts']],
                             json_normalize(cbs_balances_s3_raw['cbs_unsecured_balances_interest_bearing_l6m'])],
                            axis=1)


# Step 2: Rename key columns for account status, i.e. cash advance, full payment and balance transfer.

cbs_account_status_non_s3 = cbs_account_status_non_s3.rename(columns={'cash_advance': 'payment_behaviour',
                                                                      'balance_transfer': 'cash_advance_bt'})

cbs_account_status_s3 = cbs_account_status_s3.rename(columns={'cash_advance': 'payment_behaviour',
                                                              'balance_transfer': 'cash_advance_bt'})


# Step 3: Exclude unnecessary information and perform data cleaning, e.g. accounts that closed, last 6 months only.

cbs_account_status_non_s3['date_closed'] = pd.to_datetime(cbs_account_status_non_s3['date_closed'])
cbs_account_status_non_s3 = cbs_account_status_non_s3[cbs_account_status_non_s3.date_closed.isnull()]
cbs_account_status_non_s3['cc_ind'] = [1 if 'unsecured credit card' in x else 0 for x in cbs_account_status_non_s3['product_bank_type']]
cbs_account_status_non_s3['payment_behaviour_l6m'] = [x[:6] for x in cbs_account_status_non_s3['payment_behaviour']]
cbs_account_status_non_s3['full_payment_l6m']      = ['' if not x else x[:6] for x in cbs_account_status_non_s3['full_payment']]
cbs_account_status_non_s3['cash_advance_bt_l6m']   = ['' if not x else x[:6] for x in cbs_account_status_non_s3['cash_advance_bt']]
cbs_account_status_non_s3 = cbs_account_status_non_s3.reset_index(drop=True)

exclude_account_status_s3 = ['03/06/2018nnyyyyyyyyyy',
                             'diners04/07/20190//**2*2*']
cbs_account_status_s3 = cbs_account_status_s3.loc[~cbs_account_status_s3['date_closed'].isin(exclude_account_status_s3)]
cbs_account_status_s3['date_closed'] = pd.to_datetime(cbs_account_status_s3['date_closed'])
cbs_account_status_s3 = cbs_account_status_s3[cbs_account_status_s3.date_closed.isnull()]
cbs_account_status_s3['cc_ind'] = [1 if 'unsecured credit card' in x else 0 for x in cbs_account_status_s3['product_bank_type']]
cbs_account_status_s3['payment_behaviour_l6m'] = [x[:6] for x in cbs_account_status_s3['payment_behaviour']]
cbs_account_status_s3['full_payment_l6m']      = ['' if not x else x[:6] for x in cbs_account_status_s3['full_payment']]
cbs_account_status_s3['cash_advance_bt_l6m']   = ['' if not x else x[:6] for x in cbs_account_status_s3['cash_advance_bt']]
cbs_account_status_s3 = cbs_account_status_s3.reset_index(drop=True)

cbs_balances_non_s3['year_month'] = pd.to_datetime(cbs_balances_non_s3['year_month'])
cbs_balances_non_s3 = cbs_balances_non_s3[~(cbs_balances_non_s3.cbs_request_date.isnull())]
cbs_balances_non_s3.sort_values(['borrower_id', 'cbs_request_date', 'cbs_individual_id_number', 'year_month'], ascending=True, inplace=True)
cbs_balances_non_s3['order'] = cbs_balances_non_s3.groupby(['borrower_id', 'cbs_request_date', 'cbs_individual_id_number']).cumcount()
cbs_balances_non_s3 = cbs_balances_non_s3.reset_index(drop=True)

exclude_balances_s3 = ['disc la',
                       '100 1997',
                       '1997 ?',
                       'of kin',
                       'engineering pte',
                       'ltd (?kin',
                       'engineering?) supply',
                       'unsecured cc',
                       'ggregated month']
cbs_balances_s3 = cbs_balances_s3.loc[~cbs_balances_s3['year_month'].isin(exclude_balances_s3)]
cbs_balances_s3['year_month'] = pd.to_datetime(cbs_balances_s3['year_month'])
cbs_balances_s3 = cbs_balances_s3[~cbs_balances_s3.unsecured_bal_int_bearing.str.contains('-')]
cbs_balances_s3.sort_values(['loan_id', 'cbs_individual_id_number', 'year_month'], ascending=True, inplace=True)
cbs_balances_s3['order'] = cbs_balances_s3.groupby(['loan_id', 'cbs_individual_id_number']).cumcount()
cbs_balances_s3 = cbs_balances_s3.reset_index(drop=True)


# Step 4a: Define all the key features for balances, i.e. L6M Average UIBB, L6M Trend UIBB.

cbs_balances_non_s3_l6m = cbs_balances_non_s3[~(cbs_balances_non_s3.order >= 6)]
cbs_balances_non_s3_f3m = cbs_balances_non_s3[~(cbs_balances_non_s3.order >= 3)]
cbs_balances_non_s3_l3m = cbs_balances_non_s3_l6m[~(cbs_balances_non_s3_l6m.order <= 2)]

cbs_balances_s3_l6m = cbs_balances_s3[~(cbs_balances_s3.order >= 6)]
cbs_balances_s3_f3m = cbs_balances_s3[~(cbs_balances_s3.order >= 3)]
cbs_balances_s3_l3m = cbs_balances_s3_l6m[~(cbs_balances_s3_l6m.order <= 2)]

def average_l6m(value, by, file):
    file[value] = pd.to_numeric(file[value])

    df = file.groupby(by, as_index=False)[value].mean()
    df = df.rename(columns={value: value + '_avg_l6m'})

    return df

def trend_f3m_l3m(value, by, file_f3m, file_l3m):
    file_f3m[value] = pd.to_numeric(file_f3m[value])
    file_l3m[value] = pd.to_numeric(file_l3m[value])

    df_f3m = file_f3m.groupby(by, as_index=False)[value].mean()
    df_l3m = file_l3m.groupby(by, as_index=False)[value].mean()

    df_f3m = df_f3m.rename(columns={value: value + '_f3m'})
    df_l3m = df_l3m.rename(columns={value: value + '_l3m'})

    df = df_f3m.merge(df_l3m,
                      on=by,
                      how='left')

    try:
        df[value + '_trend_l6m'] = df[value + '_l3m']/df[value + '_f3m']-1
    except ZeroDivisionError:
        df[value + '_trend_l6m'] = float('NaN')

    return df

l6m_avg_uibb_non_s3 = average_l6m('unsecured_bal_int_bearing',
                                  ['borrower_id', 'cbs_request_date', 'cbs_individual_id_number'],
                                  cbs_balances_non_s3_l6m)

l6m_trend_uibb_non_s3 = trend_f3m_l3m('unsecured_bal_int_bearing',
                                      ['borrower_id', 'cbs_request_date', 'cbs_individual_id_number'],
                                      cbs_balances_non_s3_f3m,
                                      cbs_balances_non_s3_l3m)

l6m_avg_uibb_s3 = average_l6m('unsecured_bal_int_bearing',
                              ['loan_id', 'cbs_individual_id_number'],
                              cbs_balances_s3_l6m)

l6m_trend_uibb_s3 = trend_f3m_l3m('unsecured_bal_int_bearing',
                                  ['loan_id', 'cbs_individual_id_number'],
                                  cbs_balances_s3_f3m,
                                  cbs_balances_s3_l3m)


# Step 4b: Define all the key features for account status, i.e. L6M Cash Adv Ind, L6M Partial Payment Count, L6M Payment Status.

def partial_payment_count_l6m(by, file):
    file['month6_pp'] = [1 if x[0:1] == 'n' else 0 for x in file['full_payment_l6m']]
    file['month5_pp'] = [1 if x[1:2] == 'n' else 0 for x in file['full_payment_l6m']]
    file['month4_pp'] = [1 if x[2:3] == 'n' else 0 for x in file['full_payment_l6m']]
    file['month3_pp'] = [1 if x[3:4] == 'n' else 0 for x in file['full_payment_l6m']]
    file['month2_pp'] = [1 if x[4:5] == 'n' else 0 for x in file['full_payment_l6m']]
    file['month1_pp'] = [1 if x[5:6] == 'n' else 0 for x in file['full_payment_l6m']]

    df = file.groupby(by, as_index=False)['month6_pp',
                                          'month5_pp',
                                          'month4_pp',
                                          'month3_pp',
                                          'month2_pp',
                                          'month1_pp'].max()

    df['partial_payment_count_l6m'] = df['month6_pp'] + df['month5_pp'] + df['month4_pp'] + df['month3_pp'] + df['month2_pp'] + df['month1_pp']

    return df

def cash_adv_ind(by, file):
    file['cash_adv_ind_l6m'] = [1 if 'y' in x else 0 for x in file['cash_advance_bt_l6m']]

    df = file.groupby(by, as_index=False)['cash_adv_ind_l6m'].max()

    return df

def worst_dq_status_l6m(by, file):
    file['worst_b'] = [2 if 'b' in x else 1 for x in file['payment_behaviour_l6m']]
    file['worst_c'] = [3 if 'c' in x else 1 for x in file['payment_behaviour_l6m']]
    file['worst_d'] = [4 if 'd' in x else 1 for x in file['payment_behaviour_l6m']]
    file['worst_r'] = [4 if 'r' in x else 1 for x in file['payment_behaviour_l6m']]
    file['worst_s'] = [4 if 's' in x else 1 for x in file['payment_behaviour_l6m']]
    file['worst_w'] = [4 if 'w' in x else 1 for x in file['payment_behaviour_l6m']]

    file['worst_dq_status_l6m'] = file[['worst_b', 'worst_c', 'worst_d', 'worst_r', 'worst_s', 'worst_w']].max(axis=1)

    df = file.groupby(by, as_index=False)['worst_dq_status_l6m'].max()

    return df

l6m_partial_payment_count_non_s3 = partial_payment_count_l6m(['borrower_id', 'cbs_request_date', 'cbs_individual_id_number'],
                                                             cbs_account_status_non_s3)

l6m_cash_adv_ind_non_s3 = cash_adv_ind(['borrower_id', 'cbs_request_date', 'cbs_individual_id_number'],
                                       cbs_account_status_non_s3)

l6m_worst_dq_status_non_s3 = worst_dq_status_l6m(['borrower_id', 'cbs_request_date', 'cbs_individual_id_number'],
                                                 cbs_account_status_non_s3)

l6m_partial_payment_count_s3 = partial_payment_count_l6m(['loan_id', 'cbs_individual_id_number'],
                                                         cbs_account_status_s3)

l6m_cash_adv_ind_s3 = cash_adv_ind(['loan_id', 'cbs_individual_id_number'],
                                   cbs_account_status_s3)

l6m_worst_dq_status_s3 = worst_dq_status_l6m(['loan_id', 'cbs_individual_id_number'],
                                             cbs_account_status_s3)


# Step 5: Merge all the features together with the development base.

cbs_base = p1_base_preparation.base
cbs_base = cbs_base.rename(columns={'id': 'loan_id'})

def merge(by, main_file, to_be_merged, join_type):
    df = main_file.merge(to_be_merged,
                         on=by,
                         how=join_type)
    return df

cbs_non_s3 = merge(['borrower_id', 'cbs_request_date', 'cbs_individual_id_number'], l6m_avg_uibb_non_s3, l6m_trend_uibb_non_s3, 'outer')
cbs_non_s3 = merge(['borrower_id', 'cbs_request_date', 'cbs_individual_id_number'], cbs_non_s3, l6m_partial_payment_count_non_s3, 'outer')
cbs_non_s3 = merge(['borrower_id', 'cbs_request_date', 'cbs_individual_id_number'], cbs_non_s3, l6m_cash_adv_ind_non_s3, 'outer')
cbs_non_s3 = merge(['borrower_id', 'cbs_request_date', 'cbs_individual_id_number'], cbs_non_s3, l6m_worst_dq_status_non_s3, 'outer')

cbs_base = merge('loan_id', cbs_base, l6m_avg_uibb_s3, 'left')
cbs_base = merge('loan_id', cbs_base, l6m_trend_uibb_s3, 'left')
cbs_base = merge('loan_id', cbs_base, l6m_partial_payment_count_s3, 'left')
cbs_base = merge('loan_id', cbs_base, l6m_cash_adv_ind_s3, 'left')
cbs_base = merge('loan_id', cbs_base, l6m_worst_dq_status_s3, 'left')

print(cbs_base)
