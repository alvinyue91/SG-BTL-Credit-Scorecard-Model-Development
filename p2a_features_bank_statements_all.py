"""
02a Features (Bank Statements - All)

This script extracts the information from the borrowers' bank statements from datalake and defines the key features.
"""

from scripts import p1_base_preparation

import pandas as pd
from utils.config import get_config
from utils.database_service import DataService
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

datalake_config = get_config()['datalake_write']
datalake_db = DataService(datalake_config)

usd_sgd = 1.35


# Step 1: Extract all bank statements from datalake.

def get_bank_statements():
    df = datalake_db.query_postgres('''
    select
        c.company_name,
        b.id,
        b.borrower_id,
        a.dms_id,
        a.loan_code,
        a.bank,
        b.created_at as created_at_date,
        a.date as bank_statement_date,
        a.deposit,
        a.withdrawal,
        a.balance,
        a.currency,
        a.source
    from credit_scorecard.tbl_sg_bank_analysis a
    left join loan_db.loans b
    on a.loan_code=b.loan_code
    left join member_db.members c
    on b.borrower_id=c.id
    ;
    ''')
    df['created_at_date'] = df['created_at_date'].dt.date
    df = df[~(df.bank_statement_date > df.created_at_date)].reset_index(drop=True)

    return df

bank_statements = get_bank_statements()


# Step 2: Convert foreign currency USD to SGD and roll up bank accounts to the borrower level.

bank_statements['rate'] = [usd_sgd if x == 'USD' else 1 for x in bank_statements['currency']]

def convert_to_sgd(value, file):
    file[value + '_sgd'] = file[value] * file['rate']
    return file

bank_statements = convert_to_sgd('deposit', bank_statements)
bank_statements = convert_to_sgd('withdrawal', bank_statements)
bank_statements = convert_to_sgd('balance', bank_statements)

bank_statements_roll_up = bank_statements[['loan_code', 'created_at_date', 'bank_statement_date']].drop_duplicates().reset_index(drop=True)

def roll_up(value, file):
    df = file.groupby(['loan_code', 'bank_statement_date'], as_index=False)[value].sum()
    return df

def merge_to_base(by, main_file, to_be_merged):
    df = main_file.merge(to_be_merged,
                         on=by,
                         how='left')
    return df

bank_statements_roll_up = merge_to_base(['loan_code', 'bank_statement_date'], bank_statements_roll_up, roll_up('deposit_sgd', bank_statements))
bank_statements_roll_up = merge_to_base(['loan_code', 'bank_statement_date'], bank_statements_roll_up, roll_up('withdrawal_sgd', bank_statements))
bank_statements_roll_up = merge_to_base(['loan_code', 'bank_statement_date'], bank_statements_roll_up, roll_up('balance_sgd', bank_statements))


# Step 3: Select the appropriate months, i.e. created_at_date > bank_statement_date.

def difference_in_month(row):
    return (row['created_at_date'].year-row['bank_statement_date'].year)*12 + row['created_at_date'].month - row['bank_statement_date'].month

bank_statements_roll_up['order'] = bank_statements_roll_up.apply(difference_in_month, axis=1)


# Step 4: Define all the key features.

bank_statements_roll_up = bank_statements_roll_up[bank_statements_roll_up.order != 0]
bank_statements_roll_up_l6m = bank_statements_roll_up[~(bank_statements_roll_up.order > 6)]
bank_statements_roll_up_l3m = bank_statements_roll_up[~(bank_statements_roll_up.order > 3)]
bank_statements_roll_up_f3m = bank_statements_roll_up_l6m[~(bank_statements_roll_up_l6m.order < 4)]

def average_l6m(value, by, file):
    file[value] = pd.to_numeric(file[value])

    df = file.groupby([by], as_index=False)[value].mean()
    df = df.rename(columns={value: value + '_avg_l6m'})

    return df

def trend_f3m_l3m(value, by, file_f3m, file_l3m):
    file_f3m[value] = pd.to_numeric(file_f3m[value])
    file_l3m[value] = pd.to_numeric(file_l3m[value])

    df_f3m = file_f3m.groupby([by], as_index=False)[value].mean()
    df_l3m = file_l3m.groupby([by], as_index=False)[value].mean()

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

def volatility_l6m(value, by, file):
    file[value] = pd.to_numeric(file[value])

    df_max = file.groupby([by], as_index=False)[value].max()
    df_min = file.groupby([by], as_index=False)[value].min()

    df_max = df_max.rename(columns={value: value + '_max'})
    df_min = df_min.rename(columns={value: value + '_min'})

    df = df_max.merge(df_min,
                      on=by,
                      how='left')

    try:
        df[value + '_vol_l6m'] = (df[value + '_max']-df[value + '_min'])/df[value + '_max']
    except ZeroDivisionError:
        df[value + '_vol_l6m'] = float('NaN')

    return df

l6m_trend_deposit    = trend_f3m_l3m('deposit_sgd', 'loan_code', bank_statements_roll_up_f3m, bank_statements_roll_up_l3m)
l6m_vol_deposit      = volatility_l6m('deposit_sgd', 'loan_code', bank_statements_roll_up_l6m)
l6m_avg_deposit      = average_l6m('deposit_sgd', 'loan_code', bank_statements_roll_up_l6m)

l6m_trend_withdrawal = trend_f3m_l3m('withdrawal_sgd', 'loan_code', bank_statements_roll_up_f3m, bank_statements_roll_up_l3m)
l6m_vol_withdrawal   = volatility_l6m('withdrawal_sgd', 'loan_code', bank_statements_roll_up_l6m)
l6m_avg_withdrawal   = average_l6m('withdrawal_sgd', 'loan_code', bank_statements_roll_up_l6m)

l6m_trend_balance    = trend_f3m_l3m('balance_sgd', 'loan_code', bank_statements_roll_up_f3m, bank_statements_roll_up_l3m)
l6m_vol_balance      = volatility_l6m('balance_sgd', 'loan_code', bank_statements_roll_up_l6m)
l6m_avg_balance      = average_l6m('balance_sgd', 'loan_code', bank_statements_roll_up_l6m)


# Step 3: Merge all the features together with the development base.

bank_statements_base = p1_base_preparation.base

def merge(by, main_file, to_be_merged):
    df = main_file.merge(to_be_merged,
                         on=by,
                         how='left')
    return df

bank_statements_base = merge('loan_code', bank_statements_base, l6m_trend_deposit)
bank_statements_base = merge('loan_code', bank_statements_base, l6m_vol_deposit)
bank_statements_base = merge('loan_code', bank_statements_base, l6m_avg_deposit)
bank_statements_base = merge('loan_code', bank_statements_base, l6m_trend_withdrawal)
bank_statements_base = merge('loan_code', bank_statements_base, l6m_vol_withdrawal)
bank_statements_base = merge('loan_code', bank_statements_base, l6m_avg_withdrawal)
bank_statements_base = merge('loan_code', bank_statements_base, l6m_trend_balance)
bank_statements_base = merge('loan_code', bank_statements_base, l6m_vol_balance)
bank_statements_base = merge('loan_code', bank_statements_base, l6m_avg_balance)

print(bank_statements_base)
