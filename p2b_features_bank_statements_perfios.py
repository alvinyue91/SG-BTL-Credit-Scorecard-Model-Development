"""
02b Features (Bank Statements - Perfios)

This script extracts the information from the borrowers' Perfios bank statements and defines the key features.
"""

from scripts import p1_base_preparation
from scripts import p2a_features_bank_statements_all
from scripts import p9_perfios_borrower_id_list

import pandas as pd
import numpy as np
import os
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

usd_sgd = 1.35


# Step 1: Define the functions to extract the key summarised information from the Perfios bank statements.

def read_one_file(path):
    df_analysis = pd.read_excel(path, sheet_name='Analysis')

    borrower_name   = df_analysis.loc[df_analysis['Unnamed: 1'] == 'Name of the Account Holder', 'Unnamed: 2']
    address         = df_analysis.loc[df_analysis['Unnamed: 1'] == 'Address'                   , 'Unnamed: 2']
    bank_name       = df_analysis.loc[df_analysis['Unnamed: 1'] == 'Name of the Bank'          , 'Unnamed: 2']
    account_no      = df_analysis.loc[df_analysis['Unnamed: 1'] == 'Account Number'            , 'Unnamed: 2']
    currency        = df_analysis.loc[df_analysis['Unnamed: 1'] == 'PAN'                       , 'Unnamed: 2']

    first_index     = df_analysis.loc[df_analysis["Unnamed: 1"] == "Total No. of Credit Transactions"].index

    df_analysis = df_analysis.iloc[first_index[0] - 1:, 1:-1]
    df_analysis.rename(columns={'Unnamed: 1': 'index'}, inplace=True)
    df_analysis = df_analysis.set_index('index').T.reset_index(drop=True)
    df_analysis = df_analysis.rename(columns={np.nan: "date",
                                              'Total No. of Credit Transactions': 'total_no_credit_txn',
                                              'Total Amount of Credit Transactions': 'total_amt_credit_txn',
                                              'Percentage Swing of MoM Deposit ': 'pct_swing_mom_deposit',
                                              'Total No. of Debit Transactions': 'total_no_debit_txn',
                                              'Total Amount of Debit Transactions': 'total_amt_debit_txn',
                                              'Total No. of Cash Deposits': 'total_no_cash_deposits',
                                              'Total Amount of Cash Deposits': 'total_amt_cash_deposits',
                                              '% of Cash deposit amt to total credit amt ': 'pct_cash_deposit_amt_to_total_credit_amt',
                                              'Total No. of Cash Withdrawals': 'total_no_cash_withdrawals',
                                              'Total Amount of Cash Withdrawals': 'total_amt_cash_withdrawals',
                                              'Total No. of Cheque Deposits': 'total_no_cheque_deposits',
                                              'Total Amount of Cheque Deposits': 'total_amt_cheque_deposits',
                                              '% of Cheque deposit amt to total credit amt': 'pct_cheque_deposit_amt_to_total_credit_amt',
                                              'Total number of Card deposits': 'total_no_card_deposits',
                                              'Total amount of Card deposits': 'total_amt_card_deposits',
                                              '% of Card deposit amt to total credit amt ': 'pct_card_deposit_amt_to_total_credit_amt',
                                              'Total No. of Cheque Issues': 'total_no_cheque_issues',
                                              'Total Amount of Cheque Issues': 'total_amt_cheque_issues',
                                              'Total No. of Inward Cheque Bounces': 'total_no_inward_cheque_bounces',
                                              'Total No. of Outward Cheque Bounces': 'total_no_outward_cheque_bounces',
                                              'Total number of Card debits': 'total_no_card_debits',
                                              'Total amount of Card debits': 'total_amt_card_debits',
                                              'Balance as on 1st': 'balance_as_on_1st',
                                              'Balance as on 15th': 'balance_as_on_15th',
                                              'Min EOD Balance': 'min_eod_balance',
                                              'Dates of Min EOD Balance': 'dates_of_min_eod_balance',
                                              'Max EOD Balance': 'max_eod_balance',
                                              'Date of the Max EOD Balance ': 'dates_of_max_eod_balance',
                                              'Average EOD Balance': 'average_eod_balance',
                                              'Average Bank Balance': 'average_bank_balance'}).reset_index()

    df_analysis = df_analysis[df_analysis.date != 'TOTAL']
    df_analysis['order'] = max(df_analysis.iloc[:, 0]) - df_analysis['index']
    df_analysis = df_analysis.iloc[:, 1:]

    df_analysis['borrower_name']    = borrower_name.item()
    df_analysis['address']          = address.item()
    df_analysis['bank_name']        = bank_name.item()
    df_analysis['account_number']   = account_no.item()
    df_analysis['currency']         = currency.item()

    return df_analysis

def read_multiple_files(folder):
    folder_name = os.listdir(folder)
    files_xlsx = [f for f in folder_name if f[-4:] == 'xlsx' and f[0:2] != '~$']

    df = pd.DataFrame()
    count = 1

    for f in files_xlsx:
        df_single = read_one_file(folder + f)
        df_single['perfios_id_by_co'] = count
        count += 1
        df = df.append(df_single)

    return df


# Step 2: Extract the key information from all the historical Perfios bank statements in the respective folders.

df_candy = read_multiple_files('/Users/alvin.yue/Documents/LOS/Data Preparation/Bank Statements/Perfios Files/Candy/')
df_candy['credit_officer'] = 'Candy'

df_gabriel = read_multiple_files('/Users/alvin.yue/Documents/LOS/Data Preparation/Bank Statements/Perfios Files/Gabriel/')
df_gabriel['credit_officer'] = 'Gabriel'

df_joseph = read_multiple_files('/Users/alvin.yue/Documents/LOS/Data Preparation/Bank Statements/Perfios Files/Joseph/')
df_joseph['credit_officer'] = 'Joseph'

df_novabelle = read_multiple_files('/Users/alvin.yue/Documents/LOS/Data Preparation/Bank Statements/Perfios Files/Novabelle/')
df_novabelle['credit_officer'] = 'Novabelle'

df_valerie = read_multiple_files('/Users/alvin.yue/Documents/LOS/Data Preparation/Bank Statements/Perfios Files/Valerie/')
df_valerie['credit_officer'] = 'Valerie'

perfios_combined = pd.concat([df_candy,
                              df_gabriel,
                              df_joseph,
                              df_novabelle,
                              df_valerie], axis=0)


# Step 3: Import Perfios borrower ID list and merge with the extracted information from the bank statements.

perfios_combined = perfios_combined.merge(p9_perfios_borrower_id_list.borrower_id,
                                          on='borrower_name',
                                          how='left')


# Step 4: Convert foreign currency USD to SGD and roll up bank accounts to the borrower level.

perfios_combined['rate'] = [usd_sgd if x == 'USD' else 1 for x in perfios_combined['currency']]
perfios_combined['date'] = perfios_combined['date'].dt.date

def convert_to_sgd(value, file):
    file[value + '_sgd'] = file[value] * file['rate']
    return file

perfios_combined = convert_to_sgd('average_bank_balance', perfios_combined)
perfios_combined = convert_to_sgd('average_eod_balance', perfios_combined)
perfios_combined = convert_to_sgd('balance_as_on_15th', perfios_combined)
perfios_combined = convert_to_sgd('balance_as_on_1st', perfios_combined)
perfios_combined = convert_to_sgd('max_eod_balance', perfios_combined)
perfios_combined = convert_to_sgd('min_eod_balance', perfios_combined)
perfios_combined = convert_to_sgd('total_amt_card_debits', perfios_combined)
perfios_combined = convert_to_sgd('total_amt_card_deposits', perfios_combined)
perfios_combined = convert_to_sgd('total_amt_cash_deposits', perfios_combined)
perfios_combined = convert_to_sgd('total_amt_cash_withdrawals', perfios_combined)
perfios_combined = convert_to_sgd('total_amt_cheque_deposits', perfios_combined)
perfios_combined = convert_to_sgd('total_amt_cheque_issues', perfios_combined)
perfios_combined = convert_to_sgd('total_amt_credit_txn', perfios_combined)
perfios_combined = convert_to_sgd('total_amt_debit_txn', perfios_combined)

perfios_roll_up = perfios_combined[['id', 'date']].drop_duplicates().reset_index(drop=True)

def roll_up(value, file):
    df = file.groupby(['id', 'date'], as_index=False)[value].sum()
    return df

def merge_to_base(by, main_file, to_be_merged):
    df = main_file.merge(to_be_merged,
                         on=by,
                         how='left')
    return df

perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('average_bank_balance_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('average_eod_balance_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('balance_as_on_15th_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('balance_as_on_1st_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('max_eod_balance_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('min_eod_balance_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_amt_card_debits_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_amt_card_deposits_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_amt_cash_deposits_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_amt_cash_withdrawals_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_amt_cheque_deposits_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_amt_cheque_issues_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_amt_credit_txn_sgd', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_amt_debit_txn_sgd', perfios_combined))

perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_card_debits', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_card_deposits', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_cash_deposits', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_cash_withdrawals', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_cheque_deposits', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_cheque_issues', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_credit_txn', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_debit_txn', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_inward_cheque_bounces', perfios_combined))
perfios_roll_up = merge_to_base(['id', 'date'], perfios_roll_up, roll_up('total_no_outward_cheque_bounces', perfios_combined))

perfios_roll_up['pct_card_deposit_to_total_credit']   = perfios_roll_up['total_amt_card_deposits_sgd'] / perfios_roll_up['total_amt_credit_txn_sgd']
perfios_roll_up['pct_cash_deposit_to_total_credit']   = perfios_roll_up['total_amt_cash_deposits_sgd'] / perfios_roll_up['total_amt_credit_txn_sgd']
perfios_roll_up['pct_cheque_deposit_to_total_credit'] = perfios_roll_up['total_amt_cheque_deposits_sgd'] / perfios_roll_up['total_amt_credit_txn_sgd']


# Step 5: Merge with the development base and select the appropriate months, i.e. created_at_date > bank_statement_date.

development_base = p1_base_preparation.base[['borrower_id', 'loan_code', 'created_at']]

perfios_roll_up = perfios_roll_up.merge(development_base,
                                        left_on='id',
                                        right_on='borrower_id',
                                        how='left')

perfios_roll_up = perfios_roll_up.dropna(subset=['loan_code'])
perfios_roll_up['created_at'] = perfios_roll_up['created_at'].dt.date
perfios_roll_up = perfios_roll_up[~(perfios_roll_up.date > perfios_roll_up.created_at)]

def difference_in_month(row):
    return (row['created_at'].year-row['date'].year)*12 + row['created_at'].month - row['date'].month

perfios_roll_up['order'] = perfios_roll_up.apply(difference_in_month, axis=1)


# Step 6: Define all the key features.

perfios_roll_up = perfios_roll_up[perfios_roll_up.order != 0]
perfios_roll_up_l6m = perfios_roll_up[~(perfios_roll_up.order > 6)]
perfios_roll_up_l3m = perfios_roll_up[~(perfios_roll_up.order > 3)]
perfios_roll_up_f3m = perfios_roll_up_l6m[~(perfios_roll_up_l6m.order < 4)]

def average_l6m(value, by, file):
    file[value] = pd.to_numeric(file[value])

    df = file.groupby([by], as_index=False)[value].mean()
    df = df.rename(columns={value: value + '_avg_l6m'})

    return df

def total_l6m(value, by, file):
    file[value] = pd.to_numeric(file[value])

    df = file.groupby([by], as_index=False)[value].sum()
    df = df.rename(columns={value: value + '_sum_l6m'})

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

l6m_avg_no_credit_txn       = average_l6m('total_no_credit_txn', 'loan_code', perfios_roll_up_l6m)
l6m_avg_no_debit_txn        = average_l6m('total_no_debit_txn', 'loan_code', perfios_roll_up_l6m)

l6m_trend_amt_credit_txn    = trend_f3m_l3m('total_amt_credit_txn_sgd', 'loan_code', perfios_roll_up_f3m, perfios_roll_up_l3m)
l6m_vol_amt_credit_txn      = volatility_l6m('total_amt_credit_txn_sgd', 'loan_code', perfios_roll_up_l6m)
l6m_avg_amt_credit_txn      = average_l6m('total_amt_credit_txn_sgd', 'loan_code', perfios_roll_up_l6m)

l6m_trend_amt_debit_txn     = trend_f3m_l3m('total_amt_debit_txn_sgd', 'loan_code', perfios_roll_up_f3m, perfios_roll_up_l3m)
l6m_vol_amt_debit_txn       = volatility_l6m('total_amt_debit_txn_sgd', 'loan_code', perfios_roll_up_l6m)
l6m_avg_amt_debit_txn       = average_l6m('total_amt_debit_txn_sgd', 'loan_code', perfios_roll_up_l6m)

l6m_total_no_inward_cheque_bounces  = total_l6m('total_no_inward_cheque_bounces', 'loan_code', perfios_roll_up_l6m)
l6m_total_no_outward_cheque_bounces = total_l6m('total_no_outward_cheque_bounces', 'loan_code', perfios_roll_up_l6m)

l6m_trend_eod_balance       = trend_f3m_l3m('average_eod_balance_sgd', 'loan_code', perfios_roll_up_f3m, perfios_roll_up_l3m)
l6m_vol_eod_balance         = volatility_l6m('average_eod_balance_sgd', 'loan_code', perfios_roll_up_l6m)
l6m_avg_eod_balance         = average_l6m('average_eod_balance_sgd', 'loan_code', perfios_roll_up_l6m)

l6m_trend_bank_balance      = trend_f3m_l3m('average_bank_balance_sgd', 'loan_code', perfios_roll_up_f3m, perfios_roll_up_l3m)
l6m_vol_bank_balance        = volatility_l6m('average_bank_balance_sgd', 'loan_code', perfios_roll_up_l6m)
l6m_avg_bank_balance        = average_l6m('average_bank_balance_sgd', 'loan_code', perfios_roll_up_l6m)

l6m_avg_pct_cash_to_credit      = average_l6m('pct_cash_deposit_to_total_credit', 'loan_code', perfios_roll_up_l6m)
l6m_avg_pct_cheque_to_credit    = average_l6m('pct_cheque_deposit_to_total_credit', 'loan_code', perfios_roll_up_l6m)
l6m_avg_pct_card_to_credit      = average_l6m('pct_card_deposit_to_total_credit', 'loan_code', perfios_roll_up_l6m)


# Step 5: Merge all the features together.

perfios_features = perfios_roll_up[['id',
                                    'loan_code',
                                    'created_at']].drop_duplicates().reset_index(drop=True)

def merge(by, main_file, to_be_merged):
    df = main_file.merge(to_be_merged,
                         on=by,
                         how='left')
    return df

perfios_features = merge('loan_code', perfios_features, l6m_avg_no_credit_txn)
perfios_features = merge('loan_code', perfios_features, l6m_avg_no_debit_txn)
perfios_features = merge('loan_code', perfios_features, l6m_trend_amt_credit_txn)
perfios_features = merge('loan_code', perfios_features, l6m_vol_amt_credit_txn)
perfios_features = merge('loan_code', perfios_features, l6m_avg_amt_credit_txn)
perfios_features = merge('loan_code', perfios_features, l6m_trend_amt_debit_txn)
perfios_features = merge('loan_code', perfios_features, l6m_vol_amt_debit_txn)
perfios_features = merge('loan_code', perfios_features, l6m_avg_amt_debit_txn)
perfios_features = merge('loan_code', perfios_features, l6m_total_no_inward_cheque_bounces)
perfios_features = merge('loan_code', perfios_features, l6m_total_no_outward_cheque_bounces)
perfios_features = merge('loan_code', perfios_features, l6m_trend_eod_balance)
perfios_features = merge('loan_code', perfios_features, l6m_vol_eod_balance)
perfios_features = merge('loan_code', perfios_features, l6m_avg_eod_balance)
perfios_features = merge('loan_code', perfios_features, l6m_trend_bank_balance)
perfios_features = merge('loan_code', perfios_features, l6m_vol_bank_balance)
perfios_features = merge('loan_code', perfios_features, l6m_avg_bank_balance)
perfios_features = merge('loan_code', perfios_features, l6m_avg_pct_cash_to_credit)
perfios_features = merge('loan_code', perfios_features, l6m_avg_pct_cheque_to_credit)
perfios_features = merge('loan_code', perfios_features, l6m_avg_pct_card_to_credit)


# Step 7: Merge the Perfios features with the development base.

perfios_base = p1_base_preparation.base
perfios_base = perfios_base.drop(['id', 'created_at'], axis=1)
perfios_base = perfios_base.merge(perfios_features,
                                  on='loan_code',
                                  how='left')


# Step 8: Combine key columns (e.g. credit and debit transactions trend) together.

bank_statements_base = p2a_features_bank_statements_all.bank_statements_base[['loan_code',
                                                                              'deposit_sgd_trend_l6m',
                                                                              'deposit_sgd_vol_l6m',
                                                                              'deposit_sgd_avg_l6m',
                                                                              'withdrawal_sgd_trend_l6m',
                                                                              'withdrawal_sgd_vol_l6m',
                                                                              'withdrawal_sgd_avg_l6m',
                                                                              'balance_sgd_trend_l6m',
                                                                              'balance_sgd_vol_l6m',
                                                                              'balance_sgd_avg_l6m']]

perfios_base = perfios_base.merge(bank_statements_base,
                                  on='loan_code',
                                  how='left')

print(perfios_roll_up)
