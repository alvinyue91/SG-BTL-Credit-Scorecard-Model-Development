"""
02c Features (Financial Statements)

This script extracts all the key features from the borrowers' financial statements from datalake.
"""

from scripts import p1_base_preparation

import pandas as pd
import numpy as np
from utils.config import get_config
from utils.database_service import DataService
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

datalake_config = get_config()['datalake_write']
datalake_db = DataService(datalake_config)


# Step 1: Extract all key financial fields from datalake for both balance sheet and income statement.

def get_financials():
    balance_sheet_raw = datalake_db.query_postgres('''
    SELECT DISTINCT ON (b.borrower_id, a.date, a.financial_metric)
        b.borrower_id,
        a.date,
        a.financial_metric,
        a.value
    FROM (
        SELECT DISTINCT ON (loan_code, date, level_3)
            loan_code,
            date,
            CASE WHEN level_3 LIKE 'current asset%%' THEN 'current_assets'
                 WHEN level_3 LIKE 'current liabilities%%' THEN 'current_liabilities'
                 WHEN level_3 SIMILAR TO '%%(trade receivables|trade creditors)%%' THEN 'trade_receivables'
                 WHEN level_3 LIKE '%%trade payables%%' THEN 'trade_payables'
                 WHEN level_3 LIKE '%%inventor%%' THEN 'inventory'
                 WHEN level_3 LIKE '%%total liabil%%' THEN 'total_liabilities'
                 WHEN level_3 LIKE '%%total asset%%' THEN 'total_assets'
                 WHEN level_3 LIKE '%%total net worth%%' THEN 'total_net_worth'
                 WHEN level_3 SIMILAR TO '%%(propert|plant & equipment)%%' THEN 'property_plant_equipment'
                 WHEN level_3 LIKE '%%cash and bank%%' THEN 'cash_and_cash_equivalent'
                 WHEN level_3 LIKE '%%retained profits%%' THEN 'retained_earnings'
                 WHEN level_3 LIKE '%%share capital%%' THEN 'share_capital'
                 ELSE level_3 END as financial_metric,
            value
        FROM credit_scorecard.tbl_sg_balance_sheet
        WHERE level_3 SIMILAR TO '%%(current asset|current liabil|trade creditors|trade receivables|trade payables|inventor|total liabil|total asset|total net worth|propert|plant & equipment|cash and bank|retained profits|share capital)%%'
        AND level_3 NOT IN ('non-current assets', 'non-current liabilities', 'total liabilities & equities','total liabilities and equity','investment property')
        ORDER BY loan_code, date, level_3, created_time DESC) a
    INNER JOIN (
        SELECT
            loan_code,
            borrower_id,
            product_id,
            status,
            created_at
        FROM loan_db.loans
        WHERE country_code='SG') b
    ON a.loan_code=b.loan_code
    ORDER BY b.borrower_id, a.date, a.financial_metric, b.created_at DESC
    ;
    ''')

    balance_sheet = pd.pivot_table(balance_sheet_raw, index=['borrower_id','date'], columns=['financial_metric'], aggfunc=np.max).reset_index()
    balance_sheet.columns = [col for col_array in balance_sheet.columns for col in col_array if col not in ['','value']]

    balance_sheet.sort_values(['borrower_id','date'], ascending=False, inplace=True)
    balance_sheet['recency_order_balance_sheet'] = balance_sheet.groupby('borrower_id').cumcount()

    income_statement_raw = datalake_db.query_postgres('''
    SELECT DISTINCT ON (b.borrower_id, a.date, a.financial_metric)
        b.borrower_id,
        a.date,
        a.financial_metric,
        a.value
    FROM (
        SELECT DISTINCT ON (loan_code, date, level_2)
            loan_code,
            date,
            CASE WHEN level_2 LIKE '%%revenue%%' THEN 'revenue'
                 WHEN level_2 LIKE '%%ebit%%' THEN 'ebit'
                 WHEN level_2 LIKE '%%ebt%%' THEN 'ebt'
                 WHEN level_2 LIKE '%%operating profit%%' THEN 'operating_profit'
                 WHEN level_2 LIKE '%%cost of sales%%' THEN 'cost_of_sales'
                 WHEN level_2 LIKE '%%depreciation%%' THEN 'depreciation_and_or_amortisation'
                 WHEN level_2 LIKE '%%gross profit%%' THEN 'gross_profit'
                 WHEN level_2 SIMILAR TO '%%(interest & taxes|interest and taxes)%%' THEN 'interest_and_taxes'
                 WHEN level_2 LIKE '%%interest expenses%%' THEN 'interest_expenses'
                 WHEN level_2 LIKE '%%net profit%%' THEN 'net_profit_or_loss'
                 WHEN level_2 LIKE '%%income taxes%%' THEN 'taxes'
                 ELSE level_2 END as financial_metric,
            value
        FROM credit_scorecard.tbl_sg_income_statement
        WHERE level_2 SIMILAR TO '%%(revenue|ebit|ebt|operating profit|cost of sales|depreciation|gross profit|interest|net profit|income taxes)%%'
        ORDER BY loan_code, date, level_2, created_time DESC) a
    INNER JOIN (
        SELECT
            loan_code,
            borrower_id,
            product_id,
            status,
            created_at
        FROM loan_db.loans
        WHERE country_code='SG') b
    ON a.loan_code=b.loan_code
    ORDER BY b.borrower_id, a.date, a.financial_metric, b.created_at DESC
    ;
    ''')

    income_statement = pd.pivot_table(income_statement_raw, index=['borrower_id', 'date'], columns=['financial_metric'], aggfunc=np.max).reset_index()
    income_statement.columns = [col for col_array in income_statement.columns for col in col_array if col not in ['', 'value']]

    income_statement.sort_values(['borrower_id', 'date'], ascending=False, inplace=True)
    income_statement['recency_order_income_statement'] = income_statement.groupby('borrower_id').cumcount()

    return balance_sheet, income_statement

balance_sheet, income_statement = get_financials()


# Step 2: Prepare the previous year's values for certain fields (e.g. revenue) so that features such as sales growth and profit growth can be computed.

balance_sheet_previous = balance_sheet[['borrower_id', 'recency_order_balance_sheet', 'inventory', 'trade_receivables', 'trade_payables']]
balance_sheet_previous['recency_order_balance_sheet'] = balance_sheet_previous['recency_order_balance_sheet'] - 1
balance_sheet_previous = balance_sheet_previous.rename(columns = {'inventory': 'inventory_previous',
                                                                  'trade_receivables': 'trade_receivables_previous',
                                                                  'trade_payables': 'trade_payables_previous'})
balance_sheet_final = pd.merge(balance_sheet,
                               balance_sheet_previous,
                               on=['borrower_id', 'recency_order_balance_sheet'],
                               how='left')

income_statement_previous = income_statement[['borrower_id', 'recency_order_income_statement', 'revenue', 'net_profit_or_loss']]
income_statement_previous['recency_order_income_statement'] = income_statement_previous['recency_order_income_statement'] - 1
income_statement_previous = income_statement_previous.rename(columns = {'revenue': 'revenue_previous',
                                                                        'net_profit_or_loss': 'net_profit_or_loss_previous'})
income_statement_final = pd.merge(income_statement,
                                  income_statement_previous,
                                  on=['borrower_id', 'recency_order_income_statement'],
                                  how='left')


# Step 3: Merge both the balance sheet and income statement into one single financials file.

financials = pd.merge(balance_sheet_final,
                      income_statement_final,
                      on=['borrower_id', 'date'],
                      how='outer')

financials.sort_values(['borrower_id', 'date'], ascending=False, inplace=True)


# Step 4: Define all the key financial ratios.

def current_ratio(row):
    try:
        value = row['current_assets'] / row['current_liabilities']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def quick_ratio(row):
    try:
        value = (row['current_assets'] - np.nan_to_num(row['inventory'])) / row['current_liabilities']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def cash_ratio(row):
    try:
        value = row['cash_and_cash_equivalent'] / row['current_liabilities']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def debt_ratio(row):
    try:
        value = row['total_liabilities'] / row['total_assets']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def debt_to_equity_ratio(row):
    try:
        value = row['total_liabilities'] / (row['total_assets'] - row['total_liabilities'])
    except ZeroDivisionError:
        value = float('NaN')
    return value

def total_net_worth(row):
    try:
        value = row['total_assets'] - row['total_liabilities']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def interest_coverage_ratio_ebitda(row):
    try:
        value = (row['ebit'] + np.nan_to_num(row['depreciation_and_or_amortisation'])) / (row['ebit'] - row['ebt'])
    except ZeroDivisionError:
        value = float('NaN')
    return value

def interest_coverage_ratio_ebit(row):
    try:
        value = row['ebit'] / (row['ebit'] - row['ebt'])
    except ZeroDivisionError:
        value = float('NaN')
    return value

def interest_coverage_ratio_op(row):
    try:
        value = row['operating_profit'] / (row['ebit'] - row['ebt'])
    except ZeroDivisionError:
        value = float('NaN')
    return value

def net_profit_margin(row):
    try:
        value = row['net_profit_or_loss'] / row['revenue']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def gross_margin(row):
    try:
        value = row['gross_profit'] / row['revenue']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def ebitda_margin(row):
    try:
        value = (row['ebit'] + np.nan_to_num(row['depreciation_and_or_amortisation'])) / row['revenue']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def ebit_margin(row):
    try:
        value = row['ebit'] / row['revenue']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def return_on_assets(row):
    try:
        value = row['net_profit_or_loss'] / row['total_assets']
    except ZeroDivisionError:
        value = float('NaN')
    return value

def return_on_equity(row):
    try:
        value = row['net_profit_or_loss'] / (row['total_assets'] - row['total_liabilities'])
    except ZeroDivisionError:
        value = float('NaN')
    return value

def sales_growth(row):
    try:
        value = row['revenue'] / row['revenue_previous'] - 1
    except ZeroDivisionError:
        value = float('NaN')
    return value

def profit_growth(row):
    try:
        value = row['net_profit_or_loss'] / row['net_profit_or_loss_previous'] - 1
    except ZeroDivisionError:
        value = float('NaN')
    return value

def inventory_turnover(row):
    try:
        value = row['cost_of_sales'] / ((row['inventory'] + row['inventory_previous']) / 2)
    except ZeroDivisionError:
        value = float('NaN')
    return value

def receivables_turnover(row):
    try:
        value = row['revenue'] / ((row['trade_receivables'] + row['trade_receivables_previous']) / 2)
    except ZeroDivisionError:
        value = float('NaN')
    return value


# Step 5: Prepare the file with all financial features here.

financials['current_ratio']                     = financials.apply(current_ratio, axis=1)
financials['quick_ratio']                       = financials.apply(quick_ratio, axis=1)
financials['cash_ratio']                        = financials.apply(cash_ratio, axis=1)
financials['debt_ratio']                        = financials.apply(debt_ratio, axis=1)
financials['debt_to_equity_ratio']              = financials.apply(debt_to_equity_ratio, axis=1)
financials['total_net_worth']                   = financials.apply(total_net_worth, axis=1)
financials['interest_coverage_ratio_ebitda']    = financials.apply(interest_coverage_ratio_ebitda, axis=1)
financials['interest_coverage_ratio_ebit']      = financials.apply(interest_coverage_ratio_ebit, axis=1)
financials['interest_coverage_ratio_op']        = financials.apply(interest_coverage_ratio_op, axis=1)
financials['net_profit_margin']                 = financials.apply(net_profit_margin, axis=1)
financials['gross_margin']                      = financials.apply(gross_margin, axis=1)
financials['ebitda_margin']                     = financials.apply(ebitda_margin, axis=1)
financials['ebit_margin']                       = financials.apply(ebit_margin, axis=1)
financials['return_on_assets']                  = financials.apply(return_on_assets, axis=1)
financials['return_on_equity']                  = financials.apply(return_on_equity, axis=1)
financials['sales_growth']                      = financials.apply(sales_growth, axis=1)
financials['profit_growth']                     = financials.apply(profit_growth, axis=1)
financials['inventory_turnover']                = financials.apply(inventory_turnover, axis=1)
financials['receivables_turnover']              = financials.apply(receivables_turnover, axis=1)


# Step 6: Merge the financial features with the development base.

financials_base = pd.merge(p1_base_preparation.base,
                           financials,
                           on='borrower_id',
                           how='left')
financials_base['created_at'] = financials_base['created_at'].dt.date

# Select only the latest set of financials closest to the 'created_at' date.
financials_base = financials_base[~(financials_base.date > financials_base.created_at)]
financials_base.sort_values(['borrower_id', 'loan_code', 'created_at', 'date'], ascending=False, inplace=True)
financials_base = financials_base.drop_duplicates(subset=['borrower_id', 'loan_code', 'created_at'], keep='first').reset_index()

print(financials_base)
