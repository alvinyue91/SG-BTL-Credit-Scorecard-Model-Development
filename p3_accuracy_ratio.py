"""
03 Accuracy Ratio

This script computes the accuracy ratio for all the features.
"""

from utils.accuracy_ratio import get_accuracy_ratio_values
from scripts import p2b_features_bank_statements_perfios
from scripts import p2c_features_financial_statements
from scripts import p2d_features_questnet_cbs
from scripts import p2e_features_cbs

def accuracy_ratio(feature, file, higher_the_better=True):
    f = file.dropna(subset=[feature])
    ar = get_accuracy_ratio_values(f['def_ind'], f[feature])
    default = f['def_ind'].sum()
    after = len(f)
    total = len(file)
    if higher_the_better:
        ar = -ar
    else:
        ar = ar
    return ar, default, after, total


# Bank Statements - Data Lake
print('Deposit Trend L6M (Data Lake): '        , accuracy_ratio('deposit_sgd_trend_l6m', p2b_features_bank_statements_perfios.perfios_base, True))
print('Deposit Volatility L6M (Data Lake): '   , accuracy_ratio('deposit_sgd_vol_l6m', p2b_features_bank_statements_perfios.perfios_base, False))
print('Deposit Average L6M (Data Lake): '      , accuracy_ratio('deposit_sgd_avg_l6m', p2b_features_bank_statements_perfios.perfios_base, True))
print('Withdrawal Trend L6M (Data Lake): '     , accuracy_ratio('withdrawal_sgd_trend_l6m', p2b_features_bank_statements_perfios.perfios_base, False))
print('Withdrawal Volatility L6M (Data Lake): ', accuracy_ratio('withdrawal_sgd_vol_l6m', p2b_features_bank_statements_perfios.perfios_base, False))
print('Withdrawal Average L6M (Data Lake): '   , accuracy_ratio('withdrawal_sgd_avg_l6m', p2b_features_bank_statements_perfios.perfios_base, False))
print('Balance Trend L6M (Data Lake): '        , accuracy_ratio('balance_sgd_trend_l6m', p2b_features_bank_statements_perfios.perfios_base, True))
print('Balance Volatility L6M (Data Lake): '   , accuracy_ratio('balance_sgd_vol_l6m', p2b_features_bank_statements_perfios.perfios_base, False))
print('Balance Average L6M (Data Lake): '      , accuracy_ratio('balance_sgd_avg_l6m', p2b_features_bank_statements_perfios.perfios_base, True))


# Bank Statements - Perfios
# Not possible to measure the AR of features derived from the Perfios bank statements because of 0 default from the ~18 loans.


# Financial Statements
print('Current Ratio: '                             , accuracy_ratio('current_ratio', p2c_features_financial_statements.financials_base, True))
print('Quick Ratio: '                               , accuracy_ratio('quick_ratio', p2c_features_financial_statements.financials_base, True))
print('Cash Ratio: '                                , accuracy_ratio('cash_ratio', p2c_features_financial_statements.financials_base, True))
print('Debt Ratio: '                                , accuracy_ratio('debt_ratio', p2c_features_financial_statements.financials_base, False))
print('Debt-to-Equity Ratio: '                      , accuracy_ratio('debt_to_equity_ratio', p2c_features_financial_statements.financials_base, False))
print('Total Net Worth: '                           , accuracy_ratio('total_net_worth', p2c_features_financial_statements.financials_base, True))
print('Interest Coverage Ratio (EBITDA): '          , accuracy_ratio('interest_coverage_ratio_ebitda', p2c_features_financial_statements.financials_base, True))
print('Interest Coverage Ratio (EBIT): '            , accuracy_ratio('interest_coverage_ratio_ebit', p2c_features_financial_statements.financials_base, True))
print('Interest Coverage Ratio (Operating Profit): ', accuracy_ratio('interest_coverage_ratio_op', p2c_features_financial_statements.financials_base, True))
print('Net Profit Margin: '                         , accuracy_ratio('net_profit_margin', p2c_features_financial_statements.financials_base, True))
print('Gross Margin: '                              , accuracy_ratio('gross_margin', p2c_features_financial_statements.financials_base, True))
print('EBITDA Margin: '                             , accuracy_ratio('ebitda_margin', p2c_features_financial_statements.financials_base, True))
print('EBIT Margin: '                               , accuracy_ratio('ebit_margin', p2c_features_financial_statements.financials_base, True))
print('ROA: '                                       , accuracy_ratio('return_on_assets', p2c_features_financial_statements.financials_base, True))
print('ROE: '                                       , accuracy_ratio('return_on_equity', p2c_features_financial_statements.financials_base, True))
print('Sales Growth: '                              , accuracy_ratio('sales_growth', p2c_features_financial_statements.financials_base, True))
print('Profit Growth: '                             , accuracy_ratio('profit_growth', p2c_features_financial_statements.financials_base, True))
print('Inventory Turnover: '                        , accuracy_ratio('inventory_turnover', p2c_features_financial_statements.financials_base, True))
print('Receivables Turnover: '                      , accuracy_ratio('receivables_turnover', p2c_features_financial_statements.financials_base, True))


# QuestNet and CBS Risk Grades
print('BRC Risk Grade: '     , accuracy_ratio('brc_credit_payment_grade_score', p2d_features_questnet_cbs.loan_docs_base, False))
print('BRI Risk Grade: '     , accuracy_ratio('bri_payment_grade_score', p2d_features_questnet_cbs.loan_docs_base, False))
print('CBS Risk Grade: '     , accuracy_ratio('cbs_risk_grade_score', p2d_features_questnet_cbs.loan_docs_base, False))
print('Number of Employees: ', accuracy_ratio('brc_number_of_employees', p2d_features_questnet_cbs.loan_docs_base, True))


# CBS Documents
print('Unsecured Interest-Bearing Balance Average L6M (S3): ', accuracy_ratio('unsecured_bal_int_bearing_avg_l6m', p2e_features_cbs.cbs_base, False))
print('Unsecured Interest-Bearing Balance Trend L6M (S3): '  , accuracy_ratio('unsecured_bal_int_bearing_trend_l6m', p2e_features_cbs.cbs_base, False))
print('Worst Delinquency Status L6M (S3): '                  , accuracy_ratio('worst_dq_status_l6m', p2e_features_cbs.cbs_base, False))
print('Partial Payment Count L6M (S3): '                     , accuracy_ratio('partial_payment_count_l6m', p2e_features_cbs.cbs_base, False))
print('Cash Advance Indicator L6M (S3): '                    , accuracy_ratio('cash_adv_ind_l6m', p2e_features_cbs.cbs_base, False))
