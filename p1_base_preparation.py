"""
01 Base Preparation

This script extracts the non-BOLT BTL development base for all loans originated since 2015 till the end of 2019.
"""

import pandas as pd
from utils.config import get_config
from utils.database_service import DataService
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

datalake_config = get_config()['datalake_write']
datalake_db = DataService(datalake_config)

base = datalake_db.query_postgres('''
select
    borrower_id,
    loan_code,
    id,
    product_id,
    status,
    case when status in ('SET-DEFAULT') then 1 else 0 end as def_ind,
    created_at,
    applied_amount,
    amount,
    applied_tenor,
    tenor,
    interest_rate,
    interest_rate_effective,
    grade,
    origination_fee
from loan_db.loans
where country_code='SG'
and product_id in (8, 10)
and status in ('SET-COMPLETE', 'SET-DEFAULT')
and created_at <= '2019-12-31'
order by borrower_id desc, created_at desc
;
''')
