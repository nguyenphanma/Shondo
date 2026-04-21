import pandas as pd
from sqlalchemy import text
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from core.sheets import get_client

gs = get_client()
sht = gs.open_by_key('1aFDuIMWZvW2dBIJsUpWgE4XUyIFfW4wFqq4Undhoyfw')
worksheet_ctl = sht.worksheet('UNPIVOT_TARGET')
data_plan = worksheet_ctl.get_all_values()
df_plan = pd.DataFrame(data_plan[1:], columns=data_plan[0])

engine = get_engine()

df_plan['sole material'] = df_plan['sole material'].astype(str).str.upper()
df_plan['type product'] = df_plan['type product'].astype(str).str.upper()
df_plan = df_plan.rename(columns={'sole material': 'sole_material', 'type product': 'type_products'})
df_plan['metric'] = df_plan['metric'].replace({'SL kì vọng': 'qty_plan', 'Dthu dự tính': 'rvn_plan'})
df_plan['year'] = 2026
df_plan['date_plan'] = pd.to_datetime(
    df_plan['year'].astype(str) + '-' +
    df_plan['month'].astype(int).astype(str).str.zfill(2) + '-01'
)

df_plan_ft = df_plan[['channel', 'sole_material', 'type_products', 'category',
                       'subcategory', 'metric', 'numbers', 'date_plan']].copy()

for c in ['channel', 'sole_material', 'type_products', 'category', 'subcategory', 'metric']:
    df_plan_ft[c] = df_plan_ft[c].astype(str).str.strip()

df_plan_ft['numbers'] = (
    df_plan_ft['numbers'].astype(str)
    .str.replace(',', '', regex=False)
    .replace({'': None, 'nan': None, 'None': None})
)
df_plan_ft['numbers'] = pd.to_numeric(df_plan_ft['numbers'], errors='coerce').fillna(0)
df_plan_ft['date_plan'] = pd.to_datetime(df_plan_ft['date_plan']).dt.date

table_name = "plan_target_unpivot"
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    channel       VARCHAR(50)  NOT NULL,
    sole_material VARCHAR(80)  NULL,
    type_products VARCHAR(80)  NULL,
    category      VARCHAR(80)  NULL,
    subcategory   VARCHAR(80)  NULL,
    metric        VARCHAR(30)  NOT NULL,
    numbers       DECIMAL(18,2) NOT NULL DEFAULT 0,
    date_plan     DATE NOT NULL,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_date_plan (date_plan),
    KEY idx_main (channel, metric, date_plan),
    KEY idx_dim (category, subcategory, sole_material, type_products)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

date_min = df_plan_ft['date_plan'].min()
date_max = df_plan_ft['date_plan'].max()

with engine.begin() as conn:
    conn.execute(text(create_table_sql))
    conn.execute(
        text(f"DELETE FROM {table_name} WHERE date_plan BETWEEN :date_min AND :date_max"),
        {"date_min": date_min, "date_max": date_max}
    )

df_plan_ft.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=20000, method='multi')
print(f"✅ Done: deleted [{date_min}..{date_max}] then inserted {len(df_plan_ft):,} rows into `{table_name}`")
