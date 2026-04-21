import pandas as pd
from sqlalchemy import text
import gspread_dataframe as gd
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from core.sheets import get_client

gs = get_client()
sht = gs.open_by_key('1ULMcAbIDIh1VQZf66xotuvmTbrSmpXL3v5CqBe7s8ME')
worksheet_ctl = sht.worksheet('CATALOGUE')
data_ctl = worksheet_ctl.get_values('C8:C')
df_ctl = pd.DataFrame(data_ctl, columns=['default_code'])

engine = get_engine()

CREATE_CATALOGUE_SQL = """
CREATE TABLE IF NOT EXISTS catalogue (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    default_code VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_default_code (default_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

df_cat = (
    df_ctl.copy()
    .assign(default_code=lambda d: d['default_code'].astype(str).str.strip())
)
df_cat = df_cat[df_cat['default_code'].ne("") & df_cat['default_code'].ne("None")]
df_cat = df_cat.drop_duplicates(subset=['default_code']).reset_index(drop=True)

with engine.begin() as conn:
    conn.execute(text(CREATE_CATALOGUE_SQL))
    conn.execute(text("TRUNCATE TABLE catalogue"))
    rows = df_cat.to_dict(orient="records")
    if rows:
        conn.execute(text("INSERT INTO catalogue (default_code) VALUES (:default_code)"), rows)

print(f"Catalogue refresh xong: {len(df_cat)} dòng.")
