import pandas as pd
from sqlalchemy import text


_PRODUCT_TEMPLATE_SQL = """
    SELECT
        ps.product_id AS parent_product_id,
        ps.code AS default_code,
        ps.category_id,
        COALESCE(ps2.code, ps.code) AS fdcode,
        COALESCE(ps2.price, ps.price) AS price,
        CASE
            WHEN UPPER(COALESCE(c2.name, c1.name)) IN ('SANDALS', 'KID SANDALS', 'KID SNEAKERS', 'SLIDES', 'SNEAKERS') THEN
                CASE
                    WHEN RIGHT(COALESCE(ps2.code, ps.code), 1) = 'W' THEN CONCAT(LEFT(COALESCE(ps2.code, ps.code), 2), 'W')
                    ELSE LEFT(COALESCE(ps2.code, ps.code), 2)
                END
            ELSE '#'
        END AS size,
        COALESCE(c1.name, c2.name) AS subcategory,
        COALESCE(c2.name, c1.name) AS category,
        COALESCE(ps2.launch_date, ps.launch_date) AS launch_date,
        CASE
            WHEN COALESCE(ps2.launch_date, ps.launch_date) IS NULL
                AND UPPER(COALESCE(c2.name, c1.name)) IN ('SANDALS', 'KID SANDALS', 'KID SNEAKERS', 'SLIDES', 'SNEAKERS')
                THEN 'SP CHỜ BÁN'
            WHEN DATEDIFF(CURRENT_DATE(), COALESCE(ps2.launch_date, ps.launch_date)) <= 90
                THEN 'SP MỚI'
            WHEN UPPER(COALESCE(c2.name, c1.name)) IN ('BAGS', 'ACCESSORIES', 'BRACELETS', 'HATS', 'T-SHIRTS')
                THEN 'PHỤ KIỆN'
            ELSE 'SP CŨ'
        END AS type_products,
        ps.image
    FROM products ps
    LEFT JOIN products ps2 ON ps2.parent_id = ps.external_product_id
    LEFT JOIN categories c1 ON ps.category_id = c1.external_category_id
    LEFT JOIN categories c2 ON c1.parent_id = c2.category_id
    WHERE ps.parent_id IN (-2, -1)
    AND ps.product_id IS NOT NULL
"""


def get_product_template(engine) -> pd.DataFrame:
    """Trả về DataFrame template sản phẩm (cha + con, danh mục, size, type)."""
    with engine.connect() as conn:
        return pd.read_sql_query(text(_PRODUCT_TEMPLATE_SQL), conn)
