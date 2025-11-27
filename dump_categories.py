import pandas as pd
import mysql.connector
from datetime import datetime
import logging
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Configure logging
LOG_FILE = os.getenv("LOG_FILE", "app.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)

# Database configuration from .env
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}
print(DB_CONFIG)
def connect_db():
    """Connect to the MySQL database."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

def parse_datetime(date_str):
    """Parse datetime string in YYYY-MM-DD HH:MM:SS format."""
    if pd.isna(date_str) or date_str == '':
        return None
    try:
        return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        logging.warning(f"Invalid datetime format: {date_str}")
        return None

def map_status(status):
    """Map 'Hoạt động' to 1, 'Không hoạt động' to 0."""
    if pd.isna(status) or status == '':
        return None
    status = status.strip().lower()
    return 1 if status == 'hoạt động' else 0 if status == 'không hoạt động' else None

def map_show_home(show_home):
    """Map 'Hiển thị' to 1, 'Không hiển thị' to 0."""
    if pd.isna(show_home) or show_home == '':
        return None
    show_home = show_home.strip().lower()
    return 1 if show_home == 'hiển thị' else 0 if show_home == 'không hiển thị' else None

def get_category_id(cursor, external_category_id):
    """Get category_id for a given external_category_id."""
    if not external_category_id:
        return None
    try:
        cursor.execute(
            "SELECT category_id FROM categories WHERE external_category_id = %s",
            (int(external_category_id),)
        )
        result = cursor.fetchone()
        return result[0] if result else None
    except mysql.connector.Error as e:
        logging.error(f"Failed to fetch category_id for external_category_id {external_category_id}: {e}")
        return None

def dump_categories(excel_file):
    """Dump category data from Excel to categories table."""
    try:
        # Read Excel file
        df = pd.read_excel(excel_file, dtype=str)
        logging.info(f"Read {len(df)} rows from {excel_file}")

        # Connect to database
        conn = connect_db()
        cursor = conn.cursor()

        # Column mappings (Excel headers to database columns)
        column_mappings = {
            'ID': 'external_category_id',  # Maps Excel 'ID' to external_category_id
            'ParentId': 'parent_id',       # Maps Excel 'ParentId' to parent_id
            'Tên danh mục': 'name',
            'Mã danh mục': 'code',
            'Số thứ tự trên hệ thống': 'display_order',
            'Hiển thị': 'show_home',
            'Số thứ tự trang chủ': 'show_home_order',
            'Hoạt động': 'status',
            'Ngày tạo': 'created_at'
        }

        # Pass 1: Insert top-level categories (ParentId empty or 0)
        top_level_df = df[df['ParentId'].isna() | (df['ParentId'] == '0')]
        for index, row in top_level_df.iterrows():
            category_data = {
                'external_category_id': None,
                'parent_id': None,
                'name': None,
                'code': None,
                'display_order': None,
                'show_home': None,
                'show_home_order': None,
                'private_id': None,
                'status': None,
                'image': None,
                'content': None,
                'created_at': None
            }

            # Map Excel columns to category data
            for excel_col, db_col in column_mappings.items():
                if excel_col in row and not pd.isna(row[excel_col]):
                    value = str(row[excel_col]).strip()
                    if db_col == 'external_category_id':
                        try:
                            category_data[db_col] = int(value)
                        except ValueError:
                            category_data[db_col] = None
                    elif db_col == 'parent_id':
                        category_data[db_col] = None  # Top-level has no parent
                    elif db_col == 'created_at':
                        category_data[db_col] = parse_datetime(value)
                    elif db_col == 'status':
                        category_data[db_col] = map_status(value)
                    elif db_col == 'show_home':
                        category_data[db_col] = map_show_home(value)
                    elif db_col in ['display_order', 'show_home_order']:
                        try:
                            category_data[db_col] = int(float(value)) if value else None
                        except ValueError:
                            category_data[db_col] = None
                    else:
                        category_data[db_col] = value if value else None

            # Insert top-level category
            try:
                cursor.execute("""
                    INSERT IGNORE INTO categories (
                        external_category_id, parent_id, name, code, display_order, show_home,
                        show_home_order, private_id, status, image, content, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    category_data['external_category_id'],
                    category_data['parent_id'],
                    category_data['name'],
                    category_data['code'],
                    category_data['display_order'],
                    category_data['show_home'],
                    category_data['show_home_order'],
                    category_data['private_id'],
                    category_data['status'],
                    category_data['image'],
                    category_data['content'],
                    category_data['created_at']
                ))
                logging.info(f"Inserted top-level category {category_data['external_category_id']}")
            except mysql.connector.Error as e:
                logging.error(f"Failed to insert top-level category {category_data['external_category_id']}: {e}")

        conn.commit()

        # Pass 2: Insert subcategories (ParentId not empty or 0)
        subcategories_df = df[~(df['ParentId'].isna() | (df['ParentId'] == '0'))]
        for index, row in subcategories_df.iterrows():
            category_data = {
                'external_category_id': None,
                'parent_id': None,
                'name': None,
                'code': None,
                'display_order': None,
                'show_home': None,
                'show_home_order': None,
                'private_id': None,
                'status': None,
                'image': None,
                'content': None,
                'created_at': None
            }

            # Map Excel columns to category data
            for excel_col, db_col in column_mappings.items():
                if excel_col in row and not pd.isna(row[excel_col]):
                    value = str(row[excel_col]).strip()
                    if db_col == 'external_category_id':
                        try:
                            category_data[db_col] = int(value)
                        except ValueError:
                            category_data[db_col] = None
                    elif db_col == 'parent_id':
                        # Resolve parent_id by looking up ParentId in categories table
                        parent_external_id = row['ParentId']
                        category_data[db_col] = get_category_id(cursor, parent_external_id)
                    elif db_col == 'created_at':
                        category_data[db_col] = parse_datetime(value)
                    elif db_col == 'status':
                        category_data[db_col] = map_status(value)
                    elif db_col == 'show_home':
                        category_data[db_col] = map_show_home(value)
                    elif db_col in ['display_order', 'show_home_order']:
                        try:
                            category_data[db_col] = int(float(value)) if value else None
                        except ValueError:
                            category_data[db_col] = None
                    else:
                        category_data[db_col] = value if value else None

            # Insert subcategory
            try:
                cursor.execute("""
                    INSERT IGNORE INTO categories (
                        external_category_id, parent_id, name, code, display_order, show_home,
                        show_home_order, private_id, status, image, content, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    category_data['external_category_id'],
                    category_data['parent_id'],
                    category_data['name'],
                    category_data['code'],
                    category_data['display_order'],
                    category_data['show_home'],
                    category_data['show_home_order'],
                    category_data['private_id'],
                    category_data['status'],
                    category_data['image'],
                    category_data['content'],
                    category_data['created_at']
                ))
                logging.info(f"Inserted subcategory {category_data['external_category_id']}")
            except mysql.connector.Error as e:
                logging.error(f"Failed to insert subcategory {category_data['external_category_id']}: {e}")

        conn.commit()
        logging.info("Category data dump completed successfully")

    except Exception as e:
        logging.error(f"Failed to process Excel file: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    excel_file = "categories_dump.xlsx"
    dump_categories(excel_file)