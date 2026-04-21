import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def get_engine():
    """Engine kết nối DB chính (ma_shondo)."""
    cs = "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 3306),
        db=os.getenv("DB_NAME"),
    )
    return create_engine(cs, pool_size=5, max_overflow=0, pool_recycle=1800, pool_pre_ping=True)


def get_ecom_engine():
    """Engine kết nối DB ECOM."""
    cs = "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
        user=os.getenv("DB_USER_ECOM"),
        password=os.getenv("DB_PASSWORD_ECOM"),
        host=os.getenv("DB_HOST_ECOM"),
        port=os.getenv("DB_PORT_ECOM", 3306),
        db=os.getenv("DB_NAME_ECOM"),
    )
    return create_engine(cs, pool_pre_ping=True, connect_args={"connect_timeout": 30})
