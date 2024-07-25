import os
from urllib.parse import quote_plus
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, ConfigDict, Field
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from datetime import date
import logging

def create_db_url(user, password, host, db_name, port):
    return f"mysql+pymysql://{user}:{quote_plus(password)}@{host}:{port}/{db_name}"

# 获取环境变量
DB_USER = os.getenv("DB_USER", "DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD", "DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "DB_HOST")
DB_NAME = os.getenv("DB_NAME", "DB_NAME")
DB_PORT = int(os.getenv("DB_PORT", "DB_PORT"))

# 创建数据库 URL
SQLALCHEMY_DATABASE_URL = create_db_url(DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT)

# 创建数据库引擎
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

class MusicDiscography(Base):
    __tablename__ = "MUSIC_DISCOGRAPHY"

    TYPE = Column(String(255), primary_key=True)
    SDATE = Column(Date)
    SALENUMBER = Column(String(255), nullable=True)
    ORDER = Column(String(255))
    MTITLE = Column(String(255))
    MAINCOLOR = Column(String(255), nullable=True)
    LABEL = Column(String(255))
    GROUP = Column(String(255))
    CENTER = Column(String(255))

class MusicDiscographySchema(BaseModel):
    TYPE: Optional[str] = None
    SDATE: Optional[date] = None
    SALENUMBER: Optional[str] = None
    ORDER: Optional[str] = None
    MTITLE: Optional[str] = None
    MAINCOLOR: Optional[str] = None
    LABEL: Optional[str] = None
    GROUP: Optional[str] = None
    CENTER: Optional[str] = None

    class Config:
        orm_mode = True

app = FastAPI()

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # 假设React应用运行在3000端口
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 获取数据库会话的依赖项
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def record_to_dict(record):
    if record is None:
        logger.warning("Encountered a None record")
        return None
    return {c.name: getattr(record, c.name, None) for c in record.__table__.columns}

@app.get("/music_discography/", response_model=List[MusicDiscographySchema])
def read_music_discography(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    try:
        logger.info(f"Querying database with skip={skip}, limit={limit}")
        query = db.query(MusicDiscography).offset(skip).limit(limit)
        records = query.all()
        logger.info(f"Retrieved {len(records)} records from database")

        valid_records = []
        for i, record in enumerate(records):
            try:
                record_dict = record_to_dict(record)
                if record_dict:
                    valid_records.append(record_dict)
                else:
                    logger.warning(f"Skipping None record at index {i}")
            except Exception as e:
                logger.error(f"Error processing record at index {i}: {e}")

        logger.info(f"Returning {len(valid_records)} valid records")
        return valid_records
    except Exception as e:
        logger.error(f"Error in read_music_discography: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 添加一个新的端点来获取原始记录信息，用于调试
@app.get("/debug_music_discography/")
def debug_music_discography(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    try:
        records = db.query(MusicDiscography).offset(skip).limit(limit).all()
        debug_info = []
        for i, record in enumerate(records):
            record_info = {
                "index": i,
                "type": type(record).__name__,
                "attributes": dir(record),
                "is_none": record is None,
                "data": record_to_dict(record) if record is not None else None
            }
            debug_info.append(record_info)
        return debug_info
    except Exception as e:
        logger.error(f"Error in debug_music_discography: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/music_discography/", response_model=MusicDiscographySchema)
def create_music_discography(record: MusicDiscographySchema, db: Session = Depends(get_db)):
    db_record = MusicDiscography(**record.dict())
    db.add(db_record)
    db.commit()
    db.refresh(db_record)
    return db_record