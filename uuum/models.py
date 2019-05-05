#!/usr/bin/env python3
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, DateTime, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


DATABASE_URI = 'mysql+pymysql://root:crush@212.64.26.110:3306/BaiProject'
# DATABASE_URI = 'mysql+pymysql://root@localhost:3306/youtube'


engine = create_engine(DATABASE_URI)
Base = declarative_base(engine)
session = sessionmaker(engine)()


class Uuum(Base):

    __tablename__ = 'uuum_190506'
    
    id = Column(String(64), nullable=False)
    category = Column(String(64), nullable=False)
    order = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)
    caption = Column(String(64), nullable=False)
    channel = Column(String(64), nullable=False)
    subscriber = Column(String(256), nullable=False)
    views = Column(String(256), nullable=False)
    link = Column(String(128), nullable=False)
    crawl_time = Column(DateTime(), default=datetime.now)



if __name__ == '__main__':
    Base.metadata.drop_all()
    Base.metadata.create_all()
