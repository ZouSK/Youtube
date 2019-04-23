#!/usr/bin/env python3
# 212.64.26.110
from datetime import datetime
from sqlalchemy import Integer, String, DateTime, create_engine, Column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


DATABASE_URI = 'mysql+pymysql://root:crush@212.64.26.110:3306/BaiProject'


engine = create_engine(DATABASE_URI)
Base = declarative_base(engine)
session = sessionmaker(engine)()


class Socialbacker(Base):

    __tablename__ = 'socialbackers'
    id = Column(String(64), nullable=False)
    rank = Column(Integer, primary_key=True)
    channel = Column(String(64), nullable=False)
    subscriber = Column(String(256), nullable=False)
    views = Column(String(256), nullable=False)
    video = Column(String(256), nullable=False)
    link = Column(String(128), nullable=False)
    crawl_time = Column(DateTime(), default=datetime.now)


if __name__ == '__main__':
    Base.metadata.drop_all()
    Base.metadata.create_all()
