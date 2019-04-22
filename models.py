#!/usr/bin/env python3
from sqlalchemy import create_engine, Column, Integer, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


DATABASE_URI = 'mysql+pymysql://localhost:3306/BaiProject'

engine = create_engine(DATABASE_URI)
Base = declarative_base(engine)
session = sessionmaker(engine)()


class Uuum(Base):

    __tablename__ = 'uuum'
    
    pass
