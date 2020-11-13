from sqlalchemy import create_engine,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,Float,DateTime
import csv
#from sqlalchemy.orm import mapper, clear_mappers
from sqlalchemy.orm import sessionmaker
from datetime import datetime

engine = create_engine('sqlite:///../../sqlfiles/Health_Insurance.db', echo = True)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
year=input("Enter a year to load:")
path='../../input/'+year+'/Benefits_Cost_Sharing_PUF.csv'
#print(path)
try:
    reader_file = csv.DictReader(open(path,'r',encoding='cp1252'))# utf-8 for 14 and 15
    field=reader_file.fieldnames
    #print(field)


    #Dynamically creates tables
    tablename="Benefits_Cost_Sharing"+"_"+year
    attr_dict = {'__tablename__': tablename}
    attr_dict["record_id"]=Column(Integer, primary_key=True, autoincrement=True)
    for i in field:
        attr_dict[i]=Column(String)
    #print(attr_dict)
    MyTableClass = type('MyTableClass', (Base,), attr_dict)

    #Static Table
    class LogTable(Base):
        __tablename__ = 'LogTable'
        transaction_id = Column(Integer, primary_key=True,autoincrement=True)
        tablename=Column(String)
        year=Column(Integer)
        start_time=Column(DateTime)
        end_time = Column(DateTime)
        transaction_status= Column(String)

    Base.metadata.create_all(engine)

    #print(len(list(reader_file)))

    confirm=input("Do you want to transform load:{y/n)\n")
    if confirm=="y":
        i = 0

        start = datetime.now()
        #To insert records
        for row in reader_file:
            try:
                record=MyTableClass(**row)
                session.add(record)
                print(i,"th record")
                if i>=500000:
                    session.commit()
                    print("5L records inserted: checkpoint")
                    i=0
                    continue
                i+=1
            except MemoryError as m:
                session.rollback()
                print("Transaction roll back: Memory error")
        else:
            session.commit()
            session.add(LogTable(tablename=tablename,year=year,start_time=start,end_time=datetime.today(),
                                 transaction_status="Success"))
            session.commit()
            print("All records inserted: Transaction Completed")



    #To drop all databases
    #Base.metadata.drop_all(engine)

except FileNotFoundError as e:
    print("Dataset for Year doesnt exist")

