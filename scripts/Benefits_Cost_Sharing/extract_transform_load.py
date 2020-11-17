from sqlalchemy import create_engine,MetaData,Table,ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,Float,DateTime
import csv
#from sqlalchemy.orm import mapper, clear_mappers
from sqlalchemy.orm import sessionmaker
from datetime import datetime,date
from dateutil.parser import parse

engine = create_engine('sqlite:///../../sqlfiles/Health_Insurance.db', echo = True)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
year=input("Enter a year to load:")
path='../../input/'+year+'/Benefits_Cost_Sharing_PUF.csv'
#print(path)
try:
    reader_file = csv.DictReader(open(path,'r',encoding='cp1252'))# utf-8 for 2014 and 2015 and #cp 1252 for 2016
    field=reader_file.fieldnames
    #print(field)


    #Dynamically creates tables
    tablename="Benefits_Cost_Sharing"+"_"+year
    attr_dict = {'__tablename__': tablename}
    attr_dict["record_id"]=Column(Integer, primary_key=True, autoincrement=True)
    for i in field:
        if i != "ImportDate" or "BusinessYear":
            attr_dict[i] = Column(String)
        elif i=="ImportDate":
            attr_dict[i]=Column(DateTime)
    #print(attr_dict)
    MyTableClass = type('MyTableClass', (Base,), attr_dict)
    Base.metadata.create_all(engine)
    #Static Table
    class LogTable(Base):
        __tablename__ = 'LogTable'
        transaction_id = Column(Integer, primary_key=True,autoincrement=True)
        tablename=Column(String)
        year=Column(Integer,ForeignKey(f'Benefits_Cost_Sharing_{year}.BusinessYear'))
        start_time=Column(DateTime)
        end_time = Column(DateTime)
        no_of_records=Column(Integer)
        transaction_status= Column(String)

    Base.metadata.create_all(engine)



    confirm=input("Do you want to transform load:{y/n)\n")
    if confirm == "y":

        start = datetime.now()

        #Checking Dataset is already available
        dat_check = session.query(MyTableClass).filter_by(BusinessYear=year).first()
        if dat_check is not None:
            MyTableClass.__table__.drop(engine)
            print("Old Records Deleted")
        flag = 0
        checkpoint = 0
        total_records = 0

        # To insert records
        try:
            for row in reader_file:
                #Cleansing ImportDate column
                #fdate=row['ImportDate'].split(" ")[0].split("-")
                #print(fdate[0],fdate[1],fdate[2])
                #dat=date(int(fdate[0]),int(fdate[1]),int(fdate[2]))
                dat= parse(row['ImportDate'])
                row['ImportDate']=dat
                try:
                    record = MyTableClass(**row)
                    session.add(record)
                    print(flag, "th record")
                    if flag >= 500000:
                        try:
                            session.commit()
                        except:
                            session.rollback()
                            MyTableClass.__table__.drop(engine)
                            print("Invalid Data format")
                            break
                        total_records+=flag
                        checkpoint+=1
                        print("5L records inserted: checkpoint")
                        flag = 0
                        continue
                    flag += 1
                except KeyboardInterrupt as k:
                    MyTableClass.__table__.drop(engine)
                    print("Process aborted")
                    break
                except MemoryError as e:
                    session.rollback()
                    MyTableClass.__table__.drop(engine)
                    session.add(LogTable(tablename=tablename, year=year, start_time=start, end_time=datetime.now(),
                                         no_of_records=total_records,transaction_status="Failure"))
                    session.commit()
                    print("Transaction roll back")
                    break
            else:
                total_records += flag
                session.commit()
                session.add(LogTable(tablename=tablename, year=year, start_time=start, end_time=datetime.now(),
                                     no_of_records=total_records,transaction_status="Success"))
                session.commit()
                print("All records inserted: Transaction Completed")

        except UnicodeDecodeError as e:

            MyTableClass.__table__.drop(engine)
            print("Check encoding")

    '''
    #To Clear table
    choice=input("Do you want to clear table\n")
    if choice=='y':
        MyTableClass.__table__.drop(engine)
        #LogTable.__table__.drop(engine)
    '''
except FileNotFoundError as e:
    print("Dataset for Year doesnt exist")

