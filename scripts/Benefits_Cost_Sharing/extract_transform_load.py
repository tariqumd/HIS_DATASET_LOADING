from sqlalchemy import create_engine,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,Float
import csv
#from sqlalchemy.orm import mapper, clear_mappers


engine = create_engine('sqlite:///../../sqlfiles/Health_Insurance.db', echo = True)
Base = declarative_base()


bcs = []
year=input("Enter a year to load:")
path='../../input/'+year+'/Benefits_Cost_Sharing_PUF.csv'
#print(path)
with open(path,'r',encoding='utf-8') as f:
    reader = csv.reader(f)
    for row in reader:
        fields=row
        break
    #print(fields)
tablename="Benefits_Cost_Sharing"+"_"+year
attr_dict = {'__tablename__': tablename}
attr_dict["record_id"]=Column(Integer, primary_key=True, autoincrement=True)
for i in fields:
    attr_dict[i]=Column(String)
#print(attr_dict)
MyTableClass = type('MyTableClass', (Base,), attr_dict)



Base.metadata.create_all(engine)
#Base.metadata.drop_all(engine)
