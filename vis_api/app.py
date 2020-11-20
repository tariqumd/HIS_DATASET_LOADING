from flask import Flask
from config import Config
from flask_sqlalchemy import SQLAlchemy
from flask import render_template,request,json,Response, redirect , flash, url_for,session,jsonify


app =  Flask(__name__)

app.config.from_object(Config)
app.config['SQLALCHEMY_DATABASE_URI']='sqlite:///../sqlfiles/Health_Insurance.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=True
db=SQLAlchemy(app)



#tables
logtable = db.Table('LogTable',db.metadata, autoload=True,autoload_with=db.engine)
bcs_table = db.Table('Benefits_Cost_Sharing_2014',db.metadata, autoload=True,autoload_with=db.engine)

@app.route('/')
@app.route('/index',methods=['GET','POST'])
def index():
    year="2014"
    if request.method == "POST":
        year=request.form.get("year")

    print(year)

    return render_template('index.html',year=year)

@app.route('/datas/<string:year>',methods=['GET','POST'])
def datas(year: str):

    tab_name="Business_Rules"+"_"+year
    br_table = db.Table(tab_name,db.metadata, autoload=True,autoload_with=db.engine)

    statecode=[]
    recs=[]
    nos=[]
    br=db.session.query(br_table).all()
    for i in br:
        recs.append(i.StateCode)
        if i.StateCode not in statecode:
            statecode.append(i.StateCode)
    for i in statecode:
        nos.append(recs.count(i))

    #print(statecode,nos)

    return jsonify({'sc':statecode,'ns':nos})





if __name__ == "__main__":
    app.run()


#BCS TABLE
'''year=[1,2,3,4]
records=[2,5,6,7]
count=0
benefits=[]
no=[]
bcs=db.session.query(bcs_table).all()
for i in bcs:
    if i.BenefitName not in benefits:
        count+=1
        benefits.append(i.BenefitName)
        no.append(bcs.count(i.BenefitName))


print(count)
'''
#Log Table
'''
year=[]
records=[]
name="Business_Rules"
logs=db.session.query(logtable).all()
for i in logs:
    if name in i.tablename:
        year.append(i.year)
        records.append(i.no_of_records)
'''
