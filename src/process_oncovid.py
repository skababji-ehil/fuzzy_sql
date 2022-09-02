import os
from pathlib import Path
import pandas as pd
import datetime


#Set paths
root_dir=Path(__file__).parent.parent
os.chdir(root_dir)
output_dir=os.path.join(root_dir,'data/real') 
input_dir=os.path.join(root_dir,'data/oncovid') 

data=pd.read_csv(os.path.join(input_dir, 'oncovid.csv'))
# ref_date="2020/01/01"
# ref_date=pd.to_datetime(ref_date, utc=False)
# convert_date =  lambda x: ref_date+ pd.DateOffset(days=x, utc=False)
# data['date_reported']=data['date_reported'].map(convert_date)

ref_date=datetime.date(2020,1,1)
convert_date= lambda x: ref_date+datetime.timedelta(x)
data['date_reported']=data['date_reported'].map(convert_date)
data.to_csv(os.path.join(output_dir,'oncovid_dtd.csv'), index=False)
