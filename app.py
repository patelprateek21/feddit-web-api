import os
import pandas as pd
from flask import Flask,request
from datetime import datetime

app = Flask(__name__)

########################## Running Data Pull and Modelling Scripts ##########################
os.system('python3 fetch.py')
os.system('python3 model.py')

############################ Reading Final Data to Expose via API ###########################
data_comments = pd.read_csv('data/comments_sentiments_processed.csv',parse_dates=['created_at'])

#print(data_comments.head())

def check_int(i):
    try:
        return int(i)
    except Exception:
        return 'error'
    
def check_date(d):
    try:
        return datetime.strptime(d,'%d/%m/%Y').date()
    except Exception:
        return 'error'

@app.get("/api/v1/subfeddits/")
def get_top_comments():
    parameters = request.args

    # Checking subfeddit input value
    if 'subfeddit_id' in parameters.keys():
        subfeddit_id = parameters.get('subfeddit_id')
        return_val = check_int(subfeddit_id)
        if type(return_val) == str:
            response = {
                "subfeddit_id":subfeddit_id,
                "message":"Please enter +ve integer subfeddit_id value."
            }  
            return response
        elif return_val < 1:
            response = {
                "subfeddit_id":subfeddit_id,
                "message":"Please enter +ve integer subfeddit_id value."
            }  
            return response
        else:
            subfeddit_id = [return_val]
    else:
        subfeddit_id = data_comments['subfeddit_id'].unique().tolist() #default values

    # Checking limit input value
    if 'limit' in parameters.keys():
        limit = parameters.get('limit')
        return_val = check_int(limit)
        if type(return_val) == str:
            response = {
                "limit":limit,
                "message":"Please enter +ve interger limit value."
            }
            return response
        elif return_val < 1:
            response = {
                "limit":limit,
                "message":"Please enter +ve integer limit value."
            }  
            return response
        else:
            limit = return_val
    else:
        limit = len(data_comments) #default value
    
    # Checking sort input values
    if 'sort' in parameters.keys():
        sort = parameters.get('sort')
        if sort not in ['asc','desc']:
            response = {
                "sort":sort,
                "message":"Incorrect sort value. Please check."
            }
            return response
    else:
        sort = 'desc' #default value

    # Checking start and end date input values
    min_date = data_comments['created_at'].min().date()
    max_date = data_comments['created_at'].max().date()
    if 'start_date' in parameters.keys():
        start_date = parameters.get('start_date')
        return_val1 = check_date(start_date)
        if 'end_date' in parameters.keys():
            end_date = parameters.get('end_date')
            return_val2 = check_date(end_date)
            if type(return_val1) == str or type(return_val2) == str :
                response = {
                    "start_date":start_date,
                    "end_date":end_date,
                    "message":"Incorrect date values. Please check."
                }
                return response
            elif return_val1 < min_date or return_val1 > max_date or return_val2 < min_date or return_val2 > max_date:
                response = {
                    "start_date":start_date,
                    "end_date":end_date,
                    "max_date":max_date.strftime('%d/%m/%Y'),
                    "min_date":min_date.strftime('%d/%m/%Y'),
                    "message":"Dates out of range. Please check."
                }
                return response
            elif return_val1 > return_val2:
                response = {
                    "start_date":start_date,
                    "end_date":end_date,
                    "message":"start_date should be less than end_date. Please check."
                }
                return response
            end_date = return_val2
        else:
            if type(return_val1) == str:
                    response = {
                        "start_date":start_date,
                        "message":"Incorrect start_date value. Please check."
                    }
                    return response
            elif return_val1 > max_date or return_val1 < min_date:
                response = {
                    "start_date":start_date,
                    "max_date":max_date.strftime('%d/%m/%Y'),
                    "min_date":min_date.strftime('%d/%m/%Y'),
                    "message":"Date out of range. Please check."
                }
                return response
            end_date = max_date
        start_date = return_val1
    elif 'end_date' in parameters.keys():
        end_date = parameters.get('end_date')
        return_val = check_date(end_date)
        if type(return_val) == str:
                response = {
                    "end_date":end_date,
                    "message":"Incorrect end_date value. Please check."
                }
                return response
        elif return_val > max_date or return_val < min_date:
            response = {
                "end_date":end_date,
                "max_date":max_date.strftime('%d/%m/%Y'),
                "min_date":min_date.strftime('%d/%m/%Y'),
                "message":"Date out of range. Please check."
            }
            return response
        start_date = min_date
        end_date = return_val
    else:
        start_date = min_date
        end_date = max_date

    # Filtering the data for the given subfeddit and rows limit and selecting necessary columns
    data_comments['created_at_date'] = data_comments['created_at'].apply(lambda x:x.date())
    print(start_date,end_date)
    data_limit = data_comments.loc[(data_comments['subfeddit_id'].isin(subfeddit_id)) & 
                                   (data_comments['created_at_date']>=start_date) & 
                                   (data_comments['created_at_date']<=end_date),
                                   ['id','comment','classification','polarity_score']].sort_values('polarity_score',
                                                                                                    ascending = True if sort=='asc' else False).head(limit).reset_index(drop=True)

    response = {
        "subfeddit_id":subfeddit_id if len(subfeddit_id)>1 else subfeddit_id[0],
        "limit":limit,
        "start_date":start_date.strftime('%d/%m/%Y'),
        "end_date":end_date.strftime('%d/%m/%Y'),
        "sort":sort,
        "comments":data_limit.to_dict('records')
    }

    return response




