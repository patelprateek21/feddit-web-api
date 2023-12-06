import re
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
# nltk.download('vader_lexicon')

#################################### Preprocessing Comments Data ######################################
data_commentsinfo = pd.read_csv('data/commentsinfo_raw.csv')

# Converting epochs to timestamp
data_commentsinfo.created_at = data_commentsinfo.created_at.apply(lambda epoch:datetime.fromtimestamp(epoch))

# Adding row id to the data and renaming text to comment
data_commentsinfo.insert(0,'row_id',data_commentsinfo.index+1)
data_commentsinfo.rename(columns={'text':'comment'},inplace=True)

print('\nComments data....\n')
print(data_commentsinfo.head(20))

# Taking subset of the data for modelling
data_commentsinfo_subset = data_commentsinfo[['row_id','comment']].copy()

# Removing all non-aphabet characters and converting to lowercase
data_commentsinfo_subset['comment'] = data_commentsinfo_subset['comment'].str.replace("[^a-zA-Z#]"," ",regex=True).str.casefold()

print('\nComments subset data....\n')
print(data_commentsinfo_subset.head(10))

########################### Calculating Classification and Polarity Score ##############################
print('\nProcessing sentiment analysis....\n')
sid = SentimentIntensityAnalyzer()

data_comments_polarity = pd.DataFrame()
for index,row in data_commentsinfo_subset.iterrows():
    scores = sid.polarity_scores(row.iloc[1])
    for key,value in scores.items():
        data_temp = pd.DataFrame(data={'row_id':row.iloc[0],'sentiment_type':key,'sentiment_score':value},index=[0])
        data_comments_polarity = pd.concat([data_comments_polarity,data_temp],ignore_index=True)

# Removing duplicates if any exist
data_comments_polarity = data_comments_polarity.drop_duplicates()
# Keeping rows where sentiment_type = compound
data_comments_polarity = data_comments_polarity[data_comments_polarity.sentiment_type == 'compound']

print('\nPrinting polarity scores data....')
print(data_comments_polarity.head(10))

# Merging polarity score data with commnents data
data_comments = data_commentsinfo.merge(data_comments_polarity,on='row_id',how='left')

# Deriving comments clssification using scores
data_comments['classification'] = np.where(data_comments['sentiment_score']<-0.5,'Negative',
                                           np.where(data_comments['sentiment_score']>0.5,'Positive','Neutral'))

# Renaming sentiment_score column to polarity score
data_comments.rename(columns={'sentiment_score':'polarity_score'},inplace=True)

print('\nPrinting merged data....\n')
print(data_comments.head())

# Rearranging colunmns to get final data 
data_comments_final = data_comments.loc[:,['subfeddit_id','id','username','comment','created_at','classification','polarity_score']].copy()

print('\nPrinting final data....\n')
print(data_comments_final.head())

print('\nSaving final data....\n')
data_comments_final.to_csv('data/comments_sentiments_processed.csv',index=False)
