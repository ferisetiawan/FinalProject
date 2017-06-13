# Final Project of Data Engineering Class
# Spring Semester 2017
#
# ---- Developed by ----
# Aria Ghora Prabono - 201631011
# Feri Setiawan - 201630189
# Sunder Ali Khowaja - 201632039
#
# This code is used to load the data from user's Facebook data
# and append it into combined dataframe.
# Moreover, this code file extract several new features
# which used in the further analysis to determine the user's empathy score.
# The extraction process of Facebook data was performed in the mobile phone
# using our own android application through Facebook Graph API.

from datetime import datetime
import pandas as pd
import json
from pandas import DataFrame, Series

validation = pd.read_csv("validation.csv")
subjects = ["A", "B", "C"]
# iterate for each subjects
for k in subjects:
    subject = k;
    df_combined = pd.read_csv(subject+'-combined.csv')

    # load the user's Facebook data in JSON format
    s = open(subject + '.json', encoding='UTF-8').read()
    jsonData = json.loads(s)

    # function for converting JSON into dataframe format
    def jsonArrayToDf(subject, jsonArray):
        df = DataFrame()

        post_timestamps = []
        post_h_timestamps = []
        post_stories = []
        post_comment_counts = []

        for o in jsonArray:
            post_stories.append(o['story'])

            ts = datetime.strptime(o['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
            tmp_ts = ts.strftime('%Y-%m-%d')

            post_timestamps.append(ts)
            post_h_timestamps.append(tmp_ts)

            comments = o['comments']
            comment_count = 0
            for comment in comments:
                comment_count += 1
                try:
                    subcomments = comment['comments']['data']
                except:
                    subcomments = []

                for subcomment in subcomments:
                    comment_count += 1

            post_comment_counts.append(comment_count)

        df['timestamp'] = post_timestamps
        df['post_story'] = post_stories
        df['comment_count'] = post_comment_counts
        df['subject_id'] = subject

        series_h_timestamps = Series(post_h_timestamps)
        count_table = series_h_timestamps.value_counts()
        df['daily_post_count'] = series_h_timestamps.map(lambda x: count_table[x])

        return df

    # write the user's Facebook data into CSV format
    df = jsonArrayToDf(subject, jsonData)
    df.to_csv(subject + '.csv', sep=',')

    post = pd.read_csv(subject+'-de-facebook.csv')

    # convert timestamp column into time-date format
    for i in range(len(df_combined)):
        df_combined.loc[i,'timestamp'] = datetime.strptime(df_combined['timestamp'][i], "%Y-%m-%d %H:%M:%S")
    del[df_combined['Unnamed: 0']]

    for i in range(len(post)):
        post.loc[i,'timestamp'] = datetime.strptime(post['timestamp'][i], "%Y-%m-%d %H:%M:%S")

    del [post['Unnamed: 0']]
    df_combined['comment_count'] = 0
    df_combined['daily_post_count'] = 0

    # count the number of user's comment and daily_post
    for i in range(len(df_combined)):
        test_temp1 = df_combined['timestamp'][i]
        for j in range(len(post)):
            test_temp2 = post['timestamp'][j]
            if (test_temp1.day == test_temp2.day):
                df_combined.loc[i,'comment_count']= post.loc[j,'comment_count']
                df_combined.loc[i,'daily_post_count'] = post.loc[j,'daily_post_count']

    # EXTRACT NEW FEATURES
    df_combined['mood_trans'] = 0

    # Mood transition feature
    for i in range(len(df_combined)):
        temp = i - 1
        if i == 0:
            df_combined['mood_trans'] = 0
        elif df_combined.loc[i, 'mood'] != df_combined.loc[temp, 'mood']:
            df_combined.loc[i, 'mood_trans'] = 1
        else:
            df_combined.loc[i, 'mood_trans'] = -1

    # Cumulative mood transition feature
    df_combined['cum_mood_trans'] = 0
    for i in range(len(df_combined)):
        temp = i - 1
        if i == 0:
            df_combined['cum_mood_trans'] = 0
        else:
            df_combined.loc[i, 'cum_mood_trans'] = df_combined.loc[i, 'mood_trans'] + df_combined.loc[temp, 'cum_mood_trans']

    # Ratio of active and non-active intervals feature
    df_combined['ratio_active_interval'] = 0.0
    for i in range(len(df_combined)):
        df_combined.loc[i, 'ratio_active_interval'] = df_combined.loc[i, 'active_interval'] / 13.0

    # Social skills measurement feature
    df_combined['social_skill'] = 0.0
    for i in range(len(df_combined)):
        df_combined.loc[i, 'social_skill'] = (df_combined.loc[i,'daily_post_count'] * df_combined.loc[i, 'ratio_active_interval']) \
        + (df_combined.loc[i,'comment_count'] * df_combined.loc[i, 'ratio_active_interval']) + df_combined.loc[i, 'ratio_active_interval']

    df_combined['social_skill'] = df_combined['social_skill']/df_combined['social_skill'].max()

    # Self-awareness measurement feature
    df_combined['self_aware'] = 0.0
    for i in range(len(df_combined)):
        temp = 0;
        temp_val = df_combined.loc[temp:i, 'mood'].value_counts()
        df_combined.loc[i, 'self_aware'] = len(temp_val)/8.0

    # calculate the proposed Empathy Score
    W1 = 0.3   # Weight for social skill
    W2 = 0.5   # Weight for self aware
    W3 = 0.2   # Weight for frequent posts
    df_combined['emp_score'] = 0.0
    for i in range(len(df_combined)):
        df_combined.loc[i, 'emp_score'] = (W1*df_combined.loc[i, 'social_skill']) + (W2*df_combined.loc[i, 'self_aware']) \
        + (W3*df_combined.loc[i, 'daily_post_count'])

    # combine the questionnaire data from validation table into "combined" dataframe
    df_combined['aff_empathy'] = int(validation.loc[(validation["subject_id"] == subject), "aff_empathy"])
    df_combined['cog_empathy'] = int(validation.loc[(validation["subject_id"] == subject), "cog_empathy"])
    df_combined['eq_score'] = int(validation.loc[(validation["subject_id"] == subject), "eq_score"])
    df_combined['eq_label'] = int(validation.loc[(validation["subject_id"] == subject), "eq_label"])

    # write the combined transformed data into CSV file
    print("--- Subject", subject, "---")
    print(df_combined['emp_score'].describe())
    filename = subject + "-transformed_data.csv"
    df_combined.to_csv(filename, sep=',')

# combine all subjects data into one CSV file
db_A=pd.read_csv("A-transformed_data.csv")
db_B=pd.read_csv("B-transformed_data.csv")
db_C=pd.read_csv("C-transformed_data.csv")
db=db_A.copy()
db=db.append(db_B, ignore_index = True)
db=db.append(db_C, ignore_index = True)
del[db['Unnamed: 0']]
db.to_csv("transformed_completeData.csv",sep=',')
