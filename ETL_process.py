# Final Project of Data Engineering Class
# Spring Semester 2017
#
# ---- Developed by ----
# Aria Ghora Prabono - 201631011
# Feri Setiawan - 201630189
# Sunder Ali Khowaja - 201632039
#
# This code is used to extract the data from multiple sources
# such as mobile screen status, user's performed activity,
# user's mood and user's Facebook's post data.
# The extraction process of Facebook data will be covered
# in the next code file. (Analysis_process.py)

from datetime import datetime
import re
import pandas as pd
import numpy as np

# subjects name list
# iterate for each subjects
subjects=["A","B","C"]
for k in subjects:
    subject=k;
    screen=subject+"-de-screen.csv"
    mood=subject+"-de-mood.csv"
    activity=subject+"-de-activity.csv"
    screen_cols = ['subject_id','timestamp','status']

    # second data source (MOOD)
    # load screen status data
    screen_df = pd.read_csv(screen, names = screen_cols)

    # convert timestamp column into time-date format
    for i in range(len(screen_df)):
        screen_df['timestamp'][i] = datetime.strptime(screen_df['timestamp'][i], "%m/%d/%Y %H:%M:%S")

    j = 0
    temp = 0
    columns = ['timestamp','active_interval','non_active_interval']
    N = int(np.ceil(len(screen_df)/12.0))
    df_screen = pd.DataFrame(pd.np.empty((N,3)) * pd.np.nan, columns = columns)

    # create new column (active_interval and non_active_interval)
    # to represents the total number of active / non-active screen status
    # of user's mobile phone for each minutes
    for i in range(len(screen_df)):
        diff = screen_df['timestamp'][i] - screen_df['timestamp'][temp]
        if (diff.seconds >= 60):
            count_temp1 = screen_df['status'][temp:i+1] == "ON"
            count_temp2 = screen_df['status'][temp:i+1] == "OFF"
            #print(sum(bool(x) for x in count_temp1))
            df_screen.loc[j,'timestamp'] = screen_df['timestamp'][i]
            df_screen.loc[j,'active_interval'] = int(sum(bool(x) for x in count_temp1))
            df_screen.loc[j,'non_active_interval'] = int(sum(bool(x) for x in count_temp2))
            temp = i
            j=j+1

    df_screen_dropna = df_screen.dropna()
    df_screen_dropna['subject_id'] = subject


    # second data source (MOOD)
    # load mood data
    cols = ['subject_id','timestamp','emotion']
    mood_df = pd.read_csv(mood, names = cols)

    # convert timestamp column into time-date format
    for i in range(len(mood_df)):
        mood_df['timestamp'][i] = datetime.strptime(mood_df['timestamp'][i], "%m/%d/%Y %H:%M:%S")

    df_mood = df_screen_dropna
    df_mood['mood'] = np.NaN

    # extrapolate the mood data with screen activity data for each minutes
    buffer = pd.Series(index = range(0,20))
    for i in range(len(df_mood)):
        test_temp1 = df_mood['timestamp'][i]
        for j in range(len(mood_df)):
            test_temp2 = mood_df['timestamp'][j]
            if (test_temp1.day == test_temp2.day):
                diff = abs(test_temp1.hour - test_temp2.hour) + abs(test_temp1.minute - test_temp2.minute)
                buffer[j] = diff
        temp_index = buffer.idxmin()
        df_mood.loc[i,'mood'] = mood_df['emotion'][temp_index]

    # ignore the "second" value of the timestamp
    for i in range(len(df_mood)):
        df_mood.loc[i, 'timestamp'] = df_mood['timestamp'][i].replace(second=0)

    # deep copy the extrapolated data frame into combined data frame
    df_combined = df_mood.copy(deep=True)

    # Third dataset (ACTIVITY)
    # load activity data
    df_activity = pd.read_csv(activity, sep=';', names=["subject_id", "timestamp", "status"])

    # convert timestamp column into time-date format
    for index,row in df_activity.iterrows():
        df_activity.loc[index,'timestamp']=datetime.strptime(df_activity['timestamp'][index],',%Y-%m-%d %H:%M:%S.%f')

    # define the regular expression template
    regexActivity = r"([A-Z])\w+"
    regexConfidence = r"(\d+)"

    # create new column in the activity data frame
    df_activity['activity'] = pd.Series(np.random.randn(df_activity['status'].count()), index=df_activity.index)
    df_activity['confidence'] = pd.Series(np.random.randn(df_activity['status'].count()), index=df_activity.index)

    # assign the matched activity and confidence with respect to the regular expression template
    for index,row in df_activity.iterrows():
        df_activity.loc[index, 'activity'] = (re.search(regexActivity, df_activity['status'][index])).group(0)
        df_activity.loc[index, 'confidence'] = (re.search(regexConfidence, df_activity['status'][index])).group(0)

    # delete "status" column
    del df_activity["status"]

    # An algorithm to handle the unknown activity value and activity with low confidence (<50%)
    for index,row in df_activity.iterrows():
        if(index<len(df_activity)-1):
            if df_activity['activity'][index+1] == 'UNKNOWN':
                df_activity.loc[index+1, 'activity'] = df_activity.loc[index, 'activity']
                df_activity.loc[index+1, 'confidence'] = df_activity.loc[index, 'confidence']
            elif int(df_activity['confidence'][index+1])<50 and int(df_activity['confidence'][index+1])<int(df_activity['confidence'][index]):
                df_activity.loc[index+1, 'activity'] = df_activity.loc[index, 'activity']
                df_activity.loc[index+1, 'confidence'] = df_activity.loc[index, 'confidence']

    # an algorithm to handle the missclassified activity
    for index,row in df_activity.iterrows():
        if(index!=0 and index<len(df_activity)-1):
                if df_activity['activity'][index-1] == df_activity['activity'][index+1]:
                    if df_activity['confidence'][index-1] > df_activity['confidence'][index+1]:
                        df_activity.loc[index, 'activity'] = df_activity.loc[index-1, 'activity']
                        df_activity.loc[index, 'confidence'] = df_activity.loc[index-1, 'confidence']
                    else:
                        df_activity.loc[index, 'activity'] = df_activity.loc[index+1, 'activity']
                        df_activity.loc[index, 'confidence'] = df_activity.loc[index+1, 'confidence']

    # combine the activity dataframe into "combined" dataframe based on the nearest timestamp
    df_combined['activity'] = np.NaN
    buffer = pd.Series(index = range(0,20))
    for i in range(len(df_combined)):
        test_temp1 = df_combined['timestamp'][i]
        for j in range(len(df_activity)):
            test_temp2 = df_activity['timestamp'][j]
            if (test_temp1.day == test_temp2.day):
                diff = abs(test_temp1.hour - test_temp2.hour) + abs(test_temp1.minute - test_temp2.minute)
                buffer[j] = diff
        temp_index = buffer.idxmin()
        df_combined.loc[i,'activity'] = df_activity['activity'][temp_index]
    filename = subject + "-combined.csv"
    df_combined.to_csv(filename, sep=',')
