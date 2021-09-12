import pandas as pd
import json
from datetime import datetime

df = pd.read_csv('trt_job.csv')
df4 = pd.DataFrame()
campaignID = df['trt_job_campaign'][0]
df = df.drop(['trt_job_id', 'trt_job_campaign', 'trt_job_submitted', 'trt_job_completed', 'trt_job_error', 'trt_job_status', 'trt_twilio_cid', 'trt_job_ref'], axis=1)

df2 = pd.DataFrame()
df['trt_job_request'] = df.apply(lambda x: [list(json.loads(x['trt_job_request']).values()), list(json.loads(x['trt_job_request']).keys())], axis=1)
df2 = df.apply(lambda x: pd.Series(x['trt_job_request'][0], index=x['trt_job_request'][1]), axis=1)

df3 = pd.DataFrame()
df = df[df.trt_twilio_job_detail.str.contains("{")]

df['trt_twilio_job_detail'] = df['trt_twilio_job_detail'].str.replace('None', '"None"')
df['trt_twilio_job_detail'] = df.apply(lambda x: [list(json.loads(x['trt_twilio_job_detail']).values()), list(json.loads(x['trt_twilio_job_detail']).keys())], axis=1)
df3 = df.apply(lambda x: pd.Series(x['trt_twilio_job_detail'][0], index=x['trt_twilio_job_detail'][1]), axis=1)
print('start time')
print(df3['start_time'])
df3['start_time'] = pd.to_datetime(df3['start_time'])
df3['start_time'] = df3['start_time'].dt.tz_convert('US/Eastern')
df3['start_time'] = df3['start_time'].astype(str).str.replace('-04:00', '')
df3[['Date', 'Time']] = df3['start_time'].str.split(' ', 1, expand=True)

report = df2[['phone', 'name_last', 'lrs_no', 'tag']]
print(df2['phone'])
report['Duration'] = df3[['duration']]
report['Date'] = df3[['Date']]
report['Time'] = df3[['Time']]
report['Retries'] = df[['trt_job_rt']]
report['Status'] = df3[['status']]
report['phone'] = report['phone'].astype(str)
print(type(report['Date'][0]))
report = report[['phone', 'name_last', 'lrs_no', 'tag', 'Date', 'Time', 'Duration', 'Retries', 'Status']]

fileName = '{campaignID}_report.csv'.format(campaignID=campaignID)
report.to_csv(fileName, index=False, line_terminator='\n')