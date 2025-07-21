# %%

import os
import requests
import updatabot


# %%

# updatabot.nomis.search('*workforce*', is_current=True)
# We found NM_130_1
print('Collecting data...')

WORKFORCE_DATASET_ID = "NM_130_1"
# Scraped these values from the sketchy Nomis web form:
UK_ID = 2092957697
LONDON_ID = 2013265927

q = (updatabot.nomis.query(WORKFORCE_DATASET_ID)
     .geography(UK_ID)
     .geography(LONDON_ID)
     .select('date', 'geography_name', 'item_name', 'obs_value')
     .filter('industry', name='Total')
     .filter('item', name='total workforce jobs')
     .filter('item', name='employee jobs')
     .filter('item', name='self-employment jobs')
     .filter('measures', name='value')
     )
df = q.dataframe()
df

# %%

# Pivot Item name into columns
df2 = df.pivot(index=['DATE', 'GEOGRAPHY_NAME'],
               columns='ITEM_NAME', values='OBS_VALUE')
df2.reset_index(inplace=True, drop=False)
df2.columns.name = None
df2

# %%
# Tidy the columns and truncate the table
df3 = df2.rename(columns={'DATE': 'date', 'GEOGRAPHY_NAME': 'area'})
since = '2005-03'
df3 = df3[df3['date'] >= since]
df3

# %%
# Rename some values
rename_area = {
    'United Kingdom': 'UK',
    'London': 'London',
}
df3['area'] = df3['area'].map(rename_area)
df3

# %%
#  reorder the columns
df3 = df3[['date', 'area', 'total workforce jobs',
           'employee jobs', 'self-employment jobs']]
df3

print('Built dataframe:')
print(df3)

# %%

# Save the data
df3.to_csv('workforce-jobs.csv', index=False)

# %%

API_KEY = os.getenv('DATAPRESS_API_KEY')
if not API_KEY:
    raise ValueError('DATAPRESS_API_KEY is not set in the environment')
TARGET_SERVER = 'https://data.london.gov.uk'
TARGET_DATASET = 'c6c1e622-1f1c-406b-9456-b8d57ea507a7'
TARGET_FILE = '73bcf44b-162f-4941-88ab-4418a19807d1'

url = f"{TARGET_SERVER}/api/dataset/{TARGET_DATASET}/resources/{TARGET_FILE}"

print('Uploading...')
response = requests.post(url, files={'file': ('workforce-jobs.csv', open('workforce-jobs.csv', 'rb'), 'text/csv')}, headers={
                         'Authorization': API_KEY})
if response.status_code == 200:
    print('Uploaded successfully')
else:
    print('Upload failed')
    print(response.text)
