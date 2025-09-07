from pathlib import Path
import zipfile
import pandas as pd
from datetime import datetime
import os
import shutil
from openai import OpenAI
import textwrap
import json
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt


# Set the directory path
folder_path = Path('D:\\my research\\my ema1\\data_collection_tracking')

dc = pd.read_csv(folder_path / 'smu_for_analysis_withclass_baseline_endline_0827.csv')
da = pd.read_csv(folder_path / 'final_data_ema_0822.csv')
a1 = pd.read_csv('D:\my research\my ema1\data_collection_tracking\EMA_social_media_android_jul30\Activity_and_Screenshot.csv')
a2 = pd.read_csv('D:\my research\my ema1\data_collection_tracking\EMA_social_media_ios_jul30\Activity_and_Screenshot.csv')
a3 = pd.read_csv('D:\my research\my ema1\data_collection_tracking\Social_media_android_jul30\Activity_and_Screenshot.csv')
a4 = pd.read_csv('D:\my research\my ema1\data_collection_tracking\Social_media_ios_jul30\Activity_and_Screenshot.csv')

a1.columns = a1.iloc[2].tolist()
a1 = a1.iloc[3:].reset_index(drop=True)
a1 = a1[['Participant ID', 'Choices', 'Start Date']]
a1['scid'] = a1.apply(lambda x: f"P{x['Choices'].split(' ')[1]}_Activity and Screenshot_{datetime.strptime(x['Start Date'], '%m/%d/%Y %I:%M%p').strftime('%d_%m_%Y_%I-%M %p')}_Q29.ipg", axis=1)
a1['scid_u'] = a1.apply(lambda x: f"{x['Participant ID']}_Activity and Screenshot_{datetime.strptime(x['Start Date'], '%m/%d/%Y %I:%M%p').strftime('%d_%m_%Y_%I-%M %p')}_Q29.ipg", axis=1)
assert a1[['Participant ID', 'Start Date']].drop_duplicates().shape[0] == pd.merge(da, a1[['Participant ID', 'Start Date', 'scid']], on=['Participant ID', 'Start Date'], how='inner').shape[0]

a2.columns = a2.iloc[2].tolist()
a2 = a2.iloc[3:].reset_index(drop=True)
a2 = a2[['Participant ID', 'Choices', 'Start Date']]
a2['scid'] = a2.apply(lambda x: f"P{x['Choices'].split(' ')[1]}_Activity and Screenshot_{datetime.strptime(x['Start Date'], '%m/%d/%Y %I:%M%p').strftime('%d_%m_%Y_%I-%M %p')}_Q29.ipg", axis=1)
a2['scid_u'] = a2.apply(lambda x: f"{x['Participant ID']}_Activity and Screenshot_{datetime.strptime(x['Start Date'], '%m/%d/%Y %I:%M%p').strftime('%d_%m_%Y_%I-%M %p')}_Q29.ipg", axis=1)
a2 = a2.drop_duplicates().reset_index(drop=True)
assert a2[['Participant ID', 'Start Date']].drop_duplicates().shape[0] == pd.merge(da, a2[['Participant ID', 'Start Date', 'scid']], on=['Participant ID', 'Start Date'], how='inner').shape[0]

a3.columns = a3.iloc[2].tolist()
a3 = a3.iloc[3:].reset_index(drop=True)
a3 = a3[['Participant ID', 'Choices', 'Start Date']]
a3['scid'] = a3.apply(lambda x: f"P{x['Choices'].split(' ')[1]}_Activity and Screenshot_{datetime.strptime(x['Start Date'], '%m/%d/%Y %I:%M%p').strftime('%d_%m_%Y_%I-%M %p')}_Q29.ipg", axis=1)
a3['scid_u'] = a3.apply(lambda x: f"{x['Participant ID']}_Activity and Screenshot_{datetime.strptime(x['Start Date'], '%m/%d/%Y %I:%M%p').strftime('%d_%m_%Y_%I-%M %p')}_Q29.ipg", axis=1)
a3 = a3.drop_duplicates().reset_index(drop=True)
assert a3[['Participant ID', 'Start Date']].drop_duplicates().shape[0] == pd.merge(da, a3[['Participant ID', 'Start Date', 'scid']], on=['Participant ID', 'Start Date'], how='inner').shape[0]

a4.columns = a4.iloc[2].tolist()
a4 = a4.iloc[3:].reset_index(drop=True)
a4 = a4[['Participant ID', 'Choices', 'Start Date']]
a4['scid'] = a4.apply(lambda x: f"P{x['Choices'].split(' ')[1]}_Activity and Screenshot_{datetime.strptime(x['Start Date'], '%m/%d/%Y %I:%M%p').strftime('%d_%m_%Y_%I-%M %p')}_Q29.ipg", axis=1)
a4['scid_u'] = a4.apply(lambda x: f"{x['Participant ID']}_Activity and Screenshot_{datetime.strptime(x['Start Date'], '%m/%d/%Y %I:%M%p').strftime('%d_%m_%Y_%I-%M %p')}_Q29.ipg", axis=1)
a4 = a4.drop_duplicates().reset_index(drop=True)
#assert a4[['Participant ID', 'Start Date']].drop_duplicates().shape[0] == pd.merge(da, a4[['Participant ID', 'Start Date', 'scid']], on=['Participant ID', 'Start Date'], how='inner').shape[0]
## failed assertion but fine i already checked
merged = a4[['Participant ID', 'Start Date', 'scid']].merge(
    da,
    on=['Participant ID', 'Start Date'],
    how='left',
    indicator=True
)
# Find rows that did not merge -- confirmed that it should be dropped
unmatched = merged[merged['_merge'] == 'left_only']
a4 = a4.loc[a4['scid']!=unmatched['scid'].tolist()[0],:].reset_index(drop=True)
assert a4[['Participant ID', 'Start Date']].drop_duplicates().shape[0] == pd.merge(da, a4[['Participant ID', 'Start Date', 'scid']], on=['Participant ID', 'Start Date'], how='inner').shape[0]

## merge with all data
aa = pd.concat([a1, a2, a3, a4], axis=0)
aa['scid_u'] = aa['scid_u'].apply(lambda x: x.replace('.ipg', '.jpg'))
print(aa.columns)
assert aa.shape[0] == aa['scid_u'].nunique()
da = pd.merge(da, aa[['Participant ID', 'Start Date', 'scid', 'scid_u']], on=['Participant ID', 'Start Date'], how='left')
print(da.shape)
dc = pd.merge(dc, aa[['Participant ID', 'Start Date', 'scid', 'scid_u']], on=['Participant ID', 'Start Date'], how='left')
assert dc.loc[dc['scid'].notna(),:].shape[0] == da.loc[(da['scid'].notna()) & (da['is_smu_or_act']=='smu'),:].shape[0]

## later on you need to thnk about the different levels of matching

## But now let us use gpt first
def unzip_to_same_folder(zip_path):
    # Ensure the file exists
    if not os.path.isfile(zip_path):
        raise FileNotFoundError(f"{zip_path} not found.")

    # Get the folder name by removing .zip extension
    extract_dir = os.path.splitext(zip_path)[0]

    # Create the folder if it doesn't exist
    os.makedirs(extract_dir, exist_ok=True)

    # Extract all contents
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    print(f"Extracted {zip_path} to {extract_dir}")

def merge_multiple_folders(folders, destination):
    os.makedirs(destination, exist_ok=True)

    total_files = 0

    for folder in folders:
        for file in os.listdir(folder):
            src = os.path.join(folder, file)
            if os.path.isfile(src):
                total_files += 1
                dst = os.path.join(destination, file)

                # If file already exists, auto-rename it
                if os.path.exists(dst):
                    base, ext = os.path.splitext(file)
                    i = 1
                    new_name = f"{base}_{i}{ext}"
                    dst = os.path.join(destination, new_name)
                    while os.path.exists(dst):
                        i += 1
                        new_name = f"{base}_{i}{ext}"
                        dst = os.path.join(destination, new_name)

                shutil.copy2(src, dst)

    # Count files in destination
    dest_files = sum(os.path.isfile(os.path.join(destination, f)) for f in os.listdir(destination))

    # Assert check
    assert total_files == dest_files, f"Mismatch: expected {total_files}, but got {dest_files}"

    print(f"✅ All {total_files} files from {len(folders)} folders are now in {destination}")

def rename_plots_from_df(df=a1, folder=folder_path / 'screenshot' / 'ema_an', col_old="scid", col_new="scid_u"):
    # Loop through dataframe rows
    for _, row in df.iterrows():
        old_name = str(row[col_old]).split('.')[0]
        new_name = str(row[col_new]).split('.')[0]

        # Try to find the file in the folder (with any extension, e.g. .png, .jpg, .pdf)
        for file in os.listdir(folder):
            fname, ext = os.path.splitext(file)
            if fname == old_name:
                old_path = os.path.join(folder, file)
                new_path = os.path.join(folder, new_name + ext)
                os.rename(old_path, new_path)
                print(f"Renamed {old_name}{ext} → {new_name}{ext}")
                break
        else:
            print(f"⚠️ File for {old_name} not found in {folder}")

unzip_to_same_folder(folder_path / 'screenshot' / 'ema_an_1.zip')
unzip_to_same_folder(folder_path / 'screenshot' / 'ema_an_2.zip')
merge_multiple_folders(folders=[folder_path / 'screenshot' / 'ema_an_1',
                                folder_path / 'screenshot' / 'ema_an_2'],
                       destination=folder_path / 'screenshot' / 'ema_an')

unzip_to_same_folder(folder_path / 'screenshot' / 'ema_ios_1.zip')
unzip_to_same_folder(folder_path / 'screenshot' / 'ema_ios_2.zip')
unzip_to_same_folder(folder_path / 'screenshot' / 'ema_ios_3.zip')
merge_multiple_folders(folders=[folder_path / 'screenshot' / 'ema_ios_1',
                                folder_path / 'screenshot' / 'ema_ios_2',
                                folder_path / 'screenshot' / 'ema_ios_3'],
                       destination=folder_path / 'screenshot' / 'ema_ios')

rename_plots_from_df(df=a1, folder=folder_path / 'screenshot' / 'ema_an', col_old="scid", col_new="scid_u")
rename_plots_from_df(df=a2, folder=folder_path / 'screenshot' / 'ema_ios', col_old="scid", col_new="scid_u")

for i in [1,2,3,4,5,6,7]:
    xx = f'ema_new_an_{i}.zip'
    unzip_to_same_folder(folder_path / 'screenshot' / xx)
merge_multiple_folders(folders=[folder_path / 'screenshot' / f'ema_new_an_{i}' for i in [1,2,3,4,5,6,7]],
                       destination=folder_path / 'screenshot' / 'ema_new_an')
rename_plots_from_df(df=a3, folder=folder_path / 'screenshot' / 'ema_new_an', col_old="scid", col_new="scid_u")

for i in range(1,22):
    xx = f'ios_{i}.zip'
    unzip_to_same_folder(folder_path / 'screenshot' / xx)
merge_multiple_folders(folders=[folder_path / 'screenshot' / f'ios_{i}' for i in range(1,22)],
                       destination=folder_path / 'screenshot' / 'ema_new_ios')
rename_plots_from_df(df=a4, folder=folder_path / 'screenshot' / 'ema_new_ios', col_old="scid", col_new="scid_u")

merge_multiple_folders(folders=[folder_path / 'screenshot' / 'ema_new_ios',
                                folder_path / 'screenshot' / 'ema_new_an',
                                folder_path / 'screenshot' / 'ema_ios',
                                folder_path / 'screenshot' / 'ema_an', ],
                       destination=folder_path / 'screenshot' / 'ema_all')
## ok now use gpt to read the numbers from the screenshots


api_key = ''
client = OpenAI(api_key=api_key)
pp = textwrap.dedent(f"""\
Extract the following from the screenshot and return only JSON (no extra text):

tab ("Day" or "Week")

date (string as shown)

time (total screen time, e.g., "4h 59m")

categories (object: category → duration string)

updated_time (string as shown)

most_used_apps (array of objects with app and duration)
If a field is missing, output NA or []. Do not infer; copy exactly as displayed.

Expected JSON format (example using your data):

{{
  "tab": "Day",
  "date": "Today, July 16",
  "time": "4h 59m",
  "categories": {{
    "Social": "3h 20m",
    "Utilities": "1h 12m",
    "Information & Reading": "54m"
  }},
  "updated_time": "Today at 6:14 PM" or NA,
  "most_used_apps": [
    {{"app": "Discord", "duration": "2h 12m"}},
    {{"app": "Messages", "duration": "1h 30m"}},
    {{"app": "Instagram", "duration": "1h 16m"}},
    {{"app": "Remote Desktop", "duration": "1h 06m"}}
  ]
}}
""")

def create_file(file_path):
  with open(file_path, "rb") as file_content:
    result = client.files.create(
        file=file_content,
        purpose="vision",
    )
    return result.id



#example_pic = folder_path / 'screenshot' / 'ema_new_ios'/"5e2f49dc143a882e0cda08b8_Activity and Screenshot_16_07_2025_06-12 PM_Q29.jpg"
all_file = os.listdir(folder_path / 'screenshot' / 'ema_all')
res_all = pd.DataFrame()
fail_l = []
for i,img_scid in enumerate(all_file[5:]):
    try:
        idd = create_file(folder_path / 'screenshot' / 'ema_all' / img_scid)
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": pp},
                    {"type": "input_image", "file_id": idd,
                    },
                ]
            }]
        )
        #print(resp.output_text)
        raw_output = resp.output_text
        cleaned = raw_output.strip("`").split("\n", 1)[1]
        cleaned = cleaned.rsplit("```", 1)[0]
        data = json.loads(cleaned)

        res1 = pd.DataFrame.from_dict({
            'tab': data['tab'],
            'date': data['date'],
            'time': data['time'],
            'updated_time': data['updated_time'],
        }, orient='index').T
        res2 = pd.DataFrame(
            [dict(list(data["categories"].items()))]
        )
        res2.columns = [f'category_{x.lower()}' for x in res2.columns]
        row_dict = {f"app_{d['app'].lower()}": d['duration'] for d in data['most_used_apps']}
        res3 = pd.DataFrame([row_dict])
        res = pd.concat([res1, res2, res3], axis=1)
        res['scid_u'] = img_scid
        res_all = pd.concat([res_all, res], axis=0)
        res_all.to_csv(folder_path / 'img_content.csv', index=False)
        print(f'{i} done: {img_scid}')

    except:
        fail_l.append(img_scid)
        pd.DataFrame([fail_l]).T.to_csv(folder_path / 'img_content_fail.csv', index=False)
        print(f'{i} fail')

fail_l2 = []
for i,img_scid in enumerate(fail_l[0:]):
    try:
        idd = create_file(folder_path / 'screenshot' / 'ema_all' / img_scid)
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": pp},
                    {"type": "input_image", "file_id": idd,
                    },
                ]
            }]
        )
        #print(resp.output_text)
        raw_output = resp.output_text
        cleaned = raw_output.strip("`").split("\n", 1)[1]
        cleaned = cleaned.rsplit("```", 1)[0]
        data = json.loads(cleaned)

        res1 = pd.DataFrame.from_dict({
            'tab': data['tab'],
            'date': data['date'],
            'time': data['time'],
            'updated_time': data['updated_time'],
        }, orient='index').T
        res2 = pd.DataFrame(
            [dict(list(data["categories"].items()))]
        )
        res2.columns = [f'category_{x.lower()}' for x in res2.columns]
        row_dict = {f"app_{d['app'].lower()}": d['duration'] for d in data['most_used_apps']}
        res3 = pd.DataFrame([row_dict])
        res = pd.concat([res1, res2, res3], axis=1)
        res['scid_u'] = img_scid
        res_all = pd.concat([res_all, res], axis=0)
        res_all.to_csv(folder_path / 'img_content.csv', index=False)
        print(f'{i} done: {img_scid}')

    except:
        fail_l2.append(img_scid)
        pd.DataFrame([fail_l2]).T.to_csv(folder_path / 'img_content_fail2.csv', index=False)
        print(f'{i} fail')

fail_l3 = []
for i,img_scid in enumerate(fail_l2[0:]):
    try:
        idd = create_file(folder_path / 'screenshot' / 'ema_all' / img_scid)
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": pp},
                    {"type": "input_image", "file_id": idd,
                    },
                ]
            }]
        )
        #print(resp.output_text)
        raw_output = resp.output_text
        cleaned = raw_output.strip("`").split("\n", 1)[1]
        cleaned = cleaned.rsplit("```", 1)[0]
        data = json.loads(cleaned)

        res1 = pd.DataFrame.from_dict({
            'tab': data['tab'],
            'date': data['date'],
            'time': data['time'],
            'updated_time': data['updated_time'],
        }, orient='index').T
        res2 = pd.DataFrame(
            [dict(list(data["categories"].items()))]
        )
        res2.columns = [f'category_{x.lower()}' for x in res2.columns]
        row_dict = {f"app_{d['app'].lower()}": d['duration'] for d in data['most_used_apps']}
        res3 = pd.DataFrame([row_dict])
        res = pd.concat([res1, res2, res3], axis=1)
        res['scid_u'] = img_scid
        res_all = pd.concat([res_all, res], axis=0)
        res_all.to_csv(folder_path / 'img_content.csv', index=False)
        print(f'{i} done: {img_scid}')

    except:
        fail_l3.append(img_scid)
        pd.DataFrame([fail_l3]).T.to_csv(folder_path / 'img_content_fail3.csv', index=False)
        print(f'{i} fail')
print(len(fail_l3))

fail_l4 = []
for i,img_scid in enumerate(fail_l3[0:]):
    try:
        idd = create_file(folder_path / 'screenshot' / 'ema_all' / img_scid)
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": pp},
                    {"type": "input_image", "file_id": idd,
                    },
                ]
            }]
        )
        #print(resp.output_text)
        raw_output = resp.output_text
        cleaned = raw_output.strip("`").split("\n", 1)[1]
        cleaned = cleaned.rsplit("```", 1)[0]
        data = json.loads(cleaned.replace(': NA,', ': "NA",'))

        res1 = pd.DataFrame.from_dict({
            'tab': data['tab'],
            'date': data['date'],
            'time': data['time'],
            'updated_time': data['updated_time'],
        }, orient='index').T
        res2 = pd.DataFrame(
            [dict(list(data["categories"].items()))]
        )
        res2.columns = [f'category_{x.lower()}' for x in res2.columns]
        row_dict = {f"app_{d['app'].lower()}": d['duration'] for d in data['most_used_apps']}
        res3 = pd.DataFrame([row_dict])
        res = pd.concat([res1, res2, res3], axis=1)
        res['scid_u'] = img_scid
        res_all = pd.concat([res_all, res], axis=0)
        res_all.to_csv(folder_path / 'img_content.csv', index=False)
        print(f'{i} done: {img_scid}')

    except:
        fail_l4.append(img_scid)
        pd.DataFrame([fail_l4]).T.to_csv(folder_path / 'img_content_fail4.csv', index=False)
        print(f'{i} fail')

fail_l5 = []
for i,img_scid in enumerate(fail_l4[0:]):
    try:
        idd = create_file(folder_path / 'screenshot' / 'ema_all' / img_scid)
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": pp},
                    {"type": "input_image", "file_id": idd,
                    },
                ]
            }]
        )
        #print(resp.output_text)
        raw_output = resp.output_text
        cleaned = raw_output.strip("`").split("\n", 1)[1]
        cleaned = cleaned.rsplit("```", 1)[0]
        data = json.loads(cleaned.replace(': NA,', ': "NA",'))

        res1 = pd.DataFrame.from_dict({
            'tab': data['tab'],
            'date': data['date'],
            'time': data['time'],
            'updated_time': data['updated_time'],
        }, orient='index').T
        res2 = pd.DataFrame(
            [dict(list(data["categories"].items()))]
        )
        res2.columns = [f'category_{x.lower()}' for x in res2.columns]
        row_dict = {f"app_{d['app'].lower()}": d['duration'] for d in data['most_used_apps']}
        res3 = pd.DataFrame([row_dict])
        res = pd.concat([res1, res2, res3], axis=1)
        res['scid_u'] = img_scid
        res_all = pd.concat([res_all, res], axis=0)
        res_all.to_csv(folder_path / 'img_content.csv', index=False)
        print(f'{i} done: {img_scid}')

    except:
        fail_l5.append(img_scid)
        pd.DataFrame([fail_l5]).T.to_csv(folder_path / 'img_content_fail5.csv', index=False)
        print(f'{i} fail')

fail_l6 = []
for i,img_scid in enumerate(fail_l5[1:]):
    try:
        idd = create_file(folder_path / 'screenshot' / 'ema_all' / img_scid)
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": pp},
                    {"type": "input_image", "file_id": idd,
                    },
                ]
            }]
        )
        #print(resp.output_text)
        raw_output = resp.output_text
        cleaned = raw_output.strip("`").split("\n", 1)[1]
        cleaned = cleaned.rsplit("```", 1)[0]
        data = json.loads(cleaned.replace(': NA,', ': "NA",'))

        res1 = pd.DataFrame.from_dict({
            'tab': data['tab'],
            'date': data['date'],
            'time': data['time'],
            'updated_time': data['updated_time'],
        }, orient='index').T
        res2 = pd.DataFrame(
            [dict(list(data["categories"].items()))]
        )
        res2.columns = [f'category_{x.lower()}' for x in res2.columns]
        row_dict = {f"app_{d['app'].lower()}": d['duration'] for d in data['most_used_apps']}
        res3 = pd.DataFrame([row_dict])
        res = pd.concat([res1, res2, res3], axis=1)
        res['scid_u'] = img_scid
        res_all = pd.concat([res_all, res], axis=0)
        res_all.to_csv(folder_path / 'img_content.csv', index=False)
        print(f'{i} done: {img_scid}')

    except:
        fail_l6.append(img_scid)
        pd.DataFrame([fail_l6]).T.to_csv(folder_path / 'img_content_fail6.csv', index=False)
        print(f'{i} fail')

fail_l7 = []
for i,img_scid in enumerate(fail_l6[0:]):
    try:
        idd = create_file(folder_path / 'screenshot' / 'ema_all' / img_scid)
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": pp},
                    {"type": "input_image", "file_id": idd,
                    },
                ]
            }]
        )
        #print(resp.output_text)
        raw_output = resp.output_text
        cleaned = raw_output.strip("`").split("\n", 1)[1]
        cleaned = cleaned.rsplit("```", 1)[0]
        if cleaned.strip()[0] != '{':
            cleaned = '{' + cleaned
        data = json.loads(cleaned.replace(': NA,', ': "NA",').replace(': NA}', ': "NA"}'))

        res1 = pd.DataFrame.from_dict({
            'tab': data['tab'],
            'date': data['date'],
            'time': data['time'],
            'updated_time': data['updated_time'],
        }, orient='index').T
        res2 = pd.DataFrame(
            [dict(list(data["categories"].items()))]
        )
        res2.columns = [f'category_{x.lower()}' for x in res2.columns]
        row_dict = {f"app_{d['app'].lower()}": d['duration'] for d in data['most_used_apps']}
        res3 = pd.DataFrame([row_dict])
        res = pd.concat([res1, res2, res3], axis=1)
        res['scid_u'] = img_scid
        res_all = pd.concat([res_all, res], axis=0)
        res_all.to_csv(folder_path / 'img_content.csv', index=False)
        print(f'{i} done: {img_scid}')

    except:
        fail_l7.append(img_scid)
        pd.DataFrame([fail_l7]).T.to_csv(folder_path / 'img_content_fail7.csv', index=False)
        print(f'{i} fail')
## check the errors
# Create the folder if it doesn't exist
source_folder = folder_path / 'screenshot' / 'ema_all'
target_folder = folder_path / 'screenshot' / 'failed_5'
os.makedirs(target_folder, exist_ok=True)
for file_name in fail_l5:
    src_path = os.path.join(source_folder, file_name)
    dst_path = os.path.join(target_folder, file_name)

    if os.path.exists(src_path):
        shutil.copy2(src_path, dst_path)  # copy2 preserves metadata
        print(f"Copied: {file_name}")
    else:
        print(f"File not found, skipped: {file_name}")
print("Done copying files!")

## now do the next step of coding
all_file = os.listdir(folder_path / 'screenshot' / 'ema_all')
res_all = pd.read_csv(folder_path / 'img_content.csv')
assert res_all.shape[0] == len(all_file)
import numpy as np
from PIL import Image, ImageOps
import os

## clean the updated time col
res_all.loc[res_all['updated_time'].apply(lambda x: len(str(x))==4), 'updated_time'] = 'Updated today at 6:13 PM'



def crop_top_bar(img_name, input_path=source_folder, output_path=folder_path/'screenshot'/'after_cropped',
                 top_ratio=0.05, width_ratio=0.25):
    """
    Keep only the top `top_ratio` of the image height and save the result.
    - input_path: path to the source image
    - output_path: where to save (default: add '_top' before extension)
    - top_ratio: fraction of height to keep (e.g., 0.10 for top 10%)
    """
    # Open and normalize orientation
    img = Image.open(input_path/img_name)
    img = ImageOps.exif_transpose(img)

    w, h = img.size
    keep_h = max(1, int(h * top_ratio))  # at least 1px
    keep_w = max(1, int(w * width_ratio))  # at least 1px
    cropped = img.crop((0, 0, keep_w, keep_h))

    root, ext = os.path.splitext(img_name)
    output_name = output_path / f"{root}{ext}"

    # Preserve format if possible
    cropped.save(output_name)
    print(f"Saved: {output_name}")

need_cropl = res_all.loc[res_all['updated_time'].isna(), 'scid_u'].tolist()
for i, imgg in enumerate(need_cropl):
    crop_top_bar(img_name=imgg)

## further read another round of time points from the data
pp2 = textwrap.dedent(f"""\
Extract the time shown in the plot.

Output the result strictly in the format HH:MM (e.g., 06:08, 05:03, 11:23).

If no time is present, output exactly "Missing".

Do not infer, reformat, or estimate—return the text exactly as it appears in the image.
""")
read_time = pd.DataFrame()
fail_readtime_l = []
for i,img_scid in enumerate(need_cropl[12:]):
    try:
        idd = create_file(folder_path / 'screenshot' / 'after_cropped' / img_scid)
        resp = client.responses.create(
            model="gpt-5-mini",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": pp2},
                    {"type": "input_image", "file_id": idd,
                    },
                ]
            }]
        )
        #print(resp.output_text)
        raw_output = resp.output_text
        read_time = pd.concat([read_time, pd.DataFrame([[img_scid, raw_output]]).rename(columns={0:'scid_u', 1:'update_time'})], axis=0)
        read_time.to_csv(folder_path / 'img_content_readtime.csv', index=False)
        print(f'{i} done: {img_scid}')

    except:
        fail_readtime_l.append(img_scid)
        pd.DataFrame([fail_readtime_l]).T.to_csv(folder_path / 'img_content_readtime_fail.csv', index=False)
        print(f'{i} fail')

read_time = pd.read_csv(folder_path / 'img_content_readtime.csv')
read_time_fail = pd.read_csv(folder_path / 'img_content_readtime_fail.csv')

## manul input the unreaded plots
source_folder = folder_path / 'screenshot' / 'ema_all'
target_folder = folder_path / 'screenshot' / 'failed_croped_img'
os.makedirs(target_folder, exist_ok=True)
for file_name in read_time_fail['0'].tolist():
    src_path = os.path.join(source_folder, file_name)
    dst_path = os.path.join(target_folder, file_name)

    if os.path.exists(src_path):
        shutil.copy2(src_path, dst_path)  # copy2 preserves metadata
        print(f"Copied: {file_name}")
    else:
        print(f"File not found, skipped: {file_name}")
print("Done copying files!")
manual_croped_date = ['06:35', '05:01', '09:02', '05:04', '09:01',
                      '05:02', '08:29', '05:04', '09:24', '05:02',
                      '09:07', '05:05', '09:01', '05:03', 'Missing']
read_time_fail['update_time']= manual_croped_date
read_time_fail = read_time_fail.rename(columns={'0':'scid_u'})
read_time = pd.concat([read_time, read_time_fail], axis=0)
read_time = read_time.rename(columns={'update_time':'updated_time'})
read_time.shape

assert set(res_all.loc[res_all['updated_time'].isna(), 'scid_u'].tolist()) - set(read_time['scid_u'].tolist()) == set()
assert set(read_time['scid_u'].tolist()) - set(res_all.loc[res_all['updated_time'].isna(), 'scid_u'].tolist()) == set()
merge_d = dict(zip(read_time['scid_u'].tolist(), read_time['updated_time'].tolist()))
mask = res_all['updated_time'].isna()
res_all.loc[mask, 'updated_time'] = res_all.loc[mask, 'scid_u'].map(merge_d)
#res_all['updated_time'].iloc[10:30]
import re
 #now check the maotch or not
def extract_time_from_scid(scid):
    """
    Extract time from scid_u like: ..._16_07_2025_05-41 PM_Q29.jpg
    Returns minutes since midnight (0-1439) or None.
    """
    if not isinstance(scid, str):
        return None
    m = re.search(r'(\d{1,2})[-:](\d{2})\s*(AM|PM)', scid, flags=re.IGNORECASE)
    if not m:
        return None
    hh, mm, ap = m.groups()
    h = int(hh)
    mnt = int(mm)
    ap = ap.upper()
    # 12-hour -> minutes since midnight
    if ap == "AM":
        if h == 12:
            h = 0
    else:  # PM
        if h != 12:
            h += 12
    return h * 60 + mnt


def parse_updated_time(s, ref_meridiem='PM'):
    """
    Parse 'updated_time' strings to minutes since midnight.
    Handles:
      - 'HH:MM' (assumes same AM/PM as ref_meridiem if provided; otherwise treats as 24h)
      - 'Updated today at H:MM AM/PM'
    Returns minutes since midnight (0-1439) or None.
    """
    if not isinstance(s, str):
        return None
    s_clean = s.strip()

    # Case: 'Updated today at 6:14 PM' or "Today at 6:14 PM"
    m = re.search(r'(?:Updated\s+)?Today\s+at\s+(\d{1,2}):(\d{2})\s*(AM|PM)\b',
                  s_clean, flags=re.IGNORECASE)
    if m:
        hh, mm, ap = m.groups()
        h = int(hh); mnt = int(mm); ap = ap.upper()
        if ap == "AM":
            if h == 12: h = 0
        else:
            if h != 12: h += 12
        return h * 60 + mnt

    # Case: plain 'HH:MM'
    m = re.fullmatch(r'(\d{1,2}):(\d{2})', s_clean)
    if m:
        h = int(m.group(1))
        mnt = int(m.group(2))
        if ref_meridiem in ("AM", "PM"):
            # assume same meridiem as the filename’s time
            if ref_meridiem == "AM":
                if h == 12: h = 0
            else:  # PM
                if h != 12: h += 12
            return h * 60 + mnt
        else:
            # treat as 24-hour clock if no reference meridiem available
            if 0 <= h < 24:
                return h * 60 + mnt
            return None

    return None


def extract_meridiem_from_scid(scid):
    """Return 'AM'/'PM' if present in scid_u, else None."""
    if not isinstance(scid, str):
        return None
    m = re.search(r'_(\d{2})-(\d{2})\s*(AM|PM)\b', scid, flags=re.IGNORECASE)
    return m.group(3).upper() if m else 'PM'


def minutes_diff_circular(a, b):
    """Minimal circular difference in minutes on a 24h clock."""
    if a is None or b is None:
        return None
    d = abs(a - b)
    return min(d, 1440 - d)


# --- main computation on your DataFrame `res_all` ---
# 1) extract reference (filename) time in minutes & meridiem
res_all["scid_minutes"] = res_all["scid_u"].apply(extract_time_from_scid)
res_all["scid_meridiem"] = res_all["scid_u"].apply(extract_meridiem_from_scid)

# 2) parse updated_time using the scid’s meridiem as a hint for plain HH:MM strings
res_all["updated_minutes"] = [
    parse_updated_time(ut, ref_meridiem=mer)
    for ut, mer in zip(res_all["updated_time"], res_all["scid_meridiem"])
]

# 3) compute minimal difference and flag within ±5 minutes
res_all["time_diff_min"] = [
    minutes_diff_circular(u, s) for u, s in zip(res_all["updated_minutes"], res_all["scid_minutes"])
]
res_all["quality_flag_timecheck"] = res_all["time_diff_min"].apply(lambda d: bool(d is not None and d <= 60))
res_all["quality_flag_timecheck"].value_counts()
res_all.loc[res_all['quality_flag_timecheck']==False,['scid_u','updated_time','scid_minutes', 'updated_minutes', 'time_diff_min', 'quality_flag_timecheck']].iloc[3].tolist()
res_all.loc[res_all['quality_flag_timecheck']==False, 'updated_time'].tolist()

## process another date columns
res_all['date'].unique()
from dateutil import parser
import re
from datetime import datetime


def clean_date(s, reference_date=None):
    """
    Convert messy date strings into standard YYYY-MM-DD.
    - s: raw date string
    - reference_date: assumed year (default: current year)
    """
    s = str(s).strip().lower()

    if reference_date is None:
        reference_date = datetime.today()
    year = reference_date.year

    # Remove 'today' if present
    s = re.sub(r"\btoday\b", "", s).strip(", ")

    try:
        # Parse with dateutil
        dt = parser.parse(s, dayfirst=False, yearfirst=False, fuzzy=True, default=reference_date)
        # Force year to current year if missing
        if not re.search(r"\d{4}", s):
            dt = dt.replace(year=year)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None  # if parsing fails

res_all['date_cleaned'] = res_all['date'].apply(clean_date)
res_all[['date_cleaned', 'scid_u']]
def extract_date_from_scid(scid):
    """
    Extracts date from scid_u string (pattern dd_mm_yyyy).
    Returns string in YYYY-MM-DD format.
    """
    match = re.search(r"(\d{2})_(\d{2})_(\d{4})", scid)
    if match:
        day, month, year = match.groups()
        return datetime.strptime(f"{day}-{month}-{year}", "%d-%m-%Y").strftime("%Y-%m-%d")
    return None

res_all["date_from_scid"] = res_all["scid_u"].apply(extract_date_from_scid)
res_all["date_match"] = res_all["date_cleaned"] == res_all["date_from_scid"]
res_all["date_match"].value_counts(dropna=False)
res_all["date_cleaned"].value_counts(dropna=False)
res_all = res_all.rename(columns={'date_match':'quality_flag_date'})
res_all.loc[res_all["date_cleaned"].isna(), 'quality_flag_date'] = None
res_all["quality_flag_date"].value_counts(dropna=False)

##
res_all.columns
res_all['tab'].value_counts(dropna=False)
res_all['date'].value_counts(dropna=False)
res_all['time'].value_counts(dropna=False)
res_all['time'].unique()
res_all['updated_time'].value_counts(dropna=False)

str(row['date']).lower() == 'nan'
res_all['quality_flag_dayview'] = res_all['tab'].apply(lambda x: 'week' not in str(x).lower())
res_all['quality_flag_dayview'].value_counts(dropna=False)
res_all.loc[res_all["tab"].isna(), 'quality_flag_dayview'] = None
res_all['quality_flag_dayview'].value_counts(dropna=False)

## transfer the time period
## manual correct the other format of the time variable
cat_cols = [x for x in res_all.columns if 'category_' in x]
app_cols = [x for x in res_all.columns if 'app_' in x]
res_all[res_all['time'].apply(lambda x: len(re.findall(r"[a-zA-Z]+", str(x).lower()))==0)]
for cat in cat_cols:
    ll = [x for x in set(res_all[cat].dropna().tolist()) if 'than' in x]
    if len(ll)>0:
        print(cat, '__', ll)
for cat in app_cols:
    ll = [x for x in set(res_all[cat].dropna().tolist()) if 'than' in x]
    if len(ll)>0:
        print(cat, '__', ll)
for cat in app_cols:
    ll = [x for x in set(res_all[cat].dropna().tolist()) if 'Less' in x]
    if len(ll)>0:
        print(cat, '__', ll)
## clean the three rows
'''app_healthlibraryportal.elsevier.com __ ['Less than 1 minute']
app_login.parkland.edu __ ['Less than 1 minute']
app_prettylittlething.us __ ['Less than 1 minute']'''
res_all.loc[res_all['app_healthlibraryportal.elsevier.com']=='Less than 1 minute', 'app_healthlibraryportal.elsevier.com'] = '0min'
res_all.loc[res_all['app_login.parkland.edu']=='Less than 1 minute', 'app_login.parkland.edu'] = '0min'
res_all.loc[res_all['app_prettylittlething.us']=='Less than 1 minute', 'app_prettylittlething.us'] = '0min'

assert res_all.loc[983, 'time'] == '10小時44分鐘'
assert res_all.loc[2122, 'time'] == '15小時 4分鐘'
res_all.loc[983, 'time'] = '10h 44min'
res_all.loc[2122, 'time'] = '15h 4min'

## check unique tokens

hour_tokens = set()
min_tokens = set()
sec_tokens = set()
for d in res_all['time'].tolist():
    # find sequences of letters in the string
    tokens = re.findall(r"[a-zA-Z]+", str(d).lower())
    for t in tokens:
        if "h" in t:  # crude check if token refers to hours
            hour_tokens.add(t)
        if "m" in t:  # crude check if token refers to hours
            min_tokens.add(t)
        if "s" in t:  # crude check if token refers to hours
            sec_tokens.add(t)
for cat in cat_cols:
    for d in res_all[cat].dropna().tolist():
        # find sequences of letters in the string
        tokens = re.findall(r"[a-zA-Z]+", str(d).lower())
        for t in tokens:
            if "h" in t:  # crude check if token refers to hours
                hour_tokens.add(t)
            if "m" in t:  # crude check if token refers to hours
                min_tokens.add(t)
            if "s" in t:  # crude check if token refers to hours
                sec_tokens.add(t)
for app in app_cols:
    for d in res_all[app].dropna().tolist():
        # find sequences of letters in the string
        tokens = re.findall(r"[a-zA-Z]+", str(d).lower())
        for t in tokens:
            if "h" in t:  # crude check if token refers to hours
                hour_tokens.add(t)
            if "m" in t:  # crude check if token refers to hours
                min_tokens.add(t)
            if "s" in t:  # crude check if token refers to hours
                sec_tokens.add(t)
print("Unique hour tokens found:", hour_tokens)
print("Unique min tokens found:", min_tokens)
print("Unique sec tokens found:", sec_tokens)

# Precompile patterns for speed/readability
HOUR_RE   = re.compile(r'(\d+)\s*(h|hr|hrs|hour|hours)\b', re.IGNORECASE)
MIN_RE    = re.compile(r'(\d+)\s*(m|min|mins|minute|minutes)\b', re.IGNORECASE)
SEC_RE    = re.compile(r'(\d+)\s*(s|sec|secs|second|seconds)\b', re.IGNORECASE)

def duration_to_minutes(text):
    """
    Convert a duration string into total minutes.
    Handles tokens like: h, hr, hrs, hour(s); m, min(s), minute(s); s, sec(s), second(s).
    Works with or without spaces (e.g., '9h03m', '5 hr, 23 min', '1m 3s', '46m').
    Rounds seconds >= 30 up to the next minute.
    """
    if text is None:
        return None

    s = str(text).lower()
    # Normalize separators
    s = s.replace(",", " ").strip()
    s = re.sub(r'\s+', ' ', s)

    hours = sum(int(n) for n, _ in HOUR_RE.findall(s))
    minutes = sum(int(n) for n, _ in MIN_RE.findall(s))
    seconds = sum(int(n) for n, _ in SEC_RE.findall(s))

    # If nothing matched but the string is just digits, treat as minutes
    if hours == minutes == seconds == 0 and re.fullmatch(r'\d+', s):
        minutes = int(s)

    total_minutes = hours * 60 + minutes + (1 if seconds >= 30 else 0)
    return total_minutes

res_all["total_time"] = res_all["time"].apply(duration_to_minutes)
for cat in cat_cols:
    res_all[cat] = res_all[cat].apply(duration_to_minutes)
for app in app_cols:
    res_all[app] = res_all[app].apply(duration_to_minutes)
res_all.to_csv(folder_path / 'img_content_processed.csv', index=False)
res_all0 = pd.read_csv(folder_path / 'img_content_processed.csv')

## get the most frequent categories
app_browser = ['app_safari', 'app_chrome', 'app_google', 'app_google.com', 'app_firefox',
               'app_brave browser',
               'app_ecosia browser',
               'app_adblock browser',
               'app_icabmobile',
               'app_brave',
                'app_瀏覽器',
'app_samsung internet',
 'app_edge',
 'app_aloha', 'app_bing',
 'app_msn'
               ]
app_message = ['app_messages', 'app_messenger', 'app_whatsapp',  'app_qq',
 'app_chat',
 'app_textnow', 'app_groupme',
 'app_wechat',
 'app_live.remesh.chat',
 'app_messaging',
 'app_mess',]
app_mail = ['app_gmail', 'app_mail', 'app_outlook', 'app_yahoo mail', 'app_outlook.live.com', 'app_mail.google.com',
            'app_email']
app_map = ['app_maps', 'app_google maps', 'app_waze', 'app_transit',
           'app_route',
           'app_parkwhiz.com',
'app_traintime',
 'app_google earth'
           ]
app_music = ['app_spotify', 'app_music', 'app_yt music',
 'app_youtube music',
 'app_amazon music', 'app_musi',
             'app_iheartradio',
             'app_pocket casts'
             ]
app_shop = ['app_amazon', 'app_walmart','app_instacart',
 'app_amazon shopping',
 'app_ebay',
 'app_depop',
 'app_shein',
 "app_mcdonald\'s",
 'app_mercari',
 'app_doordash',
 'app_food street',
 'app_whatnot',
            'app_poshmark',
            'app_shop',
            'app_shopping & food',
            'app_target',
            'app_uber eats',
            'app_the realreal',
            'app_wholefoods.icanmakeitb...',
            'app_etsy',
            'app_popeyes',
            'app_my h-e-b',
'app_fetch',
 'app_receiptjar',
 'app_yelp',
 'app_sezzle',
 'app_shopper',
 'app_prettylittlething.us',
 'app_ticketmaster',
 'app_kfc',
 'app_chick-fil-a',
 'app_dasher',
 'app_accesssecurepak.com',
'app_temu',
 'app_receiptpal',
 'app_old navy',
 "app_sam's club",
 'app_kroger.com',
 'app_chewy',
 'app_b&bw',
 'app_heb.com',
 'app_aliexpress',
 "app_domino's",
 'app_starbucks',
 'app_kroger',
 'app_albertsons',
 "app_dunkin'",
 'app_dairy queen®',
 'app_subway',
 'app_dairy queen',
 'app_subway®',
 'app_grubhub',
 'app_hollister'
           ]
app_photo = ['app_camera', 'app_photos',  'app_screenshots',
 'app_capcut',
 'app_gallery',
 'app_google photos',
             'app_canva: ai photo & video editor',
             'app_inshot',
             'app_incollage - coll...',
             'app_edits'
             ]
app_game = ['app_monopolygo', 'app_duckduckgo', 'app_match factory', 'app_pokémon go', 'app_bingo blitz', 'app_solitaire clash',
            'app_domino dreams',
            'app_bus escape: traffic jam',
            'app_cookie run: kingdom',
            'app_royal match',
            'app_dot link',
            'app_water sort',
            'app_fanduel faceoff',
            'app_coin master',
            'app_word solitaire',
            'app_candy crush saga',
            'app_merge gardens',
            'app_fish of fortune',
            'app_word search',
            'app_happy color',
            'app_match masters',
            'app_clawcrazy',
            'app_puzzles & spells',
            'app_solitaire',
            'app_bingo showdown',
            'app_block blast!',
            'app_monopoly go',
            'app_magic puzzles',
            'app_fortnite',
            'app_pokémon tcgp',
            'app_fate/go',
            'app_solitaire cash',
            'app_match jong',
            'app_traffic escape!',
            'app_gossip harbor',
            'app_triple factory',
            'app_phase 10',
            'app_blitz',
            'app_worldwinner',
            'app_balloon master 3d',
            'app_legend city',
            'app_merge studio',
            'app_pocket7games',
            'app_dice dreams',
            'app_baseball 9',
            'app_fanduel fantasy sports',
            'app_spades+',
            'app_coin chef',
            'app_solitaire slam',
            'app_bullet smile',
            'app_bingo frenzy',
            'app_i am cat',
            'app_godzilla x kong',
            'app_bus craze',
            'app_ladder',
            'app_domino ocean',
            'app_farm adventure',
            'app_myvegas',
            'app_g eternal',
            'app_sword of convallaria',
            'app_gardenscapes',
            'app_honkai: star rail',
            'app_sb daily trivia',
            'app_pokehub',
            'app_angry birds 2',
            'app_nyt games',
            'app_draftkings',
            'app_fly coin voyage',
            'app_euchre 3d',
            'app_ballz',
            'app_delta force',
'app_goblins wood',
 'app_zen color',
 'app_euchre 3d',
 'app_skip-bo',
 'app_love & fashion',
 'app_pokehub',
 'app_nyt games',
 'app_ballz',
 'app_angry birds 2',
 'app_magic',
 'app_brawl stars',
 'app_toy blast',
 "app_june's journey",
 'app_monopoly: bingo!',
 'app_guessperson',
 'app_boom blocks',
 'app_fishing master',
 'app_maze',
 'app_candy crush...',
 'app_abradoodle bingo',
 'app_girl rescue',
 'app_sand blast!',
 'app_car parking',
 'app_games',
'app_solitaire: card games',
 'app_woody block color blast',
 'app_bingo bash',
 'app_pulszbingo.com',
 'app_clash royale',
 'app_ragdollbreak',
 'app_akinator',
 'app_bubble pop shooter',
 'app_screw master 3d: pin puzzle',
 'app_bus frenzy',
 'app_toy match 3d',
 'app_perfect tidy',
 'app_spider',
 'app_misty continent',
 'app_covet',
 'app_paint by number',
 'app_stardew valley',
 'app_pokémon tcg',
 'app_livly island',
 'app_solitaire verse',
 'app_lively island',
 'app_midas merge',
 'app_offline games',
 'app_spider solitaire - c',
 'app_neuronation',
 'app_spider solitaire',
 'app_mario kart',
 'app_ginrummyplus',
 'app_ginrummy',
 'app_traffic escape',
 'app_clash of clans'
            ]

cols_to_sum = [c for c in app_browser if c in res_all.columns]
res_all['app_sub_browser'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in app_message if c in res_all.columns]
res_all['app_sub_message'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in app_mail if c in res_all.columns]
res_all['app_sub_mail'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in app_map if c in res_all.columns]
res_all['app_sub_map'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in app_music if c in res_all.columns]
res_all['app_sub_music'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in app_shop if c in res_all.columns]
res_all['app_sub_shop'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in app_photo if c in res_all.columns]
res_all['app_sub_photo'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in app_game if c in res_all.columns]
res_all['app_sub_game'] = res_all[cols_to_sum].sum(axis=1)

app_cols2 = [x for x in res_all.columns if 'app_' in x]

res_all[app_cols2].sum(axis=0).reset_index().sort_values(by=[0], ascending=False).iloc[0:30]
final_chosen_app = ['app_sub_browser',
 'app_tiktok',
 'app_sub_game',
 'app_sub_message',
 'app_youtube',
 'app_facebook',
 'app_instagram',
 'app_sub_mail',
 'app_chatgpt',
 'app_reddit',
 'app_sub_shop',
 'app_x',
 'app_sub_music',
 'app_sub_photo',
 'app_snapchat',
 'app_sub_map',
 'app_kindle']
renamel = [
'app_Browser',
 'app_TikTok',
 'app_Game',
 'app_Message',
 'app_YouTube',
 'app_Facebook',
 'app_Instagram',
 'app_Mail',
 'app_Chatgpt',
 'app_Reddit',
 'app_Shop',
 'app_X',
 'app_Music',
 'app_Photo',
 'app_Snapchat',
 'app_Map',
 'app_Kindle'
]

res_all_out = res_all[final_chosen_app]
res_all_out = res_all_out.rename(columns=dict(zip(final_chosen_app, renamel)))
other_core_col = ['tab',
 'date',
 'time',
 'updated_time','scid_minutes',
 'scid_meridiem',
 'updated_minutes',
 'time_diff_min',
 'quality_flag_timecheck',
 'date_cleaned',
 'date_from_scid',
 'quality_flag_date',
 'quality_flag_dayview',
 'total_time',
                  'scid_u']
res_all_out = pd.concat([res_all_out, res_all[other_core_col]], axis=1)
res_all_out.shape

## finally, just the category variable
cat_cols
cat_social = ['category_social', 'category_instagram', 'category_社交', 'category_social media',
 'category_social networking',
              'category_facebook',
              'category_tiktok',
              'category_communication',
              'category_reddit',
              'category_discord',
              'category_twitter',
              'category_messenger',
              'category_whatsapp',
              'category_x',
              'category_messaging',
              'category_messages',
              'category_facetime',

              ]
cat_productivity = ['category_productivity & finance',
                    'category_productivity and finance',
                    'category_creativity','category_productivity and financial',
 'category_生產力與財務','category_fidelity','category_email',
 'category_mail',
 'category_outlook',
 'category_gmail',
 'category_yahoo mail',
 'category_chatgpt',
                    'category_productivity', 'category_productivity &...', 'category_productivity &',
                    'category_surveys',
                    'category_notes',
                    'category_loop11 user testing',
                    'category_expiwell',
                    ]
cat_entertainment = ['category_entertainment', 'category_video', 'category_audio', 'category_youtube', 'category_影片',
                     'category_spotify',
                     'category_streaming',
                     'category_hd movies 2025 watch',
                     'category_watch hd movies 2025',
                     'category_freevee',
                     'category_roku',
                     'category_vizio',
                     'category_prime video',
'category_music',
 'category_music & audio',
 'category_yt music',
 'category_music','category_media'
                     ]
cat_games = ['category_games',
             'category_solitaire',
             'category_honkai: star rail',
             'category_spider solitaire',
             'category_ginrummyplus',
             'category_clash of clans',
             'category_offline games',
             'category_solitaire verse',
             'category_livly island',
             'category_lively island',
             'category_midas merge',
             'category_pokémon go',
'category_gaming',
             ]
cat_utilities = ['category_utilities',
                 'category_settings',
                 'category_home & lock screen',
                 'category_reminders',
                 'category_phone',
                 'category_file',
                 'category_camera',
                 'category_clock',
'category_image', 'category_photos','category_play store',
                 'category_blink',
                 'category_tuta',
                 'category_keep notes',
                 'category_ringcentral',
                 'category_ring',
'category_health & fitness', 'category_travel', 'category_maps and travel',
                'category_health and fitness',
                'category_health',
                'category_treat',
                'category_navigation',
                'category_maps'

                 ]
cat_information = ['category_information & reading', 'category_education', 'category_reading', 'category_chrome',
                   'category_browsing', 'category_web browsing',
                   'category_news and information',
                   'category_news & magazines',
                   'category_information',
                   'category_news',
                   'category_blogging',
                   'category_google',
                   'category_web',
'category_internet','category_search',
                   'category_newsbreak',
                   'category_bing',
                   'category_kindle',
                   'category_google earth',
                   'category_browser',
                   'category_brave',
                   'category_opera'
                   ]
cat_shop = ['category_shopping & food', 'category_shopping and food',
            'category_shopping',
            'category_food & drink',
            'category_b&bw',
            'category_amazon shop',
            "category_mcdonald's",
            'category_pinterest',
            'category_burger king',
            'category_fliff',
            'category_walmart',
            'category_doordash',
            'category_amazon shopping',
            "category_wendy's",
            'category_dairy queen®',
            'category_amazon music',  # borderline → music, but also retail ecosystem
            "category_domino's",
            'category_subway®',

            ]
cat_others = ['category_其他', 'category_other',
              'category_others',
              'category_na',
              'category_fac...',
              'category_red...',
              'category_face...',
              'category_expaiwell',
              'category_five surveys',
              'category_freecash',
              'category_prime opinion',
              'category_mess',
              'category_heycash',
              'category_wicshop...',
              'category_thrillzz',
              'category_rebet',
              'category_videous',
              'category_an earn app by mode',
              'category_coin'
              ]

cols_to_sum = [c for c in cat_social if c in res_all.columns]
res_all['cat_social'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in cat_productivity if c in res_all.columns]
res_all['cat_productivity'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in cat_entertainment if c in res_all.columns]
res_all['cat_entertainment'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in cat_games if c in res_all.columns]
res_all['cat_games'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in cat_utilities if c in res_all.columns]
res_all['cat_utilities'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in cat_information if c in res_all.columns]
res_all['cat_information'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in cat_shop if c in res_all.columns]
res_all['cat_shop'] = res_all[cols_to_sum].sum(axis=1)

cols_to_sum = [c for c in cat_others if c in res_all.columns]
res_all['cat_others'] = res_all[cols_to_sum].sum(axis=1)

[x for x in res_all if 'cat_' in x]
final_chosen_cat = ['cat_social',
 'cat_productivity',
 'cat_entertainment',
 'cat_games',
 'cat_utilities',
 'cat_information',
 'cat_shop',
 'cat_others']
res_all_out2 = res_all[final_chosen_cat]
res_all_out = pd.concat([res_all_out, res_all_out2], axis=1)
res_all_out.shape

res_all_out.to_csv(folder_path / 'img_content_processed_cleaned_0906.csv', index=False)
res_all.to_csv(folder_path / 'img_content_processed_0906.csv', index=False)

## now merge the image results with the dc dataframe
#dc.to_csv(folder_path / 'smu_for_analysis_withclass_baseline_endline_screenshot_0827.csv')
## first between-person level
res_all_out['pid'] = res_all_out['scid_u'].apply(lambda x: x.split('_Activity')[0])
res_all_out.groupby(['pid'])['scid_u'].count().reset_index()


## quality check
# Aggregate your data
counts_df = res_all_out.groupby(['pid'])['scid_u'].count().reset_index()
counts = counts_df['scid_u']

## my selected color! #6D2F20FF, #B75347FF, #DF7E66FF, #E09351FF, #EDC775FF, #94B594FF, #224B5EFF
## another one: #4F6980FF, #849DB1FF, #A2CEAAFF, #638B66FF, #BFBB60FF, #F47942FF, #FBB04EFF, #B66353FF, #D7CE9FFF, #B9AA97FF, #7E756DFF -- miller stone

# Create figure and axis
fig, ax = plt.subplots(figsize=(8, 6))
bins = np.arange(0.5, 14.5 + 1)
ax.hist(counts, bins=bins, edgecolor='white', color='#4F6980FF')  # adjust bins as needed
ax.set_xlabel("Number of screenshots per user", fontsize=13)
ax.set_ylabel("Frequency", fontsize=13)
ax.set_xticks(range(1, 15))
ax.set_title("Distribution of donated screenshots per user", fontsize=15, fontweight='bold')
plt.tight_layout()
fig.savefig(folder_path / 'res' / "screenshot_per_user_histogram.png", dpi=300)

## create quality check result
qual_cols = ['quality_flag_timecheck', 'quality_flag_date', 'quality_flag_dayview']
res_all_out['quality_all3'] = res_all_out[qual_cols].all(axis=1)
res_all_out['quality_atleast1'] = res_all_out[qual_cols].any(axis=1)

res_all_out['quality_flag_timecheck'].value_counts(dropna=False)
res_all_out['quality_flag_date'].value_counts(dropna=False)
res_all_out['quality_flag_dayview'].value_counts(dropna=False)

res_all_out.loc[res_all_out['quality_flag_dayview'].isna(),'quality_flag_dayview'] = 'missing'
res_all_out.loc[res_all_out['quality_flag_date'].isna(),'quality_flag_date'] = 'missing'

res_all_out['quality_all3'].value_counts(dropna=False)
res_all_out['quality_atleast1'].value_counts(dropna=False)

# Build one dataframe per column
df_timecheck = res_all_out['quality_flag_timecheck'].value_counts(dropna=False).reset_index()
df_timecheck.columns = ['Value', 'count']
df_timecheck['qual_var'] = 'quality_flag_timecheck'

df_date = res_all_out['quality_flag_date'].value_counts(dropna=False).reset_index()
df_date.columns = ['Value', 'count']
df_date['qual_var'] = 'quality_flag_date'

df_dayview = res_all_out['quality_flag_dayview'].value_counts(dropna=False).reset_index()
df_dayview.columns = ['Value', 'count']
df_dayview['qual_var'] = 'quality_flag_dayview'

stats_df = pd.concat([df_timecheck, df_date, df_dayview], axis=0)
print(stats_df)

stats_df["Value"] = stats_df["Value"].replace("missing", "NA")

# Ensure consistent ordering of Value
order = ["NA", False, True]
colors = {True: "#4F6980FF", False: "#FBB04EFF", 'NA': "lightgrey"}
pivot_df = stats_df.pivot(index="qual_var", columns="Value", values="count").fillna(0)[order]

fig, ax = plt.subplots(figsize=(8, 6))
bar_height = 0.25
y_positions = range(len(pivot_df))

for j, val in enumerate(order):
    ax.barh(
        [y + j * bar_height for y in y_positions],
        pivot_df[val],
        height=bar_height,
        color=colors[val],
        edgecolor="white",
        label=val
    )

# Y-axis labels centered at group
ax.set_yticks([y + bar_height for y in y_positions])
ax.set_yticklabels(['Screenshot\ndates match\nwith survey','Screenshot\nis in the\nday view', 'Screenshot\ntimes match\nwith survey'], fontsize=13)

ax.set_xlabel("Count of screenshots", fontsize=13)
ax.set_title("Quality Check Distribution of Screenshots", fontsize=15, fontweight='bold')
handles, labels = ax.get_legend_handles_labels()
by_label = dict(zip(labels, handles))
ax.legend(by_label.values(), by_label.keys(), title="", fontsize=13)
plt.tight_layout()
fig.savefig(folder_path / 'res'/ "quality_flags_grouped.png", dpi=300)

## aother quality check plot
res_all_out['quality_all3'].value_counts(dropna=False)
res_all_out['quality_atleast1'].value_counts(dropna=False)
# Example counts from your output

# Get counts (robust to missing categories)
all3_counts = res_all_out['quality_all3'].value_counts(dropna=False)
atleast1_counts = res_all_out['quality_atleast1'].value_counts(dropna=False)
colors2 = {'True': "#4F6980FF", 'False': "#FBB04EFF", 'NA': "lightgrey"}
def get_count(s, key):
    # value_counts keys are booleans; ensure missing returns 0
    return int(s.get(key, 0))

# Data for plotting (order True then False)
groups = [
    "Satisfied all 3\nquality checks",
    "Satisfied at least 1\nquality checks"
]
true_counts  = [get_count(all3_counts, True),  get_count(atleast1_counts, True)]
false_counts = [get_count(all3_counts, False), get_count(atleast1_counts, False)]

# Plot
fig, ax = plt.subplots(figsize=(8, 6))
x = range(len(groups))
bar_width = 0.35

bars_true = ax.bar([xi - bar_width/2 for xi in x], true_counts,
                   width=bar_width, color=colors2['True'], edgecolor="white", label="True")
bars_false = ax.bar([xi + bar_width/2 for xi in x], false_counts,
                    width=bar_width, color=colors2['False'], edgecolor="white", label="False")

# Labels and title
ax.set_xticks(list(x))
ax.set_xticklabels(groups)
ax.set_ylabel("Count", fontsize=13)
ax.set_title("Screenshot quality Check Summary", fontsize=15, fontweight='bold')

# Legend
ax.legend(title="", fontsize=13)
ax.set_xticklabels(groups, fontsize=14)
# Annotate counts above bars
def annotate(bars):
    for b in bars:
        height = b.get_height()
        ax.text(b.get_x() + b.get_width()/2, height + max(1, 0.01*height),
                f"{int(height)}", ha="center", va="bottom", fontsize=9)

annotate(bars_true)
annotate(bars_false)

plt.tight_layout()
fig.savefig(folder_path / 'res' / "quality_flags_comparison.png", dpi=300)

# do it for another one -- time of missing
assert pd.merge(da[['scid_u', 'Day of Survey']], res_all_out, on=['scid_u'], how='inner').shape[0] == res_all_out.shape[0]
res_all_out = pd.merge(da[['scid_u', 'Day of Survey']], res_all_out, on=['scid_u'], how='inner')
df = res_all_out.groupby(['Day of Survey'])['scid_u'].count().reset_index()
df['Day of Survey'] = df['Day of Survey'] - 1
df.columns = ['Day of Study', 'count']
df['total'] = 266*2

# Plot
fig, ax = plt.subplots(figsize=(7, 5))

bars = ax.bar(df["Day of Study"], df["count"], color="#4F6980FF", edgecolor="white")
ax.set_xlabel("Day of Study", fontsize=13)
ax.set_ylabel("Count", fontsize=13)
ax.set_title("Number of Screenshots per Day of Study", fontsize=15, fontweight='bold')

import matplotlib.pyplot as plt
import pandas as pd

# Example DataFrame
df = pd.DataFrame({
    "Day of Study": [1, 2, 3, 4, 5, 6, 7],
    "count": [360, 360, 348, 333, 350, 346, 355],
    "total": [532, 532, 532, 532, 532, 532, 532]
})

# Compute the "missing" portion = total - count
df["missing"] = df["total"] - df["count"]

# Plot
fig, ax = plt.subplots(figsize=(8, 6))
#"#4F6980FF", 'False': "#FBB04EFF", 'NA': "#B9AA97FF"
bars_count = ax.bar(df["Day of Study"], df["count"],
                    color="#4F6980FF", edgecolor="black", label="Collected")
bars_missing = ax.bar(df["Day of Study"], df["missing"], bottom=df["count"],
                      color="lightgrey", edgecolor="black", label="Missing")

# Labels and title
ax.set_xlabel("Day of Study", fontsize=13)
ax.set_ylabel("Number of Screenshots", fontsize=13)
ax.set_title("Number of Screenshots by Days in Study", fontsize=15, fontweight='bold')

# Annotate collected counts
for bar in bars_count:
    height = bar.get_height()
    if height != 266*2:
        ax.text(bar.get_x() + bar.get_width()/2, height/2,
                str(height), ha='center', va='center', color="white", fontsize=9)
ax.set_ylim([0,560])
ax.legend(fontsize=13)
plt.tight_layout()
fig.savefig(folder_path / 'res'/ "counts_per_day_0906.png", dpi=300)

## now get the ndividual level time
time_col = ['cat_social',
 'cat_productivity',
 'cat_entertainment',
 'cat_games',
 'cat_utilities',
 'cat_information',
 'cat_shop',
 'cat_others',
            'total_time',
'app_Browser', 'app_TikTok', 'app_Game',
       'app_Message', 'app_YouTube', 'app_Facebook', 'app_Instagram',
       'app_Mail', 'app_Chatgpt', 'app_Reddit', 'app_Shop', 'app_X',
       'app_Music', 'app_Photo', 'app_Snapchat', 'app_Map', 'app_Kindle']
indi_df = res_all_out.groupby(['pid'])[time_col].mean().reset_index()

## rename cols
indi_df.columns

renamel = ['cat_social', 'cat_productivity', 'cat_entertainment',
       'cat_games', 'cat_utilities', 'cat_information', 'cat_shop',
       'cat_others', 'total_time', 'app_Browser', 'app_TikTok', 'app_Game',
       'app_Message', 'app_YouTube', 'app_Facebook', 'app_Instagram',
       'app_Mail', 'app_Chatgpt', 'app_Reddit', 'app_Shop', 'app_X',
       'app_Music', 'app_Photo', 'app_Snapchat', 'app_Map', 'app_Kindle']
indi_df = indi_df.rename(columns=dict(zip(renamel, [f'{x}_person' for x in renamel])))
indi_df = indi_df.rename(columns={'pid':'Participant ID'})
assert pd.merge(da, indi_df, on=['Participant ID'], how='left').shape[0] == da.shape[0]
da = pd.merge(da, indi_df, on=['Participant ID'], how='left')
assert pd.merge(dc, indi_df, on=['Participant ID'], how='left').shape[0] == dc.shape[0]
dc = pd.merge(dc, indi_df, on=['Participant ID'], how='left')
da.to_csv(folder_path / 'final_data_ema_0906.csv', index=False)
dc.to_csv(folder_path / 'smu_for_analysis_withclass_baseline_endline_0906.csv', index=False)
res_all_out.to_csv(folder_path / 'img_content_processed_cleaned_0906.csv', index=False)
res_all.to_csv(folder_path / 'img_content_processed_0906.csv', index=False)

## add the person day view
da = pd.read_csv(folder_path / 'final_data_ema_0906.csv')
dc = pd.read_csv(folder_path / 'smu_for_analysis_withclass_baseline_endline_0906.csv')
res_all_out = pd.read_csv(folder_path / 'img_content_processed_cleaned_0906.csv')
res_all = pd.read_csv(folder_path / 'img_content_processed_0906.csv')

'pid' in res_all_out.columns
res_all_out['pidd'] = res_all_out[['pid','date_from_scid']].apply(lambda x: x['date_from_scid']+'&'+x['pid'], axis=1)
res_all_out

example_pidd = res_all_out['pidd'].iloc[1]   # just grabs the first pidd value
g = res_all_out[res_all_out['pidd'] == example_pidd]
def _pick_row(g: pd.DataFrame) -> pd.DataFrame:
    # If only one row, keep it
    if len(g) == 1:
        return g

    # --- Step 0: quality flag checks (only if exactly 2 rows) ---
    if len(g) == 2:
        # Case 1: if one row has quality_flag_dayview == True
        if (g['quality_flag_dayview'] == 'True').any():
            cand = g[g['quality_flag_dayview'] == 'True']
            if len(cand) == 1:
                return cand

        # Case 2: if both quality_flag_dayview are False/NA, check quality_flag_date
        if (g['quality_flag_dayview'] != 'True').all() and (g['quality_flag_date'] == 'True').any():
            cand = g[g['quality_flag_date'] == True]
            if len(cand) == 1:
                return cand

        # Case 3: if both quality_flag_date are False/NA, check quality_flag_timecheck
        if (g['quality_flag_dayview'] != True).all() and (g['quality_flag_date'] != True).all() \
           and (g['quality_flag_timecheck'] == True).any():
            cand = g[g['quality_flag_timecheck'] == True]
            if len(cand) == 1:
                return cand
    # ------------------------------------------------------------

    # Step 1: keep rows where total_time is not NA (if available)
    cand = g[g['total_time'].notna()]
    if cand.empty:
        cand = g

    # Step 2: if all updated_minutes are not NA, pick the largest
    if len(cand) > 1 and cand['updated_minutes'].notna().all():
        idx = cand['updated_minutes'].idxmax()
        return g.loc[[idx]]

    # Step 3: otherwise, prefer scid_minutes if available
    scid = cand['scid_minutes']
    if scid.notna().any():
        idx = scid.idxmax()
    elif cand['updated_minutes'].notna().any():
        idx = cand['updated_minutes'].idxmax()
    else:
        idx = cand.index[0]

    return g.loc[[idx]]

result = res_all_out.groupby('pidd', group_keys=False).apply(_pick_row).reset_index(drop=True)
result = result.rename(columns=dict(zip(time_col, [f'{x}_personday' for x in time_col])))
selectl = [f'{x}_personday' for x in time_col]
selectl.append('pidd')
result = result[selectl]

## merge to the two df
from datetime import datetime

da['pidd'] = da[['Start Date', 'Participant ID']].apply(lambda x: f"{datetime.strptime(x['Start Date'].split(' ')[0], '%m/%d/%Y').strftime('%Y-%m-%d')}&{x['Participant ID']}", axis=1)
da['pidd'].nunique()
dc['pidd'] = dc[['Start Date', 'Participant ID']].apply(lambda x: f"{datetime.strptime(x['Start Date'].split(' ')[0], '%m/%d/%Y').strftime('%Y-%m-%d')}&{x['Participant ID']}", axis=1)
dc['pidd'].nunique()

assert pd.merge(da[['pidd']].drop_duplicates(), result[['pidd']], on=['pidd'], how='inner').shape[0] == result.shape[0]
assert result.shape[0] == result['pidd'].nunique()
da = pd.merge(da, result, on=['pidd'], how='left')
dc = pd.merge(dc, result, on=['pidd'], how='left')
print(da.shape, dc.shape)

da = pd.read_csv(folder_path / 'final_data_ema_0906.csv')
dc = pd.read_csv(folder_path / 'smu_for_analysis_withclass_baseline_endline_0906.csv')
res_all_out = pd.read_csv(folder_path / 'img_content_processed_cleaned_0906.csv')
res_all = pd.read_csv(folder_path / 'img_content_processed_0906.csv')

## now merge the diff
