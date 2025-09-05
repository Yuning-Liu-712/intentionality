from pathlib import Path
import zipfile
import pandas as pd
from datetime import datetime
import os
import shutil



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
from openai import OpenAI
import textwrap
import json

api_key = ""
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


## clean the updated time col
res_all.loc[res_all['updated_time'].apply(lambda x: len(str(x))==4), 'updated_time'] = 'Updated today at 6:13 PM'

from PIL import Image, ImageOps
import os

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
res_all['scid_u'][0]
row = res_all.iloc[0]
quality_flag_dayview = 'week' not in row['tab'].lower()
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

## get the most frequent categories
res_all0 = pd.read_csv(folder_path / 'img_content.csv')
#res_all0[app].value_counts(dropna=False)
num_app_l = []
for app in app_cols:
    num_app_l.append(res_all0[app].dropna().shape[0])
wanted_app_l = pd.DataFrame.from_dict(dict(zip(app_cols, num_app_l)), orient='index').reset_index().sort_values(by=[0], ascending=False).head(50)['index'].tolist()
wanted_app_l = [x for x in wanted_app_l if x not in ['app_app.prolific.com', 'app_expiwell']]
app_browser = ['app_safari', 'app_chrome', 'app_google', 'app_google.com', 'app_firefox',
               'app_brave browser',
               'app_ecosia browser',
               'app_adblock browser',
               'app_icabmobile',
               'app_brave',
                'app_瀏覽器'
               ]
app_message = ['app_messages', 'app_messenger', 'app_whatsapp',  'app_qq',
 'app_chat',
 'app_textnow',]
app_mail = ['app_gmail', 'app_mail', 'app_outlook', 'app_yahoo mail']
app_map = ['app_maps', 'app_google maps', 'app_waze', 'app_transit']
app_music = ['app_spotify', 'app_music', 'app_yt music',
 'app_youtube music',
 'app_amazon music',]
app_shop = ['app_amazon', 'app_walmart','app_instacart',
 'app_amazon shopping',
 'app_ebay',
 'app_depop',
 'app_shein',
 "app_mcdonald\'s",
 'app_mercari',
 'app_doordash',
 'app_food street',
 'app_whatnot']
app_photo = ['app_camera', 'app_photos']
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
            'app_delta force'
            ]

##game
ll_all = pd.DataFrame.from_dict(dict(zip(app_cols, num_app_l)), orient='index').reset_index().sort_values(by=[0], ascending=False).iloc[150:300]['index'].tolist()
[x for x in ll_all if 'amazon' in x]
set([ 'app_fate/go',
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
 'app_delta force']) - set(ll_all)
## now
rcc = pd.read_csv(folder_path / 'img_content_readtime.csv')
