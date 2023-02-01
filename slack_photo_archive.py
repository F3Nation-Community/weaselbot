from slack_sdk import WebClient
import os
from dotenv import load_dotenv
import ssl
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
import pickle
import requests
from PIL import Image as PillowImage
import piexif
import io
import cv2
import hashlib
import pickle

# Import checksum set, used to prevent duplicate uploads
with open('cache/checksum_set.pkl', 'rb') as checksum_pickle:
    checksum_set = pickle.load(checksum_pickle)

# Find start and end times
# Defaults to last month (currently breaks for Jaunary :))
eval_date = datetime.today()
eval_year = eval_date.year
eval_month = eval_date.month-1
start = str(round(datetime(eval_year, eval_month, 1, 0, 0, 0, 0).timestamp()))
end = str(round(datetime(eval_year, eval_month+1, 1, 0, 0, 0, 0).timestamp())-1)

# Inputs
just_pictures = True
DRIVE_FOLDER_ID = ''

# GPS location for metadata
gps_dict = {
    'ao-the-last-stop':         {1: b'N', 2: ((38, 1), (45, 1), (349, 100)), 3: b'W', 4: ((90, 1), (39, 1), (321, 100))},
    'ao-eagles-nest':           {1: b'N', 2: ((38, 1), (46, 1), (393, 100)), 3: b'W', 4: ((90, 1), (32, 1), (5629, 100))},
    'ao-braveheart':            {1: b'N', 2: ((38, 1), (45, 1), (1482, 100)), 3: b'W', 4: ((90, 1), (47, 1), (1011, 100))},
    'ao-pain-station':          {1: b'N', 2: ((38, 1), (48, 1), (5066, 100)), 3: b'W', 4: ((90, 1), (41, 1), (988, 100))},
    'ao-the-bayou':             {1: b'N', 2: ((38, 1), (48, 1), (4513, 100)), 3: b'W', 4: ((90, 1), (42, 1), (2114, 100))},
    'ao-running-with-animals':  {1: b'N', 2: ((38, 1), (44, 1), (657, 100)), 3: b'W', 4: ((90, 1), (43, 1), (2267, 100))},
    'ao-the-darkness':          {1: b'N', 2: ((38, 1), (46, 1), (43, 100)), 3: b'W', 4: ((90, 1), (34, 1), (1362, 100))},
}

# Import secrets
dummy = load_dotenv()
slack_secret = os.environ.get('slack_secret')

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']

# Drive service
def create_drive_service():
    creds = None
    if os.path.exists('secrets/token_drive.json'):
        creds = Credentials.from_authorized_user_file('secrets/token_slack_drive.json', SCOPES)

    try:
        service = build('drive', 'v3', credentials=creds)
        return service
    except HttpError as error:
        print(f'An error occurred: {error}')
        return None
    
# Photos service
def create_photos_service(client_secret_file, api_name, api_version, *scopes):
    print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]

    cred = None

    pickle_file = f'secrets/token_{API_SERVICE_NAME}_{API_VERSION}.pickle'
    # print(pickle_file)

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred, static_discovery=False)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print(e)
    return None
    
# Function to upload to Google Photos
def upload_to_google_photos(photo_name, description):
    # setup
    image_file = f'cache/{photo_name}'
    upload_url = 'https://photoslibrary.googleapis.com/v1/uploads'
    token = pickle.load(open('secrets/token_photoslibrary_v1.pickle', 'rb'))

    # headers for bytes upload
    headers = {
        'Authorization': 'Bearer ' + token.token,
        'Content-type': 'application/octet-stream',
        'X-Goog-Upload-Protocol': 'raw',
        'X-Goog-Upload-File-Name': photo_name
    }

    # open image bytes and upload
    img = open(image_file, 'rb').read()
    response = requests.post(upload_url, data=img, headers=headers)

    # headers for mediaItem upload
    request_body = {
        'newMediaItems': [
            {
                'description': description,
                'simpleMediaItem': {
                    'uploadToken': response.content.decode('utf-8')
                }
            }
        ]
    }

    # upload
    upload_response = service.mediaItems().batchCreate(body=request_body).execute()

# Create Drive service
# service = create_drive_service()

# Google Photos API config and service
API_NAME = 'photoslibrary'
API_VERSION = 'v1'
CLIENT_SECRET_FILE = 'secrets/client_secret_google_photos.json'
SCOPES = ['https://www.googleapis.com/auth/photoslibrary',
          'https://www.googleapis.com/auth/photoslibrary.sharing']
service = create_photos_service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

# Face detection inputs
cascFolder = 'env/lib/python3.8/site-packages/cv2/data'
cascPath = cascFolder + '/haarcascade_frontalface_default.xml'
faceCascade = cv2.CascadeClassifier(cascPath)

# Instantiate Slack client
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
slack_client = WebClient(slack_secret, ssl=ssl_context)

# Get user dict for replacing usernames
member_list = pd.DataFrame(slack_client.users_list()['members'])
member_list = member_list.drop('profile', axis=1).join(pd.DataFrame(member_list.profile.values.tolist()), rsuffix='_profile')
member_list['pax'] = member_list['display_name']
member_list.loc[member_list['display_name']=='', 'pax'] = member_list['real_name']
member_list['pax'] = '@' + member_list['pax'].replace(' ','_', regex=True)
member_list['pax_id'] = '<@' + member_list['id'] + '>'
member_list.set_index('pax_id', inplace=True)
member_dict = member_list['pax'].to_dict()

# Create a folder on Drive for this month
# try:
#     file_metadata = {
#         'name': str(eval_year) + str(eval_month).rjust(2, '0'),
#         'mimeType': 'application/vnd.google-apps.folder',
#         'parents': [DRIVE_FOLDER_ID]
#     }

#     file = service.files().create(body=file_metadata, fields='id').execute()
#     month_folder_id = file.get('id')
# except HttpError as e:
#     print(f'An error occurred while creating the Drive folder: {e}')

# Cycle through channels, join if needed
channels = slack_client.conversations_list()
channels_list = channels['channels']

for channel in channels_list:
    if (channel['is_member'] == False) & (channel['is_archived'] == False):
        slack_client.conversations_join(channel=channel['id'])
        
# for channel in [channels_list[4]]:
for channel in channels_list:
    if channel['is_member'] == True:
        
        print(f'running {channel["name"]}...')
        # Create a folder on Drive for this channel
        # try:
        #     file_metadata = {
        #         'name': channel['name'],
        #         'mimeType': 'application/vnd.google-apps.folder',
        #         'parents': [month_folder_id]
        #     }

        #     file = service.files().create(body=file_metadata, fields='id').execute()
        #     folder_id = file.get('id')
        # except HttpError as e:
        #     print(f'An error occurred while creating the Drive folder: {e}')
            
        # Gather all posts
        posts = slack_client.conversations_history(channel=channel['id'], oldest=start, latest=end, limit=200)
        all_messages = posts['messages']
        
        # Paginate if necessary
        if posts['has_more']:
            posts = slack_client.conversations_history(channel=channel['id'], oldest=start, latest=end, limit=200, cursor=posts['response_metadata']['next_cursor'])
            all_messages += posts['messages']
            
        # Expand files in message list (for messages with multiple files)
        messages = pd.DataFrame(all_messages)
        messages2 = pd.DataFrame(columns=['ts', 'user', 'text', 'file_id', 'filetype', 'mimetype', 'file_url', 'file_is_photo'])
        
        row_idx = 0
        for index, row in messages.iterrows():
            if (row.get('files') is not np.nan) and (row.get('files') is not None):
                for file in row['files']:
                    file_is_photo = file.get('original_w') is not None
                    messages2.loc[row_idx] = [row.get('ts'), row.get('user'), row.get('text'), file.get('id'), file.get('filetype'), file.get('mimetype'), file.get('url_private'), file_is_photo]
                    row_idx += 1
            else:
                messages2.loc[row_idx] = [row.get('ts'), row.get('user'), row.get('text'), None, None, None, None, False]
                row_idx += 1
            
        # Select messages with desired file types
        selected_messages = messages2.loc[messages2['file_is_photo'],:].copy()
        
        # Format user, date, and text fields (will go into file description)
        if len(selected_messages) > 0:
            selected_messages['text'].replace(to_replace=member_dict, regex=True, inplace=True)
            selected_messages['ts_dt'] = pd.to_datetime(selected_messages['ts'].astype(float), unit='s')
            selected_messages.loc[:,'date'] = selected_messages['ts_dt'].dt.date
            selected_messages = pd.merge(selected_messages, member_list[['id', 'pax']], how='left', left_on='user', right_on='id')

            # Pull file data
            for index, file in selected_messages.iterrows():
                print(f"processing {file['file_id']}")
                try:
                    r = requests.get(file['file_url'], headers={'Authorization': f'Bearer {slack_secret}'})
                
                    img_bytes = bytearray(r.content)
                    
                    md5_hash = hashlib.md5()
                    md5_hash.update(img_bytes)
                    checksum = md5_hash.hexdigest()
                    
                    if checksum not in checksum_set:
                        
                        pillow_img = PillowImage.open(io.BytesIO(img_bytes))
                        
                        filetype = file['filetype']
                        img_path = f"cache/{file['file_id']}.{file['filetype']}"
                        
                        exif_date = file['date'].strftime('%Y:%m:%d 06:30:00').encode('UTF-8')
                        exif_dict = {'Exif':{}}
                        exif_dict['Exif'][36867] = exif_date
                        exif_dict['Exif'][36868] = exif_date
                        exif_dict['Exif'][36880] = b'-06:00'
                        
                        # Convert pngs to JPG
                        if file['filetype'].lower() == 'png':
                            pillow_img.convert('RGB')
                            pillow_img.mode = 'RGB'
                            img_path = img_path[:-3] + 'jpg'
                            filetype = 'jpg'
                            
                        # Add GPS EXIF if from an AO
                        if channel['name'] in gps_dict.keys():
                            exif_dict['GPS'] = gps_dict[channel['name']]
                            
                        # Detect faces
                        img_cv = cv2.imdecode(np.asarray(img_bytes, dtype="uint8"), 0)
                        faces = faceCascade.detectMultiScale(img_cv, scaleFactor=1.1, minNeighbors=10, flags=cv2.CASCADE_SCALE_IMAGE)
                        
                        # Only upload if faces are detected (still save down locally either way)
                        if len(faces) == 0:
                            print('no faces detected')
                            pillow_img.save(f'cache/not_uploaded/{file["file_id"]}.{filetype}', exif=piexif.dump(exif_dict))
                        else:
                            pillow_img.save(img_path, exif=piexif.dump(exif_dict))
                            
                            # Upload to Photos
                            upload_to_google_photos(photo_name=f"{file['file_id']}.{filetype}",
                                                    description=f'Posted by: {file["pax"]}\nPosted on: {file["date"]}\nPosted in:{channel["name"]}\n\n{file["text"]}'[:1000])
                    else:
                        print('duplicate detected, skipping')
                except Exception as e:
                    print(f'hit issue - Error: {e}')
                # # Upload to Drive
                # try:
                #     file_metadata = {
                #         'name': file['file_id'],
                #         'description': f'Posted by: {file["pax"]}\nPosted on: {file["date"]}\n\n{file["text"]}',
                #         # 'spaces': ['photos']
                #         'parents': [folder_id]
                #     }
                #     media = MediaFileUpload(f'cache/{file["file_id"]}.{file["filetype"]}',
                #                             mimetype=file['mimetype'])
                #     file = service.files().create(body=file_metadata,
                #                                         media_body=media,
                #                                         fields='id').execute()
                #     file_id = file['id']
                #     request_body = {
                #         'role': 'reader',
                #         'type': 'anyone'
                #     }
                #     response_permission = service.permissions().create(fileId=file_id, body=request_body).execute()
                # except HttpError as e:
                #     print(f'An error occurred while uploading {file["file_id"]}: {e}')

                
with open('cache/checksum_set.pkl', 'wb') as checksum_pickle:
    pickle.dump(checksum_set, checksum_pickle)
