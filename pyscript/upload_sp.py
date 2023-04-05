from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.client_request_exception import ClientRequestException
from datetime import datetime, timedelta, date
import os
import tempfile

app_settings = {
    'url': 'https://ericsson.sharepoint.com/sites/MNOs/',
    'client_id': '0d719d73-aff0-42b1-9083-8756c0b8eec9',
    'client_secret': 'VMI+s171CeWdRSw9kMmLvzGPEBlQNJ8uzKsw2AswqV4=',
}

# authenticate with SharePoint
context_auth = AuthenticationContext(url=app_settings['url'])
context_auth.acquire_token_for_app(client_id=app_settings['client_id'], client_secret=app_settings['client_secret'])
ctx = ClientContext(app_settings['url'], context_auth)

# local folder path and SharePoint folder path
local_folder_path = r'Regions'
sharepoint_folder_path = 'Shared Documents/Regions/'


# get the paths, files with their paths and store then into 2 lists.
def get_all_paths(root_folder, rel_path=""):
    """
    Returns a tuple of two lists: one with all folder paths under the given root folder,
    and another with all file paths under the given root folder including their file names.
    """
    folder_paths = []
    file_paths = []
    for item in os.listdir(os.path.join(root_folder, rel_path)):
        item_path = os.path.join(rel_path, item)
        full_path = os.path.join(root_folder, item_path)
        if os.path.isdir(full_path):
            folder_paths.append(item_path)
            subfolder_paths, subfile_paths = get_all_paths(root_folder, item_path)
            folder_paths.extend(subfolder_paths)
            file_paths.extend(subfile_paths)
        else:
            file_paths.append(item_path)
    return folder_paths, file_paths


## chccking if folder exists func
def try_get_folder(url):
    try:
        return ctx.web.get_folder_by_server_relative_url(url).get().execute_query()
    except ClientRequestException as e:
        if e.response.status_code == 404:
            return None
        else:
            raise ValueError(e.response.text)


# creating folder if they are not exists in sharepoint.
###################################################################
folder_paths, files_with_paths = get_all_paths(local_folder_path)
for path in folder_paths:
    sharepoint_path = sharepoint_folder_path + path.replace('\\', '/')  # normalize the paths in win.

    folder = try_get_folder(
        sharepoint_path)  # try to check if folder created, if not, will loop through and create folders in SP
    if folder is None:
        print('folder not found, will create one for you')
        target_folder = ctx.web.ensure_folder_path(sharepoint_path).execute_query()
        print(f'folder: {target_folder.serverRelativeUrl} is created for you')
    else:
        print(f'Folder: {folder.name}. Exists in SharePoint')


#####################################################################
# check if file exists, Dont upload
def try_get_file(web, url):
    """
    :type web: office365.sharepoint.webs.web.Web
    :type url: str
    """
    try:
        return web.get_file_by_server_relative_url(url).get().execute_query()
    except ClientRequestException as e:
        if e.response.status_code == 404:
            return None
        else:
            raise ValueError(e.response.text)


#########################################################
# upload files 'big size'
def print_upload_progress(offset):
    file_size = os.path.getsize(local_path)
    print("Uploaded '{0}' bytes from '{1}'...[{2}%]".format(offset, file_size, round(offset / file_size * 100, 2)))


for file in files_with_paths:
    # Store file name
    filename = file

    target_url = sharepoint_folder_path

    path = os.path.dirname(file)
    target_url = target_url + path.replace('\\', '/')
    # print(target_url)
    target_folder = ctx.web.get_folder_by_server_relative_url(target_url)
    #####
    file_url = '/sites/MNOs/Shared Documents/Regions/' + filename.replace('\\', '/') + '/'
    file = try_get_file(ctx.web, file_url)
    # print(file)
    if file is None:
        print(f"File not found <{file_url}> we will upload it now")
        size_chunk = 1000000
        file = local_folder_path + '\\' + filename
        with open(file, 'rb') as f:
            uploaded_file = target_folder.files.create_upload_session(file, size_chunk,
                                                                      print_upload_progress).execute_query()
            print('File {0} has been uploaded successfully'.format(uploaded_file.serverRelativeUrl))
    else:
        print(f'File: <{file.name}> Exists in Sharepoint')
    ###########
