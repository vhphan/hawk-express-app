import os
import sys
from dotenv import load_dotenv
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext

load_dotenv('/data2/var/www/hawk-express-app/.env')

app_settings = {
    'url': os.getenv('SP_SITE_URL'),
    'client_id': os.getenv('SP_CLIENT_ID'),
    'client_secret': os.getenv('SP_CLIENT_SECRET'),
}

context_auth = AuthenticationContext(url=app_settings['url'])
context_auth.acquire_token_for_app(client_id=app_settings['client_id'], client_secret=app_settings['client_secret'])
ctx = ClientContext(app_settings['url'], context_auth)


def main():

    size_chunk = 1_000_000
    filename_to_upload = sys.argv[1]
    file_to_upload = os.path.join('/data2/var/www/hawk-express-app/tmp/dl', filename_to_upload)

    if not os.path.exists(file_to_upload):
        print("File '{0}' not found".format(file_to_upload))
        sys.stdout.flush()
        return

    sharepoint_folder_path = 'Shared Documents/CTR/'
    target_folder = ctx.web.get_folder_by_server_relative_url(sharepoint_folder_path)

    def print_upload_progress(offset):
        file_size = os.path.getsize(file_to_upload)
        print("Uploaded '{0}' bytes from '{1}'...[{2}%]".format(offset, file_size, round(offset / file_size * 100, 2)))
        sys.stdout.flush()

    with open(file_to_upload, 'rb') as f:
        uploaded_file = target_folder.files.create_upload_session(file_to_upload,
                                                                  size_chunk,
                                                                  print_upload_progress).execute_query()
        print('File {0} has been uploaded successfully'.format(uploaded_file.serverRelativeUrl))


if __name__ == '__main__':
    main()