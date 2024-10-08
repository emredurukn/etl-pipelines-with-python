from mixpanel_utils import MixpanelUtils
from google.cloud import storage
from google.oauth2 import service_account
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta


service_account_path = 'SERVICE_ACCOUNT_FILE_PATH'
project_name = 'GCP_PROJECT_NAME'
bucket_name = 'GCS_BUCKET_NAME'
mixpanel_secret = 'MIXPANEL_SECRET'
bucket_path = 'BUCKET_PATH'

credentials = service_account.Credentials.from_service_account_file(service_account_path)
storage_client = storage.Client(credentials=credentials, project=project_name)
bucket = storage_client.get_bucket(bucket_name)


def get_data_list(start_date_str, end_date_str):
    date_list = []
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    while start_date <= end_date:
        date_list.append(start_date_str)
    
        start_date += relativedelta(days=1)
        start_date_str = start_date.strftime('%Y-%m-%d')

    return date_list


def export_mixpanel_all_data(selected_date, bucket, bucket_path, mixpanel_secret):
    gcs_dir_name = selected_date.replace("-", "")
    file_name = "mixpanel_all_events_{}.csv".format(gcs_dir_name)
    downloaded_file_name = file_name + '.gz'

    mputils = MixpanelUtils(mixpanel_secret)
    
    mputils.export_events(
        output_file=file_name,
        add_gzip_header=True,
        compress=True,
        format="csv",
        raw_stream=True,
        params={
            "from_date": selected_date,
            "to_date": selected_date
        }
    )

    destination_blob_name = '{}/{}/{}'.format(bucket_path, gcs_dir_name, downloaded_file_name)

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(downloaded_file_name, timeout=1800)


if __name__ == "__main__":
    
    start_date_str = "2024-01-01"
    end_date_str = "2024-01-02"

    date_list = get_data_list(start_date_str, end_date_str)

    for selected_date in date_list:
        start_time = time.time()

        export_mixpanel_all_data(selected_date, bucket, bucket_path, mixpanel_secret)
        
        end_time = time.time()
        
        print("selected_date: {}, process_time: {}".format(selected_date, (end_time - start_time)))
        print("-------------------------------")
