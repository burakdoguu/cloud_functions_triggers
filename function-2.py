import functions_framework
import pandas as pd
from google.cloud import storage
import datetime
import base64
import json

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    #if request_json and 'name' in request_json:
    #    name = request_json['name']
    #elif request_args and 'name' in request_args:
    #    name = request_args['name']
    #else:
    #    name = 'World'
    #return 'Hello {}!'.format(name)

    client = storage.Client()
    bucket_name = 'retail_test_data'
    blob_name_views = 'views_data-6-2-2024.txt'
    blob_name_category = 'product-category-map.csv'

    bucket = client.bucket(bucket_name)
    blob_views = bucket.blob(blob_name_views)
    blob_category = bucket.blob(blob_name_category)

    # Verileri oku ve DataFrame oluştur
    df_views = pd.read_csv('gs://' + bucket_name + '/' + blob_name_views, header=None)
    df_category = pd.read_csv('gs://' + bucket_name + '/' + blob_name_category)

    dt = datetime.datetime.now()
    output_data = []

    # Veri işleme
    for index, row in df_views.iterrows():
        value = row.values[0]
        decoded_data = base64.b64decode(value)
        result = json.loads(decoded_data.decode('utf-8'))  # JSON string'e çevir
        dict_result = json.loads(result)  # JSON string'i dict'e çevir

        event = dict_result['event']
        messageid = dict_result['messageid']
        userid = dict_result['userid']
        productid = dict_result['properties']['productid']
        source = dict_result['context']['source']

        categoryid = df_category[df_category['productid'] == productid]['categoryid'].values[0]

        joined_output = {
            "event": event,
            "messageid": messageid,
            "userid": userid,
            "productid": productid,
            "source": source,
            "categoryid": categoryid
        }
        output_data.append(joined_output)

    result_df = pd.DataFrame(output_data)

    # Sonuçları Cloud Storage'a kaydet
    object_name = f'output_views_joined_{dt.day}-{dt.month}-{dt.year}.csv'
    save_dataframe_to_gcs(result_df, bucket_name, object_name)

    return 'Data successfully processed and saved.'


def save_dataframe_to_gcs(dataframe, bucket_name, object_name, content_type='text/csv'):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    csv_data = dataframe.to_csv(index=False)
    blob.upload_from_string(csv_data, content_type=content_type)
