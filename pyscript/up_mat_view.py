import json
import requests
from pyscript.db.main import postgres_db


def upload_mat_view(schema='daily_stats'):
    db = postgres_db(schema, 'dnb')
    view_names = db.query(
        'SELECT matviewname FROM pg_matviews WHERE schemaname = %s', (schema,), return_dict=True)
    for view in view_names:
        view_name = view['matviewname']
        mat_result = db.query_df(
            f'SELECT * FROM {schema}.{view_name}')
        # convert to json
        mat_result = mat_result.to_json(orient='records')


        url = 'https://api.eprojecttrackers.com/node/dnb-hawk/v1/refresh-mat-view'
        data = {
            'viewName': view_name,
            'schema': schema,
            'data': mat_result,
        }
        # send post request with timeout argument
        r = requests.post(url=url, json=data, timeout=10)
        print(r.text)


if __name__ == '__main__':
    upload_mat_view()
