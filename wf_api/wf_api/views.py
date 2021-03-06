from django.http import HttpResponse, JsonResponse
from django.views import View
from utils import handle_view_exception
import os
import pandas as pd
from etl_util import get_all_sample_data, filter_data_by_worth, push_df_to_db, filter_df_by_quality
import logging

logger = logging.getLogger(__name__)


@handle_view_exception
def update_csv(request):
    """
    :param request: filename, bonus1 = False
    :return: Success message on file update
    example: request = {filename: 'consolidated_output.1.csv', 'bonus1':'True'}
    """
    if request.method == 'POST':
        root = '..\output'
        df = get_all_sample_data()
        filename = request.get('filename')
        bonus1 = request.get('bonus1')

        if bonus1:
            df_filtered = filter_data_by_worth(df)
            df_filtered.to_csv(os.path.join(root, 'filtered_' + filename), index=False)
            push_df_to_db(df_filtered, table_name='filtered_data', method='pyodbc', fastexecute=True)

        df.to_csv(os.path.join(root, filename), index=False)
        push_df_to_db(df, table_name='unfiltered_data', method='pyodbc', fastexecute=True)

        response = {'msg': 'Submitted Successfully',
                    'updated': True}
        logger.info(response)

        return JsonResponse(response)


@handle_view_exception
def get_quality(request):
    """
    :param request: quality
    :return: products list
    example: request = {'quality': 'high'}
        """
    if request.method == 'GET':
        quality = request.get('quality')

        df = pd.read_csv('output/consolidated_output.1.csv', index_col=None, header=0)
        quality_df = filter_df_by_quality(df)

        result = HttpResponse(quality_df, content_type = 'application/json')
        logger.info("Data returned successfully")

        return result

