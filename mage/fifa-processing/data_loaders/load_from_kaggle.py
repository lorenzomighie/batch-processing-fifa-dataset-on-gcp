import io
import pandas as pd
import requests
import kaggle

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    # kaggle.api.authenticate()  # file under /home/src/.kaggle/kaggle.json

    # datasets = kaggle.api.dataset_list(search="fifa")
    # print(datasets)
    #kaggle.api.dataset_download_files('stefanoleone992/fifa-22-complete-player-dataset', path='dataset', unzip=True)
    
    fifa_dtypes = {
        'sofifa_id': pd.Int64Dtype(),
        'player_url': str,
        'short_name': str,
        'age': pd.Int64Dtype(),
        #'dob'
        'height_cm': pd.Int64Dtype(),
        'weight_kg': pd.Int64Dtype(),
        'nationality': str,
        'club_name': str,
        'league_name': str,
        'league_rank': pd.Int64Dtype(),
        'overall': pd.Int64Dtype(),
        'potential': pd.Int64Dtype(),
        'value_eur': pd.Int64Dtype(),
        'wage_eur': pd.Int64Dtype(),
        'player_positions': str
    }
    date_col = ['dob']

    years = range(15,23)
    file_path = 'dataset/players_'

    dataframes = []
    df_complete = pd.DataFrame()

    for y in years:

        df_y = pd.read_csv(file_path + str(y) + '.csv', dtype=fifa_dtypes, parse_dates=date_col)
        df_y['version'] = y
        dataframes.append(df_y)


    df_complete = pd.concat(dataframes)
    #url = ''
    #response = requests.get(url)
    return df_complete
    #return pd.read_csv(io.StringIO(response.text), sep=',')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
