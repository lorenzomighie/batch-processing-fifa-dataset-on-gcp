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
    kaggle.api.authenticate()  # file under /home/src/.kaggle/kaggle.json

    datasets = kaggle.api.dataset_list(search="fifa")
    print(datasets)
    kaggle.api.dataset_download_files('stefanoleone992/fifa-22-complete-player-dataset', path='dataset', unzip=True)
    #url = ''
    #response = requests.get(url)
    return 'a'
    #return pd.read_csv(io.StringIO(response.text), sep=',')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    #assert output is not None, 'The output is undefined'
