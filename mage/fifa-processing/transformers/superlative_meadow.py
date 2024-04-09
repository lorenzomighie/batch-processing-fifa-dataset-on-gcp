if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    columns_to_keep = [
        'sofifa_id',
        'player_url',
        'short_name',
        'age',
        'dob',
        'height_cm',
        'weight_kg',
        'nationality_name',
        'club_name',
        'league_name',
        'league_level',
        'overall',
        'potential',
        'value_eur',
        'wage_eur',
        'player_positions',
        'version'
    ]
    print(data.columns)

    # filter out not considered fields
    data = data[columns_to_keep]

    # filter something else out, check null values..

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'