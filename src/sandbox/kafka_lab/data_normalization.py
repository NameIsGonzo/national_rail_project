import logging
import sys
import json 

logging.basicConfig(level=logging.INFO)
testing_file = r'/Users/gonzo/Desktop/RailScope/national_rail_project/src/sandbox/stomp/rtppm/rtppm_data.json'
with open(testing_file) as f:
    data = json.load(f)


def flatten_national_page(nested_dict: dict, parent_key='', sep='_') -> dict:
    """ Recursively flattens a nested dictionary 
    with the following structure:
    
     example: dict =    "NationalPPM": {
                            "Total": "16652",
                            "OnTime": "15235",
                            "Late": "1417",
                            "CancelVeryLate": "499",
                            "PPM": {
                                "text": "91",
                                "rag": "A",
                                "ragDisplayFlag": "Y"
                            },
                            "RollingPPM": {
                                "text": "85",
                                "rag": "R",
                                "trendInd": "-"
                            }
                        },
    """
    items: list = []

    for key, value in nested_dict.items():
        new_key: str = f'{parent_key}{sep}{key}' if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_national_page(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)

