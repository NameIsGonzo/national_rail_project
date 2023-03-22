import logging

logging.basicConfig(level=logging.INFO)


# Keys to access: ['RTPPMData']['NationalPage']['NationalPPM']
def flatten_national_page_ppm(
    nested_dict: dict, parent_key="", sep="_"
) -> dict:
    """Recursively flattens a nested dictionary
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
        new_key: str = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            # Recursively call the function to flatten nested dictionaries
            items.extend(
                flatten_national_page_ppm(value, new_key, sep=sep).items()
            )
        else:
            # Append the flattened key-value pair as a tuple with two items
            items.append((new_key, value))

    # Convert the list of tuples to a dictionary and return it
    return dict(items)


# ['RTPPMData']['NationalPage']['Sector']
def flatten_national_page_sector(nested_dict: list, timestamp) -> list[dict]:
    """Iterates over a list of nested dictionaries and flattens them into a dict of dicts

    example: dict =  "Sector": [
                            {
                                "SectorPPM": {
                                    "Total": "8880",
                                    "OnTime": "8202",
                                    "Late": "678",
                                    "CancelVeryLate": "228",
                                    "PPM": {
                                        "text": "92",
                                        "rag": "G"
                                    },
                                    "RollingPPM": {
                                        "text": "84",
                                        "rag": "R",
                                        "trendInd": "-"
                                    }
                                },
                                "sectorCode": "LSE",
                                "sectorDesc": "London and South East"
                            }]
    """
    record_list: list = []
    # Traverse over the dicts inside the list
    for sector in nested_dict:
        # Create dict's from sector code and insert name
        new_data: dict = {
            "sectorCode": sector["sectorCode"],
            "sectorName": sector["sectorDesc"],
            'timestamp': timestamp,
        }
        # Traverse over the nested dict's inside SectorPPM
        for key, value in sector["SectorPPM"].items():
            # Explode the nested dicts and add their keys as part of the new key names
            if key == "PPM":
                new_data["PPM_text"] = value["text"]
                new_data["PPM_rag"] = value["rag"]
            elif key == "RollingPPM":
                new_data["RollingPPM_text"] = value["text"]
                new_data["RollingPPM_rag"] = value["rag"]
                new_data["RollingPPM_trendInd"] = value["trendInd"]
            else:
                # Insert data that isn't nested
                new_data[key] = value
        record_list.append(new_data)
    return record_list


# ['RTPPMData']['NationalPage']['Operator']
def flatten_national_page_operators(nested_dict: list, timestamp: str) -> list[dict]:
    """Iterates over a list of nested dictionaries and flattens them into a dict of dicts.
    example: dict = "Operator": [
                        {
                            "Total": "6",
                            "PPM": {
                                "text": "100",
                                "rag": "G"
                            },
                            "RollingPPM": {
                                "text": "-1",
                                "rag": "W",
                                "displayFlag": "Y"
                            },
                            "code": "35",
                            "name": "Caledonian Sleeper",
                            "keySymbol": "*"
                        }]
    """
    records_list: list = []
    for record in nested_dict:
        # Create a dictionary to store the flattened record
        flat_record = {'timestamp': timestamp}
        # Add all key-value pairs to flat_record
        for key, value in record.items():
            # Add PPM_text and PPM_rag keys for PPM dictionary
            if key == "code":
                flat_record["operatorCode"] = value
            elif key == "PPM":
                flat_record["PPM_text"] = value["text"]
                flat_record["PPM_rag"] = value["rag"]
            # Add RollingPPM_text, RollingPPM_rag, and RollingPPM_trendInd keys for RollingPPM dictionary
            elif key == "RollingPPM":
                flat_record["RollingPPM_text"] = value["text"]
                flat_record["RollingPPM_rag"] = value["rag"]
                flat_record["RollingPPM_trendInd"] = value.get("trendInd")
            elif key != "keySymbol":
                flat_record[key] = value
        # Add the flattened record to the records list
        records_list.append(flat_record)

    return records_list


# ['RTPPMData']['OOCPage']['Operator']
def flatten_out_of_course_page(nested_dict: list, timestamp: str) -> dict:
    """Since both dicts share the same structure we can call
    our function flatten_national_page_operators"""
    return flatten_national_page_operators(nested_dict, timestamp)


# ['RTPPMData']['FOCPage']['NationalPPM']
def flatten_fooc_page_ppm(nested_dict: list) -> dict:
    """Since both dicts share the same structure we can call
    our function flatten_national_page_ppm"""
    return flatten_national_page_ppm(nested_dict)


# ['RTPPMData']['FOCPage']['Operator']
def flatten_fooc_page_operators(nested_dict: list, timestamp: str) -> dict:
    """Since both dicts share the same structure we can call
    our function flatten_national_page_operators"""
    return flatten_national_page_operators(nested_dict, timestamp)


# ['RTPPMData']['OperatorPage']
def flatten_operators_page(nested_dicts: list, timestamp: str) -> dict:
    """
    Iterates over a list of nested dictionaries and flattens them
    into a single dict. Returns a dict of all our nested dicts

    example: dict = [{
                    "Operator": {
                        "Total": "20",
                        "OnTime": "14",
                        "Late": "6",
                        "CancelVeryLate": "0",
                        "PPM": {
                            "text": "70",
                            "rag": "R"
                        },
                        "RollingPPM": {
                            "text": "66",
                            "rag": "R",
                            "displayFlag": "Y",
                            "trendInd": "-"
                        },
                        "code": "97",
                        "name": "Direct Rail Services"
                    },
                    "OprToleranceTotal": {
                        "Total": "20",
                        "OnTime": "14",
                        "Late": "6",
                        "CancelVeryLate": "0",
                        "timeband": "15"
                    }
                }]
    """
    records_list: list = []
    for operator in nested_dicts:
        # Create a new dictionary to hold the flattened data for this operator
        flat_dict = {'timestamp': timestamp}

        # Extract data from the operator dictionary and flatten it
        op_data = operator["Operator"]
        flat_dict["sectorCode"] = op_data["code"]
        flat_dict["sectorName"] = op_data["name"]
        flat_dict["total"] = op_data["Total"]
        flat_dict["onTime"] = op_data["OnTime"]
        flat_dict["late"] = op_data["Late"]
        flat_dict["cancelVeryLate"] = op_data["CancelVeryLate"]
        flat_dict["PPM_text"] = op_data["PPM"]["text"]
        flat_dict["PPM_rag"] = op_data["PPM"]["rag"]
        flat_dict["RollingPPM_text"] = op_data["RollingPPM"]["text"]
        flat_dict["RollingPPM_rag"] = op_data["RollingPPM"]["rag"]
        flat_dict["RollingPPM_trendInd"] = op_data["RollingPPM"].get("trendInd")

        # Add the flattened operator data to the new_data dictionary using the operator code as the key
        records_list.append(flat_dict)
    return records_list


# ['RTPPMData']['OperatorPage']
def flatten_operators_page_groups(nested_dicts: list, timestamp: str) -> list[dict]:
    """
    Iterates over a list of nested dicts and retrieves only the service groups if exist
    After that it flattens the nested dicts inside the service groups and
    returns a list with each dict as a record

    example: dict = ["OprServiceGrp": [
                        {
                            "Total": "86",
                            "OnTime": "73",
                            "Late": "13",
                            "CancelVeryLate": "5",
                            "PPM": {
                                "text": "84",
                                "rag": "R"
                            },
                            "RollingPPM": {
                                "text": "82",
                                "rag": "R",
                                "displayFlag": "Y",
                                "trendInd": "-"
                            },
                            "name": "CrossCountry Inter City",
                            "timeband": "10",
                            "sectorCode": "LD"
                        },
                        {
                            "Total": "119",
                            "OnTime": "89",
                            "Late": "30",
                            "CancelVeryLate": "24",
                            "PPM": {
                                "text": "74",
                                "rag": "R"
                            },
                            "RollingPPM": {
                                "text": "77",
                                "rag": "R",
                                "displayFlag": "Y",
                                "trendInd": "+"
                            },
                            "name": "CrossCountry Local & Provincial",
                            "timeband": "10",
                            "sectorCode": "LD"
                        }
                    ]]
    """
    nested_groups: list = []
    service_groups: list = []

    # Iterate over the nested dicts and retrieve only the nested service groups if exist
    for operator_groups in nested_dicts:
        try:
            nested_groups.extend(operator_groups["OprServiceGrp"])
        except KeyError as e:
            pass

    # Theres a bug in my code which returns the nested_groups with non-dict elements.
    nested_groups: list = [
        record for record in nested_groups if isinstance(record, dict)
    ]

    #
    for service_group in nested_groups:
        # Create a single record for each nested service group
        record: dict = {'timestamp': timestamp}

        try:
            record["name"] = service_group["name"]
            record["Total"] = service_group["Total"]
            record["OnTime"] = service_group["OnTime"]
            record["Late"] = service_group["Late"]
            record["CancelVeryLate"] = service_group["CancelVeryLate"]
            record["PPM_text"] = service_group.get("PPM", {}).get("text", "")
            record["PPM_rag"] = service_group.get("PPM", {}).get("rag", "")
            record["RollingPPM_text"] = service_group.get("RollingPPM", {}).get(
                "text", ""
            )
            record["RollingPPM_rag"] = service_group.get("RollingPPM", {}).get(
                "rag", ""
            )
            record["RollingPPM_trendInd"] = service_group.get("RollingPPM", {}).get(
                "trendInd", ""
            )
            service_groups.append(record)
        except KeyError as e:
            logging.info(e)

    return service_groups
