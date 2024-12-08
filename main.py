# -*- coding: utf-8 -*-
import os
import pandas as pd
import requests
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
from pathlib import Path

# Load environment variables
load_dotenv()

# Acces information for IoT data
os.environ["no_proxy"] = "*"
CONNXT_BASE_URL = os.getenv("CONNXT_BASE_URL")
CONNXT_URLS = {
    "connect_token": f"{CONNXT_BASE_URL}connect/token",
    "graphql": f"{CONNXT_BASE_URL}graphql",
    # REST
    "get_telemetry": f"{CONNXT_BASE_URL}api/Telemetry",
    "get_devices": f"{CONNXT_BASE_URL}api/Devices",
}
CLIENT_ID = os.getenv("CONNXT_CLIENT_ID")

# Lizard information
USERNAME = "__key__"
PASSWORD = os.getenv("LIZARD_API_KEY")

# UUIDs of timeseries for posting data for linked to deviceId from CoNNXT
POST_DICT_IOT1 = {
    "deviceId": 609,
    "VRms": "b0e7d746-638c-4d8f-b065-087237f7e132",
    "ARms": "278f37a2-bc83-458d-8799-9f14a29bcb03",
    "APeak": "91926892-97b6-4e48-9923-421a428b1859",
    "Temperature": "59253408-eb82-4c3e-8175-d7c840f45003",
}

POST_DICT_IOT2 = {
    "deviceId": 610,
    "VRms": "2395a532-d23b-4d5b-b7a6-e0b07e07703c",
    "ARms": "234af6e6-441a-4954-8c67-1dd4fbec9b39",
    "APeak": "bdbe77bf-d21d-4509-ae80-42bcf9578e44",
    "Temperature": "0c72012c-1fa6-4071-8561-5c0e774901ab",
}

LOD = []
LOD.append(POST_DICT_IOT1)
LOD.append(POST_DICT_IOT2)


# Set up logging
def get_run_logger():
    logger = logging.getLogger("connxt_iot_toLizard")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    log_path = Path(__file__).parent / "logs"
    log_path.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(log_path / "connxt_iot_toLizard.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def get_token():
    """Create an access token. This token is valid for an hour"""
    try:
        payload = f"grant_type=client_credentials&client_id={CLIENT_ID}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        response = requests.request(
            "POST",
            CONNXT_URLS["connect_token"],
            verify=True,
            headers=headers,
            data=payload,
        )
        access_token = response.json().get("access_token", None)
        if not access_token:
            raise (Exception("No access token acquired"))
    except Exception as e:
        raise e
    return access_token


def get_data(access_token, variables):
    """
    Parameters
    ----------
    variables : _type_
        _description_
    query : str, by default None
        Needed for graphql
    """
    data = None

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    start = variables.get("from")
    end = variables.get("to")
    dataPoints = [
        {"storageName": dataPoint} for dataPoint in variables.get("dataPoints", None)
    ]
    payload = {
        "start": start,
        "end": end,
        "timeZone": "UTC",
        "bin": "None",
        "dataPoints": dataPoints,
    }

    id = variables.get("deviceId", None)

    response = requests.request(
        "POST",
        url=CONNXT_URLS["get_telemetry"] + f"/{id}",
        headers=headers,
        data=json.dumps(payload),
    )
    data = response.json()

    if data:
        return data
    else:
        raise Exception(f"no data found for query \n{id} with variables\n{variables}")


def REST_response_to_df(deviceId, data, l_rows):
    """Continue here with converting data like

    {'Temperature': [
        {'t': 1726012800000, 'v': 20.1122},
        {'t': 1726013700000, 'v': 19.9719}
        ],
    'Conductivity': [
        {'t': 1726012800000, 'v': 1083.98},
        {'t': 1726013700000, 'v': 1068.74},
        ]}
    to rows with
    {"deviceId": deviceId, "value", "timestamp"}
    """
    combined_data_dict = {}

    # iterate over each key in the data dictionary
    for key in data:
        # iterate over each entry in the list corresponding to the key
        for entry in data[key]:
            # if the timestamp is not already in the combined_data_dict, add it
            if entry["t"] not in combined_data_dict:
                combined_data_dict[entry["t"]] = {"timestamp": entry["t"]}
            # add the value to the corresponding key in the combined_data_dict
            combined_data_dict[entry["t"]][key] = entry["v"]

    # convert the combined_data_dict to a list
    combined_data = list(combined_data_dict.values())
    for row in combined_data:
        row["deviceId"] = deviceId

    return l_rows + combined_data


def get_Telemetry(deviceId, start, end, access_token, logger):
    l_rows = []
    try:
        # get telemetry data
        variables = {
            "deviceId": int(deviceId),
            "dataPoints": ["Temperature", "VRms", "ARms", "APeak"],
            "from": start,
            "to": end,
        }

        data = get_data(access_token, variables)
        l_rows = REST_response_to_df(deviceId, data, l_rows)

    except Exception as e:
        logger.error(f"Error in get_Telemetry: {e}")
        print(e)

    df = pd.DataFrame(l_rows)
    # convert 'timestamp' column to '%Y-%m-%d %H:%M:%S'
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    # format timestamp "YYYY-MM-DDTHH:MM:SS.MSMSMSMSMSMSZ"
    df["timestamp"] = (
        pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    )

    return df, l_rows


def get_IoT_data(access_token, deviceId, period, delay, logger):
    try:
        # set start/end time
        end_date = datetime.utcnow() - timedelta(days=delay)
        start_date = end_date - timedelta(hours=period)

        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"
        logger.info(f"Fetching data between {start_str} & {end_str}")

        df, l_rows = get_Telemetry(
            deviceId=deviceId,
            start=start_str,
            end=end_str,
            logger=logger,
            access_token=access_token,
        )
        return df
    except Exception as e:
        logger.error(f"Error in get_IoT_data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def post_IoT_data(data, json_headers, ts_uuid):
    url_ts = f"https://nens.lizard.net/api/v4/timeseries/{ts_uuid}/events/"
    try:
        r = requests.post(url=url_ts, data=json.dumps(data), headers=json_headers)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to post data to Lizard: {e}")
        return None
    return r


def Connxt_to_Lizard(period=24, delay=0):
    logger = get_run_logger()
    logger.info("Starting update of IoT trillingsensor data")

    # initialize connxt for HHNK
    access_token = get_token()

    json_headers = {
        "username": USERNAME,
        "password": PASSWORD,
        "Content-Type": "application/json",
    }

    for IoT_sensor in LOD:

        deviceId = IoT_sensor["deviceId"]
        df = get_IoT_data(access_token, deviceId, period, delay, logger)

        # for loop for alle IoT data
        if not df.empty:  # Check if DataFrame is not empty
            for key, value in IoT_sensor.items():
                if key != "deviceId":

                    df_post = df[["timestamp", key]].rename(
                        columns={"timestamp": "time", key: "value"}
                    )
                    df_dict = df_post.to_dict("records")
                    r = post_IoT_data(df_dict, json_headers, value)
                    if r.ok:
                        logger.info(f"{key} timeserie updated in Lizard")
                    else:
                        logger.warning(f"{r.text}")
        else:
            logger.warning(f"no data retrieved for {deviceId}")

    logger.info("End of update of IoT trillingsensor data")


if __name__ == "__main__":
    # loop over Connxt_to_Lizard in range
    MAX_HISTORY = 9
    for i in range(MAX_HISTORY):
        try:
            Connxt_to_Lizard(24, i)
            print(f"Retrieved data for day {i + 1} of {MAX_HISTORY} days")
        except Exception as e:
            print(f"Failed to retrieve data for day {i + 1}: {e}")
