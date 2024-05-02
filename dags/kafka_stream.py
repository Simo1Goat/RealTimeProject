import json
import requests
from requests.exceptions import HTTPError
from config import API_URL


def get_data(method="GET", retrial=0):
    response = None
    if retrial >= 4:
        print("Reached max retrial finishing the process ...")
        return response
    try:
        params = {
            "page": 1,
            "results": 1
        }
        response = requests.request(method=method, url=API_URL, params=params)
        response.raise_for_status()
    except HTTPError as e:
        if response.status_code == 405:
            print("Method is not allowed, Retry different HTTP method ...")
            retrial += 1
            get_data(method="POST", retrial=retrial)
        else:
            print("Http Error occurred, details {}".format(e))
    except Exception as e:
        print("An Error occurred, details {}".format(e))
    else:
        # handle successful response
        return response.json()["results"][0]


def stream_data():
    response = get_data(method="GET")
    formatted_data = format_data(response)
    return json.dumps(formatted_data, indent=3)


def format_data(res):
    location = res["location"]
    data = {
        "first_name": res["name"]["first"],
        "last_name": res["name"]["last"],
        "gender": res["gender"],
        "address": f'{location["street"]["number"]} {location["street"]["name"]}, '
                   f'{location["city"]}, {location["state"]}, {location["country"]}',
        "postcode": location["postcode"],
        "email": res["email"],
        "username": res["login"]["username"],
        "dob": res["dob"]["date"],
        "registered_data": res["registered"]["date"],
        "phone": res["phone"],
        "picture": res["picture"]["medium"]
    }
    return data
