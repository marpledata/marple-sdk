from requests import Response


def validate_response(response: Response, failure_message: str, check_status: bool = True) -> dict:
    if response.status_code == 400 or response.status_code == 500:
        raise Exception(f"{failure_message}: {response.json().get('error', 'Unknown error')}")
    if response.status_code != 200:
        response.raise_for_status()
    r_json = response.json()
    if check_status and r_json["status"] != "success":
        raise Exception(failure_message)
    return r_json
