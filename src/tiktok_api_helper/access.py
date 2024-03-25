from __future__ import annotations

import logging

import requests as rq
import yaml
import certifi
from attr import dataclass

# Global so we can use it in the hook
session: rq.Session = rq.Session()
creds = None


@dataclass
class TiktokCredentials:
    client_id: str
    client_secret: str
    client_key: str


def get_client_access_token(
    creds: TiktokCredentials,
    grant_type: str = "client_credentials",
) -> str:

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cache-Control": "no-cache",
    }

    data = {
        "client_key": creds.client_key,
        "client_secret": creds.client_secret,
        "grant_type": grant_type,
    }

    response = rq.post(
        "https://open.tiktokapis.com/v2/oauth/token/", headers=headers, data=data
    )
    if not response.ok:
        logging.error("Problem with access token response: %s", response)

    try:
        access_data = response.json()
    except rq.exceptions.JSONDecodeError as e:
        logging.info('Access token raw response: %s\n%s\n%s', response.status_code, response.headers, response.text)
        raise e
    logging.info(f"Access token response: {access_data}")

    token = access_data["access_token"]

    return token


def get_credentials(filename: str = "./secrets.yaml") -> TiktokCredentials:
    with open(filename, "r") as f:
        dict_creds = yaml.load(f, Loader=yaml.FullLoader)

    creds = TiktokCredentials(
        dict_creds["client_id"], dict_creds["client_secret"], dict_creds["client_key"]
    )

    return creds


def refresh_token(r, *args, **kwargs) -> rq.Response | None:
    # Adapted from https://stackoverflow.com/questions/37094419/python-requests-retry-request-after-re-authentication

    global session, creds
    assert creds is not None, "Credentials have not yet been set"

    if r.status_code == 401:
        logging.info("Fetching new token as the previous token expired")

        token = get_client_access_token(creds)
        session.headers.update({"Authorization": f"Bearer {token}"})

        r.request.headers["Authorization"] = session.headers["Authorization"]

        return session.send(r.request)


def get_session():
    global session, creds

    creds = get_credentials()

    headers = {
        # We add the header here so the first run won't give us a InsecureRequestWarning
        # The token may time out which is why we manually add a hook to it
        "Authorization": f"Bearer {get_client_access_token(creds)}",
        "Content-Type": "text/plain",
    }

    session.headers.update(headers)
    session.hooks["response"].append(refresh_token)
    session.verify = certifi.where()

    return session
