from __future__ import annotations

import logging
from pathlib import Path

import requests as rq
import yaml
import certifi
from attr import dataclass

@dataclass
class TiktokCredentials:
    client_id: str
    client_secret: str
    client_key: str


class TikTokApiRequestClient:

    def __init__(self, credentials_file: Path):
        self._session: rq.Session = rq.Session()
        self._set_credentials(credentials_file)

    def _get_client_access_token(
        self,
        grant_type: str = "client_credentials",
    ) -> str:

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Cache-Control": "no-cache",
        }

        data = {
            "client_key": self._creds.client_key,
            "client_secret": self._creds.client_secret,
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
            logging.info(
                "Access token raw response: %s\n%s\n%s",
                response.status_code,
                response.headers,
                response.text,
            )
            raise e
        logging.info(f"Access token response: {access_data}")

        token = access_data["access_token"]

        return token


    def _set_credentials(self, credentials_file: Path) -> TiktokCredentials:
        with credentials_file.open("r") as f:
            dict_creds = yaml.load(f, Loader=yaml.FullLoader)

        self._creds = TiktokCredentials(
            dict_creds["client_id"], dict_creds["client_secret"], dict_creds["client_key"]
        )

    def _refresh_token(self, r, *args, **kwargs) -> rq.Response | None:
        # Adapted from https://stackoverflow.com/questions/37094419/python-requests-retry-request-after-re-authentication

        assert self._creds is not None, "Credentials have not yet been set"

        if r.status_code == 401:
            logging.info("Fetching new token as the previous token expired")

            token = self._get_client_access_token()
            self._session.headers.update({"Authorization": f"Bearer {token}"})

            r.request.headers["Authorization"] = self._session.headers["Authorization"]

            return self._session.send(r.request)


    def get_session(self):
        headers = {
            # We add the header here so the first run won't give us a InsecureRequestWarning
            # The token may time out which is why we manually add a hook to it
            "Authorization": f"Bearer {self._get_client_access_token()}",
            "Content-Type": "text/plain",
        }

        self._session.headers.update(headers)
        self._session.hooks["response"].append(self._refresh_token)
        self._session.verify = certifi.where()

        return self._session
