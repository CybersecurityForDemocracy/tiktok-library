from pathlib import Path
from unittest.mock import Mock

import pytest

from . import api_client

MOCK_VIDEO_DATA = [
    {
        "music_id": 6810180973628491777,
        "playlist_id": 0,
        "region_code": "US",
        "share_count": 50,
        "username": "american_ginger_redeemed",
        "hashtag_names": [
            "whatdidyouexpect",
            "viral",
            "foryou",
            "fyp",
            "prolife",
            "greenscreensticker",
            "unbornlivesmatter",
        ],
        "id": 7094381613995478318,
        "like_count": 2135,
        "view_count": 20777,
        "video_description": "Pregnancy is a natural outcome to unprotected s*x… what did you think was gonna happen? #fyp #foryou #unbornlivesmatter #viral #prolife #whatdidyouexpect #greenscreensticker",
        "comment_count": 501,
        "create_time": 1651789438,
        "effect_ids": ["0"],
    },
    {
        "video_description": "Period. #abortionismurder #fyp #prolife #LaurelRoad4Nurses #BBPlayDate #roemustgo",
        "create_time": 1651766906,
        "effect_ids": ["0"],
        "id": 7094284837128817962,
        "region_code": "US",
        "share_count": 5,
        "view_count": 5400,
        "comment_count": 72,
        "hashtag_names": [
            "fyp",
            "prolife",
            "abortionismurder",
            "LaurelRoad4Nurses",
            "BBPlayDate",
            "roemustgo",
        ],
        "like_count": 499,
        "music_id": 6865506085311088641,
        "username": "realmorganfaith",
    },
    {
        "like_count": 760,
        "music_id": 6833934234948732941,
        "username": "edenmccourt",
        "video_description": "I don’t usually talk about myself on my public pages, but I think given the current climate it is necessary. I want to help you understand that people on both sides of this debate are just normal people with normal interests and who should be treated with respect, dignity and kindness. We can disagree and still be friends. Less polarisation and more conversation. ❤️ #foryourpage #humanlikeyou",
        "view_count": 19365,
        "comment_count": 373,
        "effect_ids": ["0"],
        "id": 7094037673978973446,
        "region_code": "GB",
        "share_count": 30,
        "create_time": 1651709360,
        "hashtag_names": ["humanlikeyou", "foryourpage"],
    },
    {
        "comment_count": 402,
        "create_time": 1651614306,
        "id": 7093629419205561606,
        "like_count": 923,
        "region_code": "GB",
        "username": "edenmccourt",
        "video_description": "It do be like that tho. #fyp #roevwade #abortion",
        "view_count": 13809,
        "effect_ids": ["0"],
        "hashtag_names": ["abortion", "fyp", "roevwade"],
        "music_id": 7016913596630207238,
        "share_count": 16,
    },
]

FAKE_SECRETS_YAML_FILE = Path('src/tiktok_api_helper/testdata/fake_secrets.yaml')

@pytest.fixture
def mock_request_session():
    return Mock()

def test_tiktok_credentials_any_value_missing_raises_value_error():
    with pytest.raises(ValueError):
            api_client.TiktokCredentials(None, None, None)

    with pytest.raises(ValueError):
            api_client.TiktokCredentials('client_id_1', 'client_secret_1', None)

    with pytest.raises(ValueError):
            api_client.TiktokCredentials('client_id_1', None, 'client_key_1')

    with pytest.raises(ValueError):
            api_client.TiktokCredentials(None, 'client_secret_1', 'client_key_1')

    with pytest.raises(ValueError):
            api_client.TiktokCredentials('', '', '')


def test_tiktok_api_request_client_empty_credentials_raises_value_error(mock_request_session):
    with pytest.raises(ValueError):
        api_client.TikTokApiRequestClient(
            credentials=None)


def test_tiktok_api_request_client_from_credentials_file_factory(mock_request_session):
    request = api_client.TikTokApiRequestClient.from_credentials_file(FAKE_SECRETS_YAML_FILE,
                                                                      session=mock_request_session)
    assert request._credentials == api_client.TiktokCredentials(client_id='client_id_1',
                                                                client_secret='client_secret_1',
                                                                client_key='client_key_1')
