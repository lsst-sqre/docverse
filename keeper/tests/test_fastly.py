import uuid

import pytest
import responses

from keeper.exceptions import FastlyError
from keeper.fastly import FastlyService


@responses.activate
def test_purge_key() -> None:
    service_id = "SU1Z0isxPaozGVKXdv0eY"
    api_key = "d3cafb4dde4dbeef"
    surrogate_key = uuid.uuid4().hex

    url = "https://api.fastly.com/service/{0}/purge/{1}".format(
        service_id, surrogate_key
    )

    # Mock the API call and response
    responses.add(responses.POST, url, status=200)

    client = FastlyService(service_id, api_key)

    client.purge_key(surrogate_key)
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == url
    assert responses.calls[0].request.headers["Fastly-Key"] == api_key
    assert responses.calls[0].request.headers["Accept"] == "application/json"


@responses.activate
def test_purge_key_fail() -> None:
    service_id = "SU1Z0isxPaozGVKXdv0eY"
    api_key = "d3cafb4dde4dbeef"
    surrogate_key = uuid.uuid4().hex

    url = "https://api.fastly.com/service/{0}/purge/{1}".format(
        service_id, surrogate_key
    )

    # Mock the API call and response
    responses.add(responses.POST, url, status=404)

    client = FastlyService(service_id, api_key)

    with pytest.raises(FastlyError):
        client.purge_key(surrogate_key)
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == url
    assert responses.calls[0].request.headers["Fastly-Key"] == api_key
    assert responses.calls[0].request.headers["Accept"] == "application/json"
