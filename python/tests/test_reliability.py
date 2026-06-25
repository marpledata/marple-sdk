from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest
import requests
from requests import Response
from requests.adapters import HTTPAdapter

from marple.utils import DBClient, TimeoutHTTPAdapter


@contextmanager
def retry_test_server(statuses: list[int]) -> Iterator[tuple[str, dict[str, int]]]:
    calls = {"GET": 0, "POST": 0, "PUT": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._handle()

        def do_POST(self) -> None:
            self._handle()

        def do_PUT(self) -> None:
            self._handle()

        def log_message(self, format: str, *args) -> None:
            return

        def _handle(self) -> None:
            calls[self.command] += 1
            length = int(self.headers.get("Content-Length", "0"))
            if length:
                self.rfile.read(length)
            status = statuses[min(calls[self.command] - 1, len(statuses) - 1)]
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()

    try:
        addr = server.server_address
        assert addr is not None
        yield f"http://{str(addr[0])}:{addr[1]}", calls
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def make_client(api_url: str = "http://example.test") -> DBClient:
    return DBClient(
        api_token="test-token",
        api_url=api_url,
        datapool="default",
        cache_folder="./.test_cache",
    )


def test_api_session_retries_retryable_get_status() -> None:
    with retry_test_server([503, 200]) as (api_url, calls):
        response = make_client(api_url).get("/health")

    assert response.status_code == 200
    assert calls["GET"] == 2


def test_sessions_use_configured_timeouts() -> None:
    client = make_client()

    api_adapter = client.session.get_adapter("http://example.test")
    storage_adapter = client.storage_session.get_adapter("http://example.test")

    assert isinstance(api_adapter, TimeoutHTTPAdapter)
    assert isinstance(storage_adapter, TimeoutHTTPAdapter)
    assert api_adapter.timeout == DBClient.API_TIMEOUT
    assert storage_adapter.timeout == DBClient.STORAGE_TIMEOUT


def test_timeout_http_adapter_applies_default(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[object] = []

    def capture_send(self, request, **kwargs) -> Response:
        seen.append(kwargs.get("timeout"))
        response = Response()
        response.status_code = 200
        return response

    monkeypatch.setattr(HTTPAdapter, "send", capture_send)

    adapter = TimeoutHTTPAdapter(timeout=DBClient.API_TIMEOUT)
    request = requests.Request("GET", "http://example.test/health").prepare()

    adapter.send(request)
    adapter.send(request, timeout=(1, 2))

    assert seen == [DBClient.API_TIMEOUT, (1, 2)]


def test_storage_put_retries_retryable_status() -> None:
    with retry_test_server([503, 200]) as (storage_url, calls):
        response = make_client().storage_session.put(f"{storage_url}/blob", data=b"payload")

    assert response.status_code == 200
    assert calls["PUT"] == 2


def test_api_post_body_is_not_replayed_after_status_failure() -> None:
    with retry_test_server([503, 200]) as (api_url, calls):
        response = make_client(api_url).post("/ingestion", data=b"payload")

    assert response.status_code == 503
    assert calls["POST"] == 1
