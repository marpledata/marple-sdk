from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest
from requests import Response
from requests.adapters import HTTPAdapter

from marple.utils import DBClient


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


def test_default_timeout_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    timeouts = []

    def fake_send(self, request, **kwargs) -> Response:
        timeouts.append(kwargs["timeout"])
        response = Response()
        response.status_code = 200
        response.url = request.url
        response._content = b"{}"
        return response

    monkeypatch.setattr(HTTPAdapter, "send", fake_send)

    client = make_client()
    client.get("/health")
    client.get("/health", timeout=(1, 2))
    client.storage_session.put("http://example.test/blob", data=b"payload")

    assert timeouts == [DBClient.DEFAULT_TIMEOUT, (1, 2), DBClient.STORAGE_TIMEOUT]


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
