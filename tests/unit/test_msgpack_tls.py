"""Real TLS handshakes against the MessagePack server's SSL context (needs the openssl CLI)."""

import asyncio
import shutil
import ssl
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio

from microdcs import MessagePackConfig
from microdcs.msgpack import MessagePackHandler
from microdcs.redis import RedisKeySchema

pytestmark = pytest.mark.skipif(
    shutil.which("openssl") is None, reason="openssl CLI not available"
)


def _openssl(*args: str, cwd: Path) -> None:
    subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture(scope="module")
def pki(tmp_path_factory: pytest.TempPathFactory) -> Path:
    d = tmp_path_factory.mktemp("pki")
    # Python's strict X.509 verification needs CA key usage and key identifiers.
    (d / "ext.cnf").write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nauthorityKeyIdentifier=keyid\n"
    )
    (d / "client.cnf").write_text("authorityKeyIdentifier=keyid\n")
    _openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key",
        "-out", "ca.crt", "-days", "2", "-subj", "/CN=test-ca",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        "-addext", "subjectKeyIdentifier=hash",
        cwd=d,
    )  # fmt: skip
    for name, ext in (
        ("tls", ["-extfile", "ext.cnf"]),
        ("client", ["-extfile", "client.cnf"]),
    ):
        _openssl(
            "req", "-newkey", "rsa:2048", "-nodes", "-keyout", f"{name}.key",
            "-out", f"{name}.csr", "-subj", f"/CN={name}",
            cwd=d,
        )  # fmt: skip
        _openssl(
            "x509", "-req", "-in", f"{name}.csr", "-CA", "ca.crt", "-CAkey", "ca.key",
            "-CAcreateserial", "-out", f"{name}.crt", "-days", "2", *ext,
            cwd=d,
        )  # fmt: skip
    return d


def _server_context(pki: Path, client_auth: bool) -> ssl.SSLContext:
    config = MessagePackConfig(
        tls_cert_path=pki / "ca.crt",
        tls_server_cert_path=pki / "tls.crt",
        tls_server_key_path=pki / "tls.key",
        tls_client_auth=client_auth,
    )
    with patch("microdcs.msgpack.redis.Redis"):
        handler = MessagePackHandler(config, MagicMock(), RedisKeySchema())
    ctx = handler._server_ssl_context()
    assert ctx is not None
    return ctx


@pytest_asyncio.fixture
async def serve():
    servers: list[asyncio.Server] = []

    async def start(ctx: ssl.SSLContext) -> int:
        async def on_connect(reader, writer):
            writer.write(b"ok")
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(on_connect, "127.0.0.1", 0, ssl=ctx)
        servers.append(server)
        return server.sockets[0].getsockname()[1]

    yield start
    for server in servers:
        server.close()
        await server.wait_closed()


async def _connect_and_read(port: int, ctx: ssl.SSLContext) -> bytes:
    reader, writer = await asyncio.open_connection("localhost", port, ssl=ctx)
    try:
        return await reader.read(2)
    finally:
        writer.close()


class TestMessagePackServerTls:
    @pytest.mark.asyncio
    async def test_server_certificate_handshake_succeeds(self, pki, serve):
        port = await serve(_server_context(pki, client_auth=False))
        client_ctx = ssl.create_default_context(cafile=str(pki / "ca.crt"))
        assert await _connect_and_read(port, client_ctx) == b"ok"

    @pytest.mark.asyncio
    async def test_client_auth_rejects_client_without_certificate(self, pki, serve):
        port = await serve(_server_context(pki, client_auth=True))
        client_ctx = ssl.create_default_context(cafile=str(pki / "ca.crt"))
        # With TLS 1.3 the rejection may surface as an error or as an empty read.
        try:
            data = await _connect_and_read(port, client_ctx)
        except ssl.SSLError, ConnectionError:
            return
        assert data != b"ok"

    @pytest.mark.asyncio
    async def test_client_auth_accepts_client_with_certificate(self, pki, serve):
        port = await serve(_server_context(pki, client_auth=True))
        client_ctx = ssl.create_default_context(cafile=str(pki / "ca.crt"))
        client_ctx.load_cert_chain(str(pki / "client.crt"), str(pki / "client.key"))
        assert await _connect_and_read(port, client_ctx) == b"ok"
