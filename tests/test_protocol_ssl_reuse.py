import shutil
import ssl

import certifi
import pytest

from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient


class SessionAuth:
    def get_headers_and_cookies(self):
        return {}, {}


@pytest.mark.asyncio
@pytest.mark.parametrize('trust_source', ['default', 'SSL_CERT_FILE', 'SSL_CERT_DIR'])
async def test_request_clients_reuse_verified_context_without_reloading_trust(
    tmp_path, monkeypatch, trust_source
):
    # Keep actual HTTPX transports, isolating machine proxy and trust settings.
    for name in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
                 'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy',
                 'SSL_CERT_FILE', 'SSL_CERT_DIR'):
        monkeypatch.delenv(name, raising=False)
    expected_locations = {'cafile': certifi.where()}
    if trust_source == 'SSL_CERT_FILE':
        bundle = tmp_path / 'custom-ca.pem'
        shutil.copyfile(certifi.where(), bundle)
        monkeypatch.setenv(trust_source, str(bundle))
        expected_locations = {'cafile': str(bundle)}
    elif trust_source == 'SSL_CERT_DIR':
        bundle_dir = tmp_path / 'custom-ca'
        bundle_dir.mkdir()
        monkeypatch.setenv(trust_source, str(bundle_dir))
        expected_locations = {'capath': str(bundle_dir)}

    trust_loads = []
    original_load = ssl.SSLContext.load_verify_locations

    def record_trust_load(context, *args, **kwargs):
        trust_loads.append(kwargs)
        return original_load(context, *args, **kwargs)

    monkeypatch.setattr(ssl.SSLContext, 'load_verify_locations', record_trust_load)
    protocol = WCLProtocolClient(SessionAuth())
    contexts = []
    for _ in range(3):
        async with protocol._make_client() as client:
            contexts.append(client._transport._pool._ssl_context)

    assert trust_loads == [expected_locations]
    assert all(context is contexts[0] for context in contexts)
    assert contexts[0].verify_mode == ssl.CERT_REQUIRED
    assert contexts[0].check_hostname is True
