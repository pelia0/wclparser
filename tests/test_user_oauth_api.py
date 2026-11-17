from urllib.parse import parse_qs, urlsplit
import json

import httpx
import pytest

from wcl_archiver.api.app import create_app
from wcl_archiver.wcl_client.auth import WCLAuthManager
from wcl_archiver.wcl_client.protocol_parser import WCLProtocolClient
from wcl_archiver.db.schema import IndexDB


def fake_flow(monkeypatch):
    calls = []
    def begin(self, uri):
        calls.append(('begin', uri))
        return {'authorization_url': 'https://www.warcraftlogs.com/oauth/authorize?state=nonce',
                'state': 'nonce', 'expires_in': 600}
    async def complete(self, code, state):
        calls.append(('complete', code, state))
        return {'has_user_oauth_token': True}
    def cancel(self, state):
        calls.append(('cancel', state))
    monkeypatch.setattr(WCLAuthManager, 'begin_user_oauth', begin, raising=False)
    monkeypatch.setattr(WCLAuthManager, 'complete_user_oauth', complete, raising=False)
    monkeypatch.setattr(WCLAuthManager, 'cancel_user_oauth', cancel, raising=False)
    return calls


async def test_root_callback_exchanges_code_and_never_returns_secrets(tmp_path, monkeypatch):
    calls = fake_flow(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        assert (await client.get('/')).status_code == 200
        response = await client.get('/api/auth/oauth/start', params={'redirect_uri': 'http://localhost:8080/'})
        assert response.status_code == 303
        assert response.headers['location'].startswith('https://www.warcraftlogs.com/oauth/authorize')
        cookie = response.headers['set-cookie'].lower()
        assert 'httponly' in cookie and 'samesite=lax' in cookie
        response = await client.get('/', params={'code': 'private-code', 'state': 'nonce'})
        assert response.status_code == 303
        assert response.headers['location'] == '/?oauth=connected'
        assert response.headers['cache-control'] == 'no-store'
        assert 'private-code' not in response.text
        assert calls == [('begin', 'http://localhost:8080/'), ('complete', 'private-code', 'nonce')]
        assert (await client.get('/?code=private-code&state=nonce')).status_code == 400
        assert len(calls) == 2


@pytest.mark.parametrize('query', ['code=x&state=wrong', 'code=x&state=nonce&state=other', 'code=x&code=y&state=nonce'])
async def test_callback_rejects_wrong_or_duplicate_state_before_exchange(tmp_path, monkeypatch, query):
    calls = fake_flow(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        await client.get('/api/auth/oauth/start')
        assert (await client.get('/?' + query)).status_code == 400
        assert not any(c[0] == 'complete' for c in calls)


async def test_callback_requires_the_initiating_browser_cookie(tmp_path, monkeypatch):
    calls = fake_flow(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        response = await client.get('/?code=x&state=nonce')
        assert response.status_code == 400
        assert calls == []


async def test_alias_start_moves_to_redirect_host_before_setting_cookie(tmp_path, monkeypatch):
    calls = fake_flow(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://127.0.0.1:8080') as client:
        response = await client.get('/api/auth/oauth/start', params={'redirect_uri': 'http://localhost:8080/'})
        assert response.status_code == 307
        target = urlsplit(response.headers['location'])
        assert target.netloc == 'localhost:8080' and target.path == '/api/auth/oauth/start'
        assert parse_qs(target.query)['redirect_uri'] == ['http://localhost:8080/']
        assert calls == []


async def test_denial_consumes_state_without_echoing_provider_content(tmp_path, monkeypatch):
    calls = fake_flow(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        await client.get('/api/auth/oauth/start')
        response = await client.get('/?error=access_denied&error_description=private&state=nonce')
        assert response.status_code == 400
        assert 'private' not in response.text
        assert calls[-1] == ('cancel', 'nonce')


async def test_failed_exchange_has_safe_error_page(tmp_path, monkeypatch):
    fake_flow(monkeypatch)
    async def fail(self, *args):
        raise RuntimeError('do-not-expose-secret')
    monkeypatch.setattr(WCLAuthManager, 'complete_user_oauth', fail)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        await client.get('/api/auth/oauth/start')
        response = await client.get('/api/auth/oauth/callback?code=hidden-code&state=nonce')
        assert response.status_code == 502
        assert 'do-not-expose-secret' not in response.text and 'hidden-code' not in response.text


@pytest.mark.parametrize('uri', ['https://example.com/', 'http://localhost:9999/', 'http://user@localhost:8080/', 'http://localhost:8080/?x=1'])
async def test_oauth_start_rejects_other_server_or_malformed_redirect(tmp_path, monkeypatch, uri):
    calls = fake_flow(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        assert (await client.get('/api/auth/oauth/start', params={'redirect_uri': uri})).status_code == 400
        assert calls == []


async def test_archive_access_check_uses_user_endpoint_and_saved_report(tmp_path, monkeypatch):
    (tmp_path / 'auth_session.json').write_text(json.dumps({'user_oauth_token':'user-token'}))
    db = IndexDB(tmp_path / 'index.db')
    db.enqueue_fight('OldReport123', 1, 'mplus', 9, 5, 42, 'boss')
    db.mark_fight_packed('OldReport123', 1, 'unused.wclf', 10, 1)
    calls = []
    async def credentials(self):
        return 'https://www.warcraftlogs.com/api/v2/user', 'user-token'
    def handler(request):
        body = json.loads(request.content)
        calls.append(body)
        assert request.url.path == '/api/v2/user'
        assert request.headers['Authorization'] == 'Bearer user-token'
        assert body['variables']['code'] == 'OldReport123'
        return httpx.Response(200, json={'data':{'rateLimitData':{'limitPerHour':3000,'pointsSpentThisHour':2,'pointsResetIn':3600},
                    'reportData':{'report':{'code':'OldReport123','archiveStatus':{'isArchived':True,'isAccessible':True}}}}})
    monkeypatch.setattr(WCLAuthManager, 'get_api_auth', credentials)
    monkeypatch.setattr(WCLProtocolClient, '_make_client', lambda self: httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        response = await client.post('/api/auth/oauth/check')
        assert response.status_code == 200
        assert response.json()['report']['isAccessible'] is True
        assert response.json()['quota']['limitPerHour'] == 3000
        assert 'user-token' not in response.text
        assert len(calls) == 1


async def test_archive_check_requires_user_authorization(tmp_path):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        response = await client.post('/api/auth/oauth/check')
        assert response.status_code == 409


async def test_disconnect_retains_client_and_browser_auth(tmp_path):
    (tmp_path / 'auth_session.json').write_text(json.dumps({'user_oauth_token':'user-token',
        'client_id':'id','client_secret':'secret','oauth_token':'client-token','cookies':{'wcl_session':'cookie'}}))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        response = await client.post('/api/auth/oauth/disconnect')
        assert response.status_code == 200
        state = response.json()
        assert not state['has_user_oauth_token']
        assert state['has_oauth_token'] and state['has_session_cookie'] and state['has_client_credentials']


async def test_real_manager_callback_and_access_check_roundtrip(tmp_path, monkeypatch):
    (tmp_path / 'auth_session.json').write_text(json.dumps({'client_id':'client-id','client_secret':'client-secret',
        'oauth_token':'client-token','cookies':{'wcl_session':'browser-cookie'}}))
    db = IndexDB(tmp_path / 'index.db')
    db.enqueue_fight('OldReport123', 1, 'mplus', 9, 5, 42, 'boss')
    db.mark_fight_packed('OldReport123', 1, 'unused.wclf', 10, 1)
    exchanges = []
    def handler(request):
        if request.url.path == '/oauth/token':
            form = parse_qs(request.content.decode())
            assert form['grant_type'] == ['authorization_code']
            assert form['redirect_uri'] == ['http://localhost:8080/']
            exchanges.append(form)
            return httpx.Response(200, json={'access_token':'user-token','refresh_token':'refresh-token','expires_in':3600})
        assert request.url.path == '/api/v2/user'
        assert request.headers['Authorization'] == 'Bearer user-token'
        return httpx.Response(200, json={'data':{'rateLimitData':{'limitPerHour':3000,'pointsSpentThisHour':2,'pointsResetIn':3600},
            'reportData':{'report':{'archiveStatus':{'isArchived':True,'isAccessible':True}}}}})
    transport = httpx.MockTransport(handler)
    initialize = WCLAuthManager.__init__
    def init(self, *args, **kwargs):
        kwargs['transport'] = transport
        initialize(self, *args, **kwargs)
    monkeypatch.setattr(WCLAuthManager, '__init__', init)
    monkeypatch.setattr(WCLProtocolClient, '_make_client', lambda self: httpx.AsyncClient(transport=transport))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(create_app(tmp_path)), base_url='http://localhost:8080') as client:
        start = await client.get('/api/auth/oauth/start')
        state = parse_qs(urlsplit(start.headers['location']).query)['state'][0]
        callback = await client.get('/', params={'code':'authorization-code','state':state})
        assert callback.status_code == 303
        status = (await client.get('/api/auth/status')).json()
        assert status['oauth_mode'] == 'user' and status['has_user_oauth_token']
        assert status['has_oauth_token'] and status['has_session_cookie']
        assert not any(secret in json.dumps(status) for secret in ('user-token','refresh-token','client-secret','browser-cookie'))
        check = await client.post('/api/auth/oauth/check')
        assert check.status_code == 200 and check.json()['report']['isAccessible']
        replay = await client.get('/', params={'code':'authorization-code','state':state})
        assert replay.status_code == 400
        assert len(exchanges) == 1
