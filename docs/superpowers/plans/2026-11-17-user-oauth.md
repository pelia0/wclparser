# User OAuth for archived WarcraftLogs reports

Goal: implement the requested user authorization so a subscribed WCL account can read archived Legion reports through `/api/v2/user`, then make the browser/API comparison possible after the user confirms access.

Architecture: retain client credentials and browser cookies separately; add Authorization Code OAuth with a ten-minute, single-use state, browser-bound callback cookie, explicit loopback redirect validation, refresh-token rotation and a separate user-token state. Support the already recommended `http://localhost:8080/` root redirect and a dedicated callback path. A UI button navigates to the chosen loopback host before redirecting to WCL, so localhost vs 127.0.0.1 cookie binding works. Request only profile scope; private-report permission is not needed for this historical public-log collection.

Interfaces: `WCLAuthManager.begin_user_oauth(redirect_uri)` returns authorization_url/state/expires_in; async `complete_user_oauth(code,state)` validates/consumes state and stores tokens; `disconnect_user_oauth()` preserves other auth; async `get_api_auth()` returns `(endpoint, token)` and refreshes user tokens under a lock. Status exposes only safe booleans/mode/expiry, never credentials. Protocol GraphQL calls use that endpoint and capture `/client` and `/user` responses equally. User-authorized metadata/events take the API path directly; legacy browser/client behavior remains compatible.

Constraints: preserve all historical records and source precision, no silent fallback from failed user authorization, no automatic source switch/harvest/server restart. Tokens remain ignored runtime data. Live WCL consent and subscription access must be confirmed by the user after a manual restart; mocked tests are not live acceptance.

- [x] Auth manager: state replay/expiry, redirect confinement, token exchange/refresh concurrency, credential changes, disconnect and atomic persistence; RED/GREEN tests.
- [x] API/UI: start redirect and HttpOnly state cookie, callback at root/dedicated path, denial/error handling, separate client/account status and connect/disconnect; mocked integration/browser tests.
- [x] Protocol: user endpoint selection, direct archived report/events, preserved raw capture/precision, 20k pagination and cancelable shared quota handling; tests.
- [x] Full suite, independent review and README. Source-only publication excludes runtime authentication and archives; production data/server remain unchanged.

Verification: complete suite passes with 433 tests. Independent final review ran all 87 user OAuth tests and approved the fixes for lost pet ownership, inaccessible archive metadata, expired-mode disconnect and cancelable long quota cooldowns. The browser UI was checked in an isolated headless profile using mock consent/callback/API responses and a visually inspected screenshot. The real subscribed account has not yet granted the new Authorization Code flow; live archive access and same-fight browser/API equivalence remain acceptance checks after the user's manual restart and WCL consent. No production token or browser session was altered by these tests.
