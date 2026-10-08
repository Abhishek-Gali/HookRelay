import pytest
import httpx
from app.providers import SlackProvider, GenericHttpProvider


@pytest.mark.asyncio
async def test_slack_provider_dispatch():
    called = []

    def mock_handler(request: httpx.Request):
        called.append(request)
        return httpx.Response(200, text="ok")

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        provider = SlackProvider()
        status, err, _ = await provider.send(
            event_type="push",
            payload={"repository": {"full_name": "acme/widget"}, "sender": {"login": "alice"}},
            destination_url="https://hooks.slack.com/services/mock",
            client=client
        )

    assert status == 200
    assert err is None
    assert len(called) == 1


@pytest.mark.asyncio
async def test_generic_http_provider_dispatch():
    called = []

    def mock_handler(request: httpx.Request):
        called.append(request)
        return httpx.Response(204)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        provider = GenericHttpProvider()
        status, err, _ = await provider.send(
            event_type="issues",
            payload={"action": "opened"},
            destination_url="https://api.mycompany.com/webhook",
            client=client
        )

    assert status == 204
    assert err is None
    assert called[0].headers.get("X-HookRelay-Event") == "issues"
