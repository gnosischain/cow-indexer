from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from curl_cffi.requests import AsyncSession


@dataclass(slots=True)
class HttpResponse:
    status: int
    data: Any
    headers: dict[str, str] = field(default_factory=dict)
    text: str = ""


class HttpTransport(Protocol):
    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> HttpResponse: ...

    async def close(self) -> None: ...


class CurlTransport:
    """curl_cffi transport using browser TLS impersonation for CoW's edge."""

    def __init__(self, timeout: float = 30.0) -> None:
        self._session = AsyncSession(impersonate="chrome", timeout=timeout)

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> HttpResponse:
        response = await self._session.request(method, url, params=params, json=json)
        text = response.text
        try:
            data = response.json()
        except ValueError:
            data = None
        return HttpResponse(
            status=response.status_code,
            data=data,
            headers={key.lower(): value for key, value in response.headers.items()},
            text=text,
        )

    async def close(self) -> None:
        await self._session.close()
