import json
from typing import Any, Optional
from urllib import error, parse, request

from board_worker.config import BoardWorkerConfig


class BoardApiClient:
    def __init__(self, config: BoardWorkerConfig):
        self.config = config

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_token}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, payload: Optional[dict[str, Any]] = None) -> Any:
        url = f"{self.config.api_url}{path}"
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        req = request.Request(url=url, method=method, data=data, headers=self._headers())
        try:
            with request.urlopen(req, timeout=60) as response:
                body = response.read()
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {path} failed: {exc.code} {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"{method} {path} failed: {exc}") from exc
        if not body:
            return None
        return json.loads(body.decode("utf-8"))

    def get(self, path: str, query: Optional[dict[str, Any]] = None) -> Any:
        query_string = ""
        if query:
            filtered = {key: value for key, value in query.items() if value is not None}
            query_string = "?" + parse.urlencode(filtered)
        return self._request("GET", f"{path}{query_string}")

    def post(self, path: str, payload: Optional[dict[str, Any]] = None) -> Any:
        return self._request("POST", path, payload)

    def register_agent(self) -> dict[str, Any]:
        return self.post(
            "/agents/register",
            {
                "name": self.config.agent_name,
                "host": self.config.host_name,
                "capabilities": self.config.capabilities,
                "status": "idle",
                "enabled": True,
                "metadata": {"worker": "board_worker"},
            },
        )

    def heartbeat_agent(self, agent_id: str, status: str) -> dict[str, Any]:
        return self.post(
            "/agents/heartbeat",
            {"agent_id": agent_id, "status": status, "metadata": {"worker": "board_worker"}},
        )
