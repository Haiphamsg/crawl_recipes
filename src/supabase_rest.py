from __future__ import annotations

import json
from typing import Any, Dict, Optional

import requests


class SupabaseRest:
    def __init__(self, supabase_url: str, service_role_key: str, timeout_s: int = 30):
        self.base = supabase_url.rstrip("/")
        self.timeout_s = timeout_s
        self.session = requests.Session()
        self.session.headers.update(
            {
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def rpc(self, fn_name: str, payload: Dict[str, Any]) -> Any:
        url = f"{self.base}/rest/v1/rpc/{fn_name}"
        resp = self.session.post(url, data=json.dumps(payload), timeout=self.timeout_s)
        if resp.status_code >= 400:
            raise RuntimeError(f"RPC {fn_name} failed: {resp.status_code} {resp.text}")
        if not resp.text or resp.text.strip() in ("null", ""):
            return None
        return resp.json()

    def select_one(self, table: str, query: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/rest/v1/{table}?{query}"
        resp = self.session.get(url, timeout=self.timeout_s)
        if resp.status_code >= 400:
            raise RuntimeError(f"SELECT {table} failed: {resp.status_code} {resp.text}")
        rows = resp.json()
        if not rows:
            return None
        return rows[0]

    def upsert(self, table: str, rows: Any, on_conflict: str) -> None:
        url = f"{self.base}/rest/v1/{table}?on_conflict={on_conflict}"
        headers = {"Prefer": "resolution=merge-duplicates,return=minimal"}
        resp = self.session.post(
            url, data=json.dumps(rows), headers=headers, timeout=self.timeout_s
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"UPSERT {table} failed: {resp.status_code} {resp.text}")

    def insert(self, table: str, rows: Any) -> None:
        if not rows:
            return
        url = f"{self.base}/rest/v1/{table}"
        headers = {"Prefer": "return=minimal"}
        resp = self.session.post(
            url, data=json.dumps(rows), headers=headers, timeout=self.timeout_s
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"INSERT {table} failed: {resp.status_code} {resp.text}")

    def delete_where(self, table: str, filter_query: str) -> None:
        url = f"{self.base}/rest/v1/{table}?{filter_query}"
        headers = {"Prefer": "return=minimal"}
        resp = self.session.delete(url, headers=headers, timeout=self.timeout_s)
        if resp.status_code >= 400:
            raise RuntimeError(f"DELETE {table} failed: {resp.status_code} {resp.text}")

