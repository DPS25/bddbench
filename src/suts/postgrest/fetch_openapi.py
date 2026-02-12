from __future__ import annotations

import argparse
from pathlib import Path

from .common import PostgrestConfig, postgrest_get


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="reports/postgrest_openapi.json")
    args = ap.parse_args()

    cfg = PostgrestConfig.from_env()
    status, body = postgrest_get(cfg, f"{cfg.url}/", extra_headers={"Accept": "application/openapi+json"})
    if status < 200 or status >= 300:
        raise RuntimeError(f"fetch_openapi failed: status={status} body={body[:300]!r}")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(body)
    print(f"[postgrest] OpenAPI saved to: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

