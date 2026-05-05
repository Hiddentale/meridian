import json
import ssl
import time
import urllib.error
import urllib.request

GRAPHQL_URL = "https://www.jumbo.com/api/graphql"

GRAPHQL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.jumbo.com",
    "Referer": "https://www.jumbo.com/",
    "x-source": "JUMBO_WEB",
    "apollographql-client-name": "JUMBO_WEB",
    "apollographql-client-version": "1.0.0",
}

_ssl_ctx = ssl.create_default_context()


def _gql(operation_name: str, query: str, variables: dict) -> dict:
    payload = json.dumps(
        {
            "operationName": operation_name,
            "query": query,
            "variables": variables,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        GRAPHQL_URL,
        data=payload,
        headers=GRAPHQL_HEADERS,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} from GraphQL: {body[:300]}") from e


def gql(
    operation_name: str, query: str, variables: dict, max_attempts: int = 3
) -> dict:
    """Execute a GraphQL request with exponential backoff retry."""
    for attempt in range(1, max_attempts + 1):
        try:
            result = _gql(operation_name, query, variables)
            if result.get("data") is not None:
                return result
            errs = result.get("errors", [])
            if attempt < max_attempts:
                wait = 2**attempt
                print(
                    f"\n  Retry {attempt}/{max_attempts} for {operation_name} "
                    f"({errs[0].get('message','')[:60] if errs else 'null data'}) "
                    f"waiting {wait}s"
                )
                time.sleep(wait)
            else:
                return result
        except Exception as exc:
            if attempt < max_attempts:
                wait = 2**attempt
                print(
                    f"\n  Retry {attempt}/{max_attempts} for {operation_name}: "
                    f"{exc}, waiting {wait}s"
                )
                time.sleep(wait)
            else:
                raise
