#!/usr/bin/env python3
"""Benchmark sparse device-shard routing against exhaustive retrieval.

Raw queries are read locally and sent to the service but are never written to
the output. Output identifies each query only by SHA-256.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

RECEIPT_FIELDS = {
    "receipt_id",
    "query_sha256",
    "eligible_shards",
    "ranked_shards",
    "selected_shards",
    "skipped_shards",
    "outcomes",
    "final_result_ids",
    "merge_digest",
}


def recall_at_k(sparse_ids: list[str], exhaustive_ids: list[str], top_k: int) -> float:
    reference = exhaustive_ids[:top_k]
    if not reference:
        return 1.0
    return len(set(sparse_ids[:top_k]).intersection(reference)) / len(set(reference))


def nearest_rank_percentile(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def receipt_complete(receipt: dict[str, Any]) -> bool:
    return RECEIPT_FIELDS.issubset(receipt) and isinstance(receipt.get("outcomes"), list)


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def request_search(
    url: str,
    credential: str,
    query: str,
    top_k: int,
    budget: int,
    exhaustive: bool,
    timeout: float,
) -> tuple[dict[str, Any], float]:
    payload = json.dumps(
        {
            "query": query,
            "limit": top_k,
            "shard_budget": budget,
            "exhaustive": exhaustive,
        }
    ).encode()
    request = urllib.request.Request(
        url.rstrip("/") + "/v1/search/witnessed",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {credential}",
            "Content-Type": "application/json",
        },
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read())
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise RuntimeError(f"HTTP {error.code}: {detail}") from error
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if not isinstance(body, dict):
        raise RuntimeError("search response is not an object")
    return body, elapsed_ms


def result_ids(body: dict[str, Any]) -> list[str]:
    return [str(value["item_id"]) for value in body.get("results", [])]


def validate_response(body: dict[str, Any]) -> dict[str, Any]:
    receipt = body.get("routing_receipt")
    if not isinstance(receipt, dict) or not receipt_complete(receipt):
        raise RuntimeError("routing receipt is incomplete")
    if body.get("receipt_stored") is not True:
        raise RuntimeError("routing receipt was not durably stored")
    errors = [outcome for outcome in receipt["outcomes"] if outcome.get("error")]
    if errors:
        raise RuntimeError(f"shard search errors were reported: {errors}")
    return receipt


def run(args: argparse.Namespace) -> dict[str, Any]:
    file_env = load_env_file(Path(args.env_file).expanduser())
    url = args.url or os.environ.get("MNEMES_URL") or file_env.get("MNEMES_URL")
    credential = (
        os.environ.get("MNEMES_CREDENTIAL")
        or file_env.get("MNEMES_CREDENTIAL")
    )
    if not url or not credential:
        raise RuntimeError("MNEMES_URL and MNEMES_CREDENTIAL are required")
    queries = json.loads(Path(args.queries).read_text())
    if not isinstance(queries, list) or not queries or not all(isinstance(q, str) and q for q in queries):
        raise RuntimeError("query file must be a non-empty JSON array of strings")

    if not args.no_warmup:
        for query in queries:
            request_search(url, credential, query, args.top_k, args.budget, False, args.timeout)
            request_search(url, credential, query, args.top_k, args.budget, True, args.timeout)

    runs: list[dict[str, Any]] = []
    for iteration in range(args.iterations):
        for query_index, query in enumerate(queries):
            order = [False, True] if (iteration + query_index) % 2 == 0 else [True, False]
            observed: dict[bool, tuple[dict[str, Any], dict[str, Any], float]] = {}
            for exhaustive in order:
                body, latency_ms = request_search(
                    url, credential, query, args.top_k, args.budget, exhaustive, args.timeout
                )
                receipt = validate_response(body)
                observed[exhaustive] = (body, receipt, latency_ms)
            sparse_body, sparse_receipt, sparse_ms = observed[False]
            exhaustive_body, exhaustive_receipt, exhaustive_ms = observed[True]
            sparse_ids = result_ids(sparse_body)
            exhaustive_ids = result_ids(exhaustive_body)
            runs.append(
                {
                    "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
                    "iteration": iteration,
                    "recall_at_k": recall_at_k(sparse_ids, exhaustive_ids, args.top_k),
                    "exact_top_k": sparse_ids[: args.top_k] == exhaustive_ids[: args.top_k],
                    "sparse_latency_ms": sparse_ms,
                    "exhaustive_latency_ms": exhaustive_ms,
                    "sparse_selected_shards": len(sparse_receipt["selected_shards"]),
                    "exhaustive_selected_shards": len(exhaustive_receipt["selected_shards"]),
                    "sparse_fallback_reason": sparse_receipt.get("fallback_reason"),
                    "sparse_result_ids": sparse_ids,
                    "exhaustive_result_ids": exhaustive_ids,
                    "sparse_receipt_id": sparse_receipt["receipt_id"],
                    "exhaustive_receipt_id": exhaustive_receipt["receipt_id"],
                }
            )

    sparse_latencies = [run["sparse_latency_ms"] for run in runs]
    exhaustive_latencies = [run["exhaustive_latency_ms"] for run in runs]
    report = {
        "schema": "pooled-shard-benchmark-v1",
        "query_count": len(queries),
        "iterations": args.iterations,
        "top_k": args.top_k,
        "sparse_budget": args.budget,
        "aggregate": {
            "mean_recall_at_k": sum(run["recall_at_k"] for run in runs) / len(runs),
            "exact_top_k_rate": sum(bool(run["exact_top_k"]) for run in runs) / len(runs),
            "sparse_p50_ms": nearest_rank_percentile(sparse_latencies, 0.50),
            "sparse_p95_ms": nearest_rank_percentile(sparse_latencies, 0.95),
            "exhaustive_p50_ms": nearest_rank_percentile(exhaustive_latencies, 0.50),
            "exhaustive_p95_ms": nearest_rank_percentile(exhaustive_latencies, 0.95),
            "mean_sparse_selected_shards": sum(run["sparse_selected_shards"] for run in runs) / len(runs),
            "mean_exhaustive_selected_shards": sum(run["exhaustive_selected_shards"] for run in runs) / len(runs),
            "fallback_rate": sum(run["sparse_fallback_reason"] is not None for run in runs) / len(runs),
            "receipt_completeness": 1.0,
        },
        "runs": runs,
    }
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--queries", required=True)
    result.add_argument("--out", required=True)
    result.add_argument("--url")
    result.add_argument("--env-file", default=str(Path.home() / ".config/mnemes/client.env"))
    result.add_argument("--top-k", type=int, default=5)
    result.add_argument("--budget", type=int, default=1)
    result.add_argument("--iterations", type=int, default=5)
    result.add_argument("--timeout", type=float, default=60.0)
    result.add_argument("--no-warmup", action="store_true")
    return result


def main() -> int:
    try:
        args = parser().parse_args()
        if args.top_k < 1 or args.budget < 1 or args.iterations < 1:
            raise RuntimeError("top-k, budget, and iterations must be positive")
        report = run(args)
        out = Path(args.out).expanduser()
        if out.exists():
            raise RuntimeError(f"output already exists: {out}")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report["aggregate"], sort_keys=True))
        return 0
    except (RuntimeError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"shard benchmark failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
