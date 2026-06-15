from __future__ import annotations

import argparse
import json
from pathlib import Path

from mje.data import Item, build_items, download_bird, download_spider
from mje.grade import parse_pred_edges, score
from mje.model_client import PRICING, ModelClient, cost
from mje.prompt import build_messages
from mje.renderers import RENDERERS


def run_eval(
    items: list[Item],
    models: list[str],
    client,
    out_path: Path,
    max_spend: float = 2.0,
    max_tokens: int = 4096,
    samples: int = 1,
    renderings: dict | None = None,
) -> list[dict]:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    spent = 0.0
    EST_PER_CALL = 0.02  # conservative pre-call guess (USD) for the budget gate
    renderings = renderings if renderings is not None else RENDERERS

    with out_path.open("w") as fh:
        for it in items:
            for rname, rfn in renderings.items():
                rendering = rfn(it.graph)
                messages = build_messages(rendering, it.endpoint_tables)
                for model in models:
                    for s in range(samples):
                        if spent + EST_PER_CALL > max_spend:
                            print(
                                "[budget] stopping: spent"
                                f" ${spent:.3f}, cap ${max_spend:.2f}"
                            )
                            return rows
                        text, in_tok, out_tok = client.call(
                            model, messages, max_tokens=max_tokens
                        )
                        call_cost = cost(model, in_tok, out_tok)
                        spent += call_cost
                        pred = parse_pred_edges(text, it.graph)
                        sc = score(pred, it.gold_edges, it.graph)
                        row = {
                            "item_id": it.item_id,
                            "db_id": it.db_id,
                            "n_joins": it.n_joins,
                            "ambiguous": it.ambiguous,
                            "model": model,
                            "rendering": rname,
                            "sample": s,
                            "in_tok": in_tok,
                            "out_tok": out_tok,
                            "truncated": out_tok >= max_tokens,
                            "cost_usd": call_cost,
                            "raw": text[:1000],
                            **sc,
                            "gold_edges": [
                                sorted(tuple(p) for p in e) for e in it.gold_edges
                            ],
                            "pred_edges": [sorted(tuple(p) for p in e) for e in pred],
                        }
                        fh.write(json.dumps(row) + "\n")
                        fh.flush()
                        rows.append(row)
    print(f"[done] {len(rows)} calls, spent ${spent:.3f}")
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--tables", default=None, help="override tables.json path")
    ap.add_argument("--dev", default=None, help="override dev.json path")
    ap.add_argument("--out", default="results/results.jsonl")
    ap.add_argument("--min-joins", type=int, default=2)
    ap.add_argument(
        "--n", type=int, default=45, help="max items (hardest strata first)"
    )
    ap.add_argument("--samples", type=int, default=1)
    ap.add_argument(
        "--max-tokens",
        type=int,
        default=4096,
        help="output token cap; must be high enough for reasoning models",
    )
    ap.add_argument("--max-spend", type=float, default=2.0)
    ap.add_argument(
        "--models",
        nargs="+",
        default=["anthropic/claude-sonnet-4.6", "deepseek/deepseek-v4-flash"],
    )
    ap.add_argument("--dataset", choices=["spider", "bird"], default="spider")
    ap.add_argument("--query-key", default=None)
    ap.add_argument(
        "--renderings",
        nargs="+",
        default=None,
        choices=list(RENDERERS),
        help="subset of renderings to run (default: all)",
    )
    args = ap.parse_args()

    unknown = [m for m in args.models if m not in PRICING]
    if unknown:
        raise SystemExit(f"Unknown model(s): {unknown}. Supported: {sorted(PRICING)}")

    if args.query_key is not None:
        query_key = args.query_key
    elif args.dataset == "bird":
        query_key = "SQL"
    else:
        query_key = "query"

    if args.tables and args.dev:
        tables_p, dev_p = Path(args.tables), Path(args.dev)
    elif args.dataset == "bird":
        tables_p, dev_p = download_bird(Path(args.data_dir) / "bird")
    else:
        tables_p, dev_p = download_spider(Path(args.data_dir))

    items = build_items(tables_p, dev_p, min_joins=args.min_joins, query_key=query_key)
    items.sort(key=lambda it: it.n_joins, reverse=True)
    items = items[: args.n]

    renderings = (
        {k: RENDERERS[k] for k in args.renderings} if args.renderings else RENDERERS
    )

    est = len(items) * len(renderings) * len(args.models) * args.samples * 0.02
    n_items = len(items)
    n_renderings = len(renderings)
    n_models = len(args.models)
    print(
        f"[plan] {n_items} items x {n_renderings} renderings x {n_models} models "
        f"x {args.samples} sample(s); rough est ${est:.2f}; cap ${args.max_spend:.2f}"
    )

    client = ModelClient()
    run_eval(
        items,
        models=args.models,
        client=client,
        out_path=Path(args.out),
        max_spend=args.max_spend,
        max_tokens=args.max_tokens,
        samples=args.samples,
        renderings=renderings,
    )


if __name__ == "__main__":
    main()
