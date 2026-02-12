#!/usr/bin/env python3
"""Generate review-first BigQuery cleanup SQL for outage recovery windows."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Iterable


def _date(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%d")
    return value


def _col_list(value: str) -> list[str]:
    cols = [c.strip() for c in value.split(",") if c.strip()]
    if not cols:
        raise argparse.ArgumentTypeError("expected comma-separated column names")
    return cols


def _date_predicate(col: str, start: str, end: str) -> str:
    return f"{col} BETWEEN DATE('{start}') AND DATE('{end}')"


def _timestamp_predicates(columns: Iterable[str], start: str, end: str) -> str:
    preds = [f"DATE({col}) BETWEEN DATE('{start}') AND DATE('{end}')" for col in columns]
    return "\n   OR ".join(preds)


def build_sql(args: argparse.Namespace) -> str:
    parts: list[str] = []
    parts.append(
        f"-- Outage recovery SQL | store={args.store_prefix} | window={args.start_date}..{args.end_date}\n"
        "-- IMPORTANT: review every statement before execution."
    )

    # Preview blocks
    parts.append(
        f"""
-- 0) Preview merged impact
SELECT {args.merged_date_column} AS dia, COUNT(*) AS row_count, COUNT(DISTINCT pedido_number) AS pedidos, SUM(valor) AS valor_total
FROM `{args.project}.{args.merged_dataset}.{args.merged_items_table}`
WHERE {_date_predicate(args.merged_date_column, args.start_date, args.end_date)}
GROUP BY dia
ORDER BY dia;
""".strip()
    )

    # Delete blocks
    parts.append(
        f"""
-- 1) Raw cleanup
DELETE FROM `{args.project}.{args.raw_dataset}.{args.raw_pdv_table}`
WHERE {_date_predicate(args.raw_pdv_date_column, args.start_date, args.end_date)};

DELETE FROM `{args.project}.{args.raw_dataset}.{args.raw_pesquisa_table}`
WHERE {_date_predicate(args.raw_pesquisa_date_column, args.start_date, args.end_date)};

DELETE FROM `{args.project}.{args.raw_dataset}.{args.raw_produto_table}`
WHERE {_timestamp_predicates(args.raw_produto_timestamp_columns, args.start_date, args.end_date)};
""".strip()
    )

    parts.append(
        f"""
-- 2) Transformed cleanup
DELETE FROM `{args.project}.{args.transformed_dataset}.{args.transformed_pedidos_table}`
WHERE {_date_predicate(args.transformed_date_column, args.start_date, args.end_date)};

DELETE FROM `{args.project}.{args.transformed_dataset}.{args.transformed_produtos_table}`
WHERE {_date_predicate(args.transformed_date_column, args.start_date, args.end_date)};
""".strip()
    )

    parts.append(
        f"""
-- 3) Loyalty cleanup
DELETE FROM `{args.project}.{args.loyalty_dataset}.{args.loyalty_pedidos_table}`
WHERE {_date_predicate(args.loyalty_date_column, args.start_date, args.end_date)};

DELETE FROM `{args.project}.{args.loyalty_dataset}.{args.loyalty_produtos_table}`
WHERE {_date_predicate(args.loyalty_date_column, args.start_date, args.end_date)};
""".strip()
    )

    parts.append(
        f"""
-- 4) Optional merged cleanup (usually unnecessary if merged is rebuilt via CREATE OR REPLACE)
-- DELETE FROM `{args.project}.{args.merged_dataset}.{args.merged_items_table}`
-- WHERE {_date_predicate(args.merged_date_column, args.start_date, args.end_date)};
""".strip()
    )

    parts.append(
        f"""
-- 5) Post-backfill verification
SELECT {args.merged_date_column} AS dia, COUNT(*) AS row_count, COUNT(DISTINCT pedido_number) AS pedidos, SUM(valor) AS valor_total
FROM `{args.project}.{args.merged_dataset}.{args.merged_items_table}`
WHERE {_date_predicate(args.merged_date_column, args.start_date, args.end_date)}
GROUP BY dia
ORDER BY dia;
""".strip()
    )

    return "\n\n".join(parts) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate review-first SQL for targeted outage cleanup.")
    parser.add_argument("--project", required=True)
    parser.add_argument("--store-prefix", required=True)

    parser.add_argument("--raw-dataset", required=True)
    parser.add_argument("--raw-pdv-table", default="pdv")
    parser.add_argument("--raw-pdv-date-column", default="data")
    parser.add_argument("--raw-pesquisa-table", default="pesquisa")
    parser.add_argument("--raw-pesquisa-date-column", default="data_pedido")
    parser.add_argument("--raw-produto-table", default="produto")
    parser.add_argument("--raw-produto-timestamp-columns", type=_col_list, default=["timestamp", "update_timestamp"])

    parser.add_argument("--transformed-dataset", required=True)
    parser.add_argument("--transformed-pedidos-table", default="pedidos")
    parser.add_argument("--transformed-produtos-table", default="produtos")
    parser.add_argument("--transformed-date-column", default="pedido_dia")

    parser.add_argument("--loyalty-dataset", required=True)
    parser.add_argument("--loyalty-pedidos-table", default="pedidos")
    parser.add_argument("--loyalty-produtos-table", default="produtos")
    parser.add_argument("--loyalty-date-column", default="pedido_dia")

    parser.add_argument("--merged-dataset", required=True)
    parser.add_argument("--merged-items-table", default="z316-itenspedido")
    parser.add_argument("--merged-date-column", default="data_pedido")

    parser.add_argument("--start-date", type=_date, required=True)
    parser.add_argument("--end-date", type=_date, required=True)
    args = parser.parse_args()

    print(build_sql(args))


if __name__ == "__main__":
    main()
