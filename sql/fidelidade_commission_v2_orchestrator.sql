-- Emporio Zingaro Fidelidade | Commission & Commission Details V2 (BigQuery Standard SQL script)
-- Purpose:
--   Build only commission artifacts with updated commission policy (V2), reusing the current cashback table.
--   Output tables:
--     1) comission_details_v2
--     2) comission_v2
--
-- V2 commission rule per pedido (sale):
--   - 2% for: Top1, Top3, Top5, Top10, Platina, Ouro
--   - 1% for all other cashback tiers (e.g. Prata, Bronze, fallback)

DECLARE project_id STRING DEFAULT 'emporio-zingaro';
DECLARE dataset_id STRING DEFAULT 'z316_fidelidade';
DECLARE pedidos_table STRING DEFAULT 'pedidos';

DECLARE run_date DATE DEFAULT CURRENT_DATE('America/Sao_Paulo');
DECLARE fallback_q_start DATE DEFAULT DATE_TRUNC(DATE_SUB(run_date, INTERVAL 1 QUARTER), QUARTER);
DECLARE fallback_q_end DATE DEFAULT DATE_SUB(DATE_ADD(fallback_q_start, INTERVAL 1 QUARTER), INTERVAL 1 DAY);
DECLARE fallback_qid STRING DEFAULT CONCAT(FORMAT_DATE('%y', fallback_q_start), 'Q', CAST(EXTRACT(QUARTER FROM fallback_q_start) AS STRING));

DECLARE quarter_id STRING DEFAULT fallback_qid;
DECLARE quarter_start DATE DEFAULT fallback_q_start;
DECLARE quarter_end DATE DEFAULT fallback_q_end;

DECLARE cashback_fqn STRING DEFAULT FORMAT('`%s.%s.cashback`', project_id, dataset_id);
DECLARE pedidos_fqn STRING DEFAULT FORMAT('`%s.%s.%s`', project_id, dataset_id, pedidos_table);
DECLARE comission_details_v2_fqn STRING DEFAULT FORMAT('`%s.%s.comission_details_v2`', project_id, dataset_id);
DECLARE comission_v2_fqn STRING DEFAULT FORMAT('`%s.%s.comission_v2`', project_id, dataset_id);

-- Try to inherit quarter metadata from current cashback table.
BEGIN
  EXECUTE IMMEDIATE FORMAT(
    "SELECT COALESCE(MAX(quarter_id), ''), MAX(quarter_start), MAX(quarter_end) FROM %s",
    cashback_fqn
  ) INTO quarter_id, quarter_start, quarter_end;

  IF quarter_id = '' OR quarter_start IS NULL OR quarter_end IS NULL THEN
    SET quarter_id = fallback_qid;
    SET quarter_start = fallback_q_start;
    SET quarter_end = fallback_q_end;
  END IF;
EXCEPTION WHEN ERROR THEN
  SET quarter_id = fallback_qid;
  SET quarter_start = fallback_q_start;
  SET quarter_end = fallback_q_end;
END;

-- ---------------------------------------------------------------------
-- Step 1) Build commission details V2
-- ---------------------------------------------------------------------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE %s
CLUSTER BY store_prefix, vendedor_id, cliente_cpf_norm
AS
WITH
  RankedPurchases AS (
    SELECT
      uuid,
      pedido_id,
      timestamp,
      pedido_dia,
      pedido_numero,
      store_prefix,
      cliente_nome,
      cliente_cpf,
      cliente_email,
      vendedor_nome,
      vendedor_id,
      pedido_valor,
      pedido_pontos,
      NULLIF(REGEXP_REPLACE(TRIM(cliente_cpf), r'\\D', ''), '') AS cliente_cpf_norm,
      ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY timestamp DESC) AS rn
    FROM %s
    WHERE pedido_dia BETWEEN DATE '%s' AND DATE '%s'
  ),
  MergedClients AS (
    SELECT
      cliente_cpf,
      cliente_nome,
      cliente_email,
      NULLIF(REGEXP_REPLACE(TRIM(cliente_cpf), r'\\D', ''), '') AS cliente_cpf_norm,
      ROW_NUMBER() OVER (
        PARTITION BY NULLIF(REGEXP_REPLACE(TRIM(cliente_cpf), r'\\D', ''), '')
        ORDER BY timestamp DESC
      ) AS rn
    FROM %s
  ),
  ClientTiers AS (
    SELECT
      cliente_cpf_norm,
      tier
    FROM %s
  )
SELECT
  '%s' AS quarter_id,
  DATE '%s' AS quarter_start,
  DATE '%s' AS quarter_end,
  rp.uuid,
  rp.pedido_id,
  rp.timestamp,
  rp.pedido_dia,
  rp.pedido_numero,
  rp.store_prefix,
  mc.cliente_nome,
  mc.cliente_cpf,
  mc.cliente_cpf_norm,
  mc.cliente_email,
  rp.vendedor_nome,
  rp.vendedor_id,
  rp.pedido_valor,
  rp.pedido_pontos,
  ct.tier,
  CASE
    WHEN ct.tier IN ('Top1', 'Top3', 'Top5', 'Top10', 'Platina', 'Ouro') THEN rp.pedido_valor * 0.02
    ELSE rp.pedido_valor * 0.01
  END AS pedido_comission
FROM RankedPurchases rp
JOIN MergedClients mc
  ON rp.cliente_cpf_norm = mc.cliente_cpf_norm
LEFT JOIN ClientTiers ct
  ON rp.cliente_cpf_norm = ct.cliente_cpf_norm
WHERE rp.rn = 1
  AND mc.rn = 1
  AND rp.cliente_cpf_norm IS NOT NULL
""",
comission_details_v2_fqn,
pedidos_fqn,
CAST(quarter_start AS STRING),
CAST(quarter_end AS STRING),
pedidos_fqn,
cashback_fqn,
quarter_id,
CAST(quarter_start AS STRING),
CAST(quarter_end AS STRING)
);

-- ---------------------------------------------------------------------
-- Step 2) Build commission payout V2
-- ---------------------------------------------------------------------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE %s
OPTIONS (
  description = 'Commission V2 by store/seller for current cashback quarter (2%% for Top/Platina/Ouro, 1%% others)'
) AS
WITH
  RecipientSplit AS (
    SELECT * FROM UNNEST([
      STRUCT('RH' AS nome,        'OH:RH'  AS id, CAST(0.05 AS NUMERIC) AS pct),
      STRUCT('Financeiro' AS nome,'OH:FIN' AS id, CAST(0.05 AS NUMERIC) AS pct),
      STRUCT('Gerente' AS nome,   'OH:GER' AS id, CAST(0.20 AS NUMERIC) AS pct)
    ])
  ),
  OverheadPct AS (
    SELECT COALESCE(SUM(pct), 0) AS overhead_pct
    FROM RecipientSplit
  ),
  ConfigValidated AS (
    SELECT
      CASE
        WHEN overhead_pct < 0 OR overhead_pct > 1 THEN ERROR(
          FORMAT('Invalid overhead split: overhead_pct=%%f. Expected between 0 and 1.', CAST(overhead_pct AS FLOAT64))
        )
        ELSE overhead_pct
      END AS overhead_pct
    FROM OverheadPct
  ),
  VendorSales AS (
    SELECT
      quarter_id,
      quarter_start,
      quarter_end,
      store_prefix,
      vendedor_nome,
      vendedor_id,
      SUM(CAST(pedido_valor AS NUMERIC)) AS revenue,
      SUM(CAST(pedido_comission AS NUMERIC)) AS commission
    FROM %s
    GROUP BY quarter_id, quarter_start, quarter_end, store_prefix, vendedor_nome, vendedor_id
  ),
  Totals AS (
    SELECT
      quarter_id,
      quarter_start,
      quarter_end,
      store_prefix,
      COALESCE(SUM(commission), 0) AS total_commission
    FROM VendorSales
    GROUP BY quarter_id, quarter_start, quarter_end, store_prefix
  ),
  Shares AS (
    SELECT
      t.quarter_id,
      t.quarter_start,
      t.quarter_end,
      t.store_prefix,
      t.total_commission,
      CAST(1 AS NUMERIC) - cv.overhead_pct AS seller_share
    FROM Totals t
    CROSS JOIN ConfigValidated cv
  ),
  SellerAdjusted AS (
    SELECT
      vs.quarter_id,
      vs.quarter_start,
      vs.quarter_end,
      vs.store_prefix,
      vs.vendedor_nome,
      vs.vendedor_id,
      vs.revenue,
      vs.commission * sh.seller_share AS pre_commission
    FROM VendorSales vs
    JOIN Shares sh
      ON sh.store_prefix = vs.store_prefix
     AND sh.quarter_id = vs.quarter_id
  ),
  OverheadRows AS (
    SELECT
      sh.quarter_id,
      sh.quarter_start,
      sh.quarter_end,
      sh.store_prefix,
      rs.nome AS vendedor_nome,
      CONCAT(sh.store_prefix, ':', rs.id) AS vendedor_id,
      CAST(0 AS NUMERIC) AS revenue,
      sh.total_commission * rs.pct AS pre_commission
    FROM RecipientSplit rs
    CROSS JOIN Shares sh
  ),
  AllRows AS (
    SELECT * FROM SellerAdjusted
    UNION ALL
    SELECT * FROM OverheadRows
  ),
  WithCents AS (
    SELECT
      quarter_id,
      quarter_start,
      quarter_end,
      store_prefix,
      vendedor_nome,
      vendedor_id,
      revenue,
      pre_commission,
      pre_commission * 100 AS ideal_cents
    FROM AllRows
  ),
  FloorParts AS (
    SELECT
      quarter_id,
      quarter_start,
      quarter_end,
      store_prefix,
      vendedor_nome,
      vendedor_id,
      revenue,
      ideal_cents,
      CAST(FLOOR(ideal_cents) AS INT64) AS floor_cents,
      (ideal_cents - FLOOR(ideal_cents)) AS frac
    FROM WithCents
  ),
  TotalsCents AS (
    SELECT
      quarter_id,
      quarter_start,
      quarter_end,
      store_prefix,
      CAST(ROUND(total_commission, 2) * 100 AS INT64) AS target_cents
    FROM Shares
  ),
  SumFloor AS (
    SELECT
      quarter_id,
      quarter_start,
      quarter_end,
      store_prefix,
      SUM(floor_cents) AS sum_floor_cents
    FROM FloorParts
    GROUP BY quarter_id, quarter_start, quarter_end, store_prefix
  ),
  Ranked AS (
    SELECT
      fp.*,
      (tc.target_cents - sf.sum_floor_cents) AS remaining_cents,
      ROW_NUMBER() OVER (
        PARTITION BY fp.quarter_id, fp.store_prefix
        ORDER BY frac DESC, ideal_cents DESC, vendedor_id, vendedor_nome
      ) AS rn
    FROM FloorParts fp
    JOIN TotalsCents tc
      ON tc.quarter_id = fp.quarter_id
     AND tc.store_prefix = fp.store_prefix
    JOIN SumFloor sf
      ON sf.quarter_id = fp.quarter_id
     AND sf.store_prefix = fp.store_prefix
  )
SELECT
  quarter_id,
  quarter_start,
  quarter_end,
  store_prefix,
  vendedor_nome,
  vendedor_id,
  revenue,
  CAST((floor_cents + CASE WHEN rn <= remaining_cents THEN 1 ELSE 0 END) AS NUMERIC) / 100 AS comission
FROM Ranked
""",
comission_v2_fqn,
comission_details_v2_fqn
);
