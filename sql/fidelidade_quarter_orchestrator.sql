-- Emporio Zingaro Fidelidade | Quarter-close orchestrator (BigQuery Standard SQL script)
-- Run this script once to execute in order:
--   1) cashback (global ranking, archived previous table)
--   2) comission_details (store-safe, no pedido_cashback column)
--   3) comission (store-safe payout with overhead validation + cent-perfect rounding)

DECLARE project_id STRING DEFAULT 'emporio-zingaro';
DECLARE dataset_id STRING DEFAULT 'z316_fidelidade';
DECLARE pedidos_table STRING DEFAULT 'pedidos';

DECLARE run_date DATE DEFAULT CURRENT_DATE('America/Sao_Paulo');
DECLARE target_q_start DATE DEFAULT DATE_TRUNC(DATE_SUB(run_date, INTERVAL 1 QUARTER), QUARTER);
DECLARE target_q_end DATE DEFAULT DATE_SUB(DATE_ADD(target_q_start, INTERVAL 1 QUARTER), INTERVAL 1 DAY);
DECLARE prev_prev_q_start DATE DEFAULT DATE_TRUNC(DATE_SUB(run_date, INTERVAL 2 QUARTER), QUARTER);

DECLARE target_qid STRING DEFAULT CONCAT(FORMAT_DATE('%y', target_q_start), 'Q', CAST(EXTRACT(QUARTER FROM target_q_start) AS STRING));
DECLARE prev_prev_qid STRING DEFAULT CONCAT(FORMAT_DATE('%y', prev_prev_q_start), 'Q', CAST(EXTRACT(QUARTER FROM prev_prev_q_start) AS STRING));
DECLARE existing_qid STRING DEFAULT '';
DECLARE archive_qid STRING DEFAULT '';

DECLARE cashback_exists BOOL DEFAULT FALSE;

DECLARE pedidos_fqn STRING DEFAULT FORMAT('`%s.%s.%s`', project_id, dataset_id, pedidos_table);
DECLARE cashback_fqn STRING DEFAULT FORMAT('`%s.%s.cashback`', project_id, dataset_id);
DECLARE comission_details_fqn STRING DEFAULT FORMAT('`%s.%s.comission_details`', project_id, dataset_id);
DECLARE comission_fqn STRING DEFAULT FORMAT('`%s.%s.comission`', project_id, dataset_id);

-- ---------------------------------------------------------------------
-- Step 0) Archive current cashback table to quarterly snapshot
-- ---------------------------------------------------------------------
EXECUTE IMMEDIATE FORMAT(
  "SELECT COUNT(*) > 0 FROM `%s.%s.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'cashback'",
  project_id,
  dataset_id
) INTO cashback_exists;

IF cashback_exists THEN
  BEGIN
    EXECUTE IMMEDIATE FORMAT(
      "SELECT COALESCE(MAX(quarter_id), '') FROM %s",
      cashback_fqn
    ) INTO existing_qid;
  EXCEPTION WHEN ERROR THEN
    SET existing_qid = '';
  END;

  SET archive_qid = IF(existing_qid = '', prev_prev_qid, existing_qid);

  EXECUTE IMMEDIATE FORMAT(
    "CREATE OR REPLACE TABLE `%s.%s.cashback_%s` AS SELECT * FROM %s",
    project_id,
    dataset_id,
    archive_qid,
    cashback_fqn
  );
END IF;

-- ---------------------------------------------------------------------
-- Step 1) Build current cashback table for last closed quarter
-- ---------------------------------------------------------------------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE %s
OPTIONS (
  description = 'Current cashback ranking for the most recently closed quarter (global clients, CPF-normalized)'
) AS
WITH
  RankedPurchases AS (
    SELECT
      *,
      NULLIF(REGEXP_REPLACE(TRIM(cliente_cpf), r'\\D', ''), '') AS cliente_cpf_norm,
      ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY timestamp DESC) AS rn
    FROM %s
    WHERE pedido_dia BETWEEN DATE '%s' AND DATE '%s'
  ),
  MergedClients AS (
    SELECT
      cliente_nome,
      cliente_cpf,
      cliente_email,
      NULLIF(REGEXP_REPLACE(TRIM(cliente_cpf), r'\\D', ''), '') AS cliente_cpf_norm,
      ROW_NUMBER() OVER (
        PARTITION BY NULLIF(REGEXP_REPLACE(TRIM(cliente_cpf), r'\\D', ''), '')
        ORDER BY timestamp DESC
      ) AS rn
    FROM %s
  ),
  FilteredPurchases AS (
    SELECT
      mc.cliente_nome,
      mc.cliente_cpf,
      mc.cliente_email,
      mc.cliente_cpf_norm,
      rp.pedido_pontos,
      rp.pedido_valor
    FROM RankedPurchases rp
    JOIN MergedClients mc
      ON rp.cliente_cpf_norm = mc.cliente_cpf_norm
    WHERE rp.rn = 1
      AND mc.rn = 1
      AND rp.cliente_cpf_norm IS NOT NULL
  ),
  ClientPointsAndSpend AS (
    SELECT
      cliente_nome,
      cliente_cpf,
      cliente_email,
      cliente_cpf_norm,
      SUM(pedido_pontos) AS points,
      SUM(pedido_valor) AS spend
    FROM FilteredPurchases
    GROUP BY cliente_nome, cliente_cpf, cliente_email, cliente_cpf_norm
  ),
  TotalPoints AS (
    SELECT SUM(points) AS total_points
    FROM ClientPointsAndSpend
  ),
  CumulativePoints AS (
    SELECT
      cliente_nome,
      cliente_cpf,
      cliente_email,
      cliente_cpf_norm,
      points,
      spend,
      ROW_NUMBER() OVER (ORDER BY points DESC, spend DESC) AS rn,
      SAFE_DIVIDE(
        SUM(points) OVER (ORDER BY points DESC, spend DESC),
        (SELECT total_points FROM TotalPoints)
      ) * 100 AS cumulative_percent
    FROM ClientPointsAndSpend
  ),
  TieredClients AS (
    SELECT
      cliente_nome,
      cliente_cpf,
      cliente_email,
      cliente_cpf_norm,
      points,
      spend,
      rn,
      CASE
        WHEN rn = 1 THEN 'Top1'
        WHEN rn BETWEEN 2 AND 3 THEN 'Top3'
        WHEN rn BETWEEN 4 AND 5 THEN 'Top5'
        WHEN rn BETWEEN 6 AND 10 THEN 'Top10'
        WHEN cumulative_percent <= 40 THEN 'Platina'
        WHEN cumulative_percent > 40 AND cumulative_percent <= 70 THEN 'Ouro'
        WHEN cumulative_percent > 70 AND cumulative_percent <= 90 THEN 'Prata'
        ELSE 'Bronze'
      END AS tier
    FROM CumulativePoints
  )
SELECT
  '%s' AS quarter_id,
  DATE '%s' AS quarter_start,
  DATE '%s' AS quarter_end,
  rn,
  cliente_nome,
  cliente_cpf,
  cliente_cpf_norm,
  cliente_email,
  points,
  spend,
  tier,
  CASE
    WHEN tier IN ('Top1', 'Top3', 'Top5', 'Top10') THEN spend *
      CASE tier
        WHEN 'Top1' THEN 0.20
        WHEN 'Top3' THEN 0.15
        WHEN 'Top5' THEN 0.10
        WHEN 'Top10' THEN 0.07
      END
    ELSE GREATEST(
      CASE tier
        WHEN 'Platina' THEN spend * 0.05
        WHEN 'Ouro' THEN spend * 0.04
        WHEN 'Prata' THEN spend * 0.03
        ELSE spend * 0.02
      END,
      CASE tier
        WHEN 'Platina' THEN 10
        WHEN 'Ouro' THEN 7
        WHEN 'Prata' THEN 5
        ELSE 3
      END
    )
  END AS cashback
FROM TieredClients
ORDER BY rn
""",
cashback_fqn,
pedidos_fqn,
CAST(target_q_start AS STRING),
CAST(target_q_end AS STRING),
pedidos_fqn,
target_qid,
CAST(target_q_start AS STRING),
CAST(target_q_end AS STRING)
);

-- ---------------------------------------------------------------------
-- Step 2) Build commission details for the same quarter
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
    WHEN ct.tier IN ('Top1', 'Top3', 'Top5', 'Top10') THEN rp.pedido_valor * 0.01
    WHEN ct.tier = 'Platina' THEN rp.pedido_valor * 0.01
    WHEN ct.tier = 'Ouro' THEN rp.pedido_valor * 0.02
    WHEN ct.tier = 'Prata' THEN rp.pedido_valor * 0.03
    ELSE rp.pedido_valor * 0.04
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
comission_details_fqn,
pedidos_fqn,
CAST(target_q_start AS STRING),
CAST(target_q_end AS STRING),
pedidos_fqn,
cashback_fqn,
target_qid,
CAST(target_q_start AS STRING),
CAST(target_q_end AS STRING)
);

-- ---------------------------------------------------------------------
-- Step 3) Build commission payout table
-- ---------------------------------------------------------------------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE %s
OPTIONS (
  description = 'Current commissions by store/seller for most recently closed quarter, with validated overhead split'
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
comission_fqn,
comission_details_fqn
);
