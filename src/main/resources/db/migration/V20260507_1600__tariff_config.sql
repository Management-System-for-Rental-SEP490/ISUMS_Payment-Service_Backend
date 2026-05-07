-- Versioned tariff configuration (electricity / water).
--
-- Composite key: (metric, plan, region) identifies a tariff stream.
-- Active row per stream: WHERE expired_at IS NULL ORDER BY effective_from DESC LIMIT 1.
--
-- Mutation rules:
--   - INSERT new row → previously active row of same (metric,plan,region) gets expired_at=now()
--   - NEVER UPDATE config_json once published
--   - Seeded rows use all-zeros UUID as created_by (system marker)
--
-- Why JSON column: tier list is heterogeneous (electricity has 6 tiers,
-- water has 4) and tariff metadata (citations, surcharges) varies by region.
-- JSONB lets admin diff/inspect versions easily.

CREATE TABLE IF NOT EXISTS tariff_config_version (
    id              uuid        PRIMARY KEY,
    metric          varchar(40) NOT NULL,
    plan            varchar(40) NOT NULL,
    region          varchar(40) NOT NULL,
    version         varchar(80) NOT NULL,
    config_json     jsonb       NOT NULL,
    effective_from  timestamptz NOT NULL DEFAULT now(),
    expired_at      timestamptz,
    notes           varchar(1000),
    created_by      uuid        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expired_by      uuid,

    CONSTRAINT chk_tariff_dates CHECK (expired_at IS NULL OR expired_at > effective_from)
);

CREATE INDEX IF NOT EXISTS idx_tariff_active
    ON tariff_config_version (metric, plan, region, effective_from DESC)
    WHERE expired_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_tariff_history
    ON tariff_config_version (metric, plan, region, effective_from DESC);

INSERT INTO tariff_config_version (id, metric, plan, region, version, config_json, created_by, notes)
VALUES
('e1ec0000-0000-0000-0000-000000000001',
 'electricity', 'residential', 'VN',
 'evn-residential-2024-10',
 '{
    "metric":"electricity","plan":"residential","region":"VN",
    "currency":"VND","unit":"kWh",
    "tiers":[
      {"index":1,"label":"Bậc 1 (0–50 kWh)","fromUnit":0,"toUnit":50,"pricePerUnitVnd":1984},
      {"index":2,"label":"Bậc 2 (51–100 kWh)","fromUnit":50,"toUnit":100,"pricePerUnitVnd":2050},
      {"index":3,"label":"Bậc 3 (101–200 kWh)","fromUnit":100,"toUnit":200,"pricePerUnitVnd":2380},
      {"index":4,"label":"Bậc 4 (201–300 kWh)","fromUnit":200,"toUnit":300,"pricePerUnitVnd":2998},
      {"index":5,"label":"Bậc 5 (301–400 kWh)","fromUnit":300,"toUnit":400,"pricePerUnitVnd":3350},
      {"index":6,"label":"Bậc 6 (>400 kWh)","fromUnit":400,"toUnit":null,"pricePerUnitVnd":3460}
    ],
    "vatRate":0.08,
    "surchargeRate":0,
    "surchargeLabel":null,
    "source":"Quyết định 2699/QĐ-BCT 2024 — Tập đoàn Điện lực Việt Nam (EVN), giá bán lẻ điện sinh hoạt 6 bậc",
    "effectiveFrom":"2024-10-11",
    "version":"evn-residential-2024-10",
    "notes":"Áp dụng từ 11/10/2024. VAT 8% theo Nghị quyết 142/2024/QH15."
 }'::jsonb,
 '00000000-0000-0000-0000-000000000000',
 'EVN residential 2024-10'),

('e1ec0000-0000-0000-0000-000000000002',
 'water', 'residential', 'HCM',
 'sawaco-residential-2022',
 '{
    "metric":"water","plan":"residential","region":"HCM",
    "currency":"VND","unit":"m3",
    "tiers":[
      {"index":1,"label":"Bậc 1 (0–4 m³)","fromUnit":0,"toUnit":4,"pricePerUnitVnd":6300},
      {"index":2,"label":"Bậc 2 (5–6 m³)","fromUnit":4,"toUnit":6,"pricePerUnitVnd":12200},
      {"index":3,"label":"Bậc 3 (7–10 m³)","fromUnit":6,"toUnit":10,"pricePerUnitVnd":14400},
      {"index":4,"label":"Bậc 4 (>10 m³)","fromUnit":10,"toUnit":null,"pricePerUnitVnd":16500}
    ],
    "vatRate":0.05,
    "surchargeRate":0.10,
    "surchargeLabel":"Phí bảo vệ môi trường",
    "source":"Quyết định 25/2019/QĐ-UBND TP. Hồ Chí Minh — Tổng công ty Cấp nước Sài Gòn (SAWACO)",
    "effectiveFrom":"2022-01-01",
    "version":"sawaco-residential-2022",
    "notes":"Đơn giá nước sinh hoạt 4 bậc. VAT 5% (NQ 43/2022/QH15) + Phí BVMT 10% (NĐ 53/2020/NĐ-CP)."
 }'::jsonb,
 '00000000-0000-0000-0000-000000000000',
 'SAWACO HCMC residential 2022')
ON CONFLICT (id) DO NOTHING;
