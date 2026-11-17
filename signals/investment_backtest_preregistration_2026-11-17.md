# Quant investment backtest preregistration — 2026-11-17

## Frozen inputs

- Quant methodology is frozen by the passing 2025 replay gate.
- 2023 and 2024 historical Quant scores were reconstructed outcome-blind.
- Company-policy exposure universe is frozen to `signals/congress_trade_policy_exposure_map_v1.csv` at this commit; only rows with `include_primary=true` are primary.
- No ticker-policy mapping may be added, removed, or re-tiered after return results are observed for this test.

## Primary signal

Quarter-over-quarter change in `economic_pressure_score` (Quant pressure acceleration).

## Primary portfolio test

At each available quarter-end, rank mapped substantive policies by pressure-score change. Map the top quartile of policy changes to their frozen primary tickers and form an equal-weight ticker portfolio. Compare forward total price return with SPY over 30, 60, 90 and 180 calendar-day horizons using the first trading close on/after the start date and first trading close on/after each horizon endpoint.

Primary horizon: **90 days**. Primary statistic: mean ticker-level excess return versus SPY, with a policy-quarter cluster-aware/permutation robustness test where feasible.

## Secondary tests

- pressure level;
- emergence / transition into NEW_EMERGING or ACCELERATING where available;
- top-half vs bottom-half mapped policy acceleration;
- sector-adjusted returns when a frozen, defensible sector benchmark mapping is available without outcome inspection;
- 30/60/180-day horizons.

All secondary results are exploratory. No Quant weights or mappings may be tuned to improve this backtest.
