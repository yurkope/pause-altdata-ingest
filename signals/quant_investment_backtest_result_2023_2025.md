# Quant investment backtest result — 2023–2025

Status: **PROMISING / NOT YET DECISION-GRADE**

The Quant methodology and ticker-policy exposure map were frozen before market returns were fetched for this test. A first implementation incorrectly entered at calendar quarter-end; that result is discarded because LDA quarterly filings are not available then. The accepted point-in-time run enters on the first trading day on/after **quarter-end + 20 calendar days**, reflecting the statutory LDA filing lag.

## Primary specification

- Signal: quarter-over-quarter change in Quant economic pressure score.
- Universe: 14 mapped policies / 56 primary tickers from the pre-existing frozen exposure map.
- Selection: policies in the top quartile of pressure change within each signal quarter.
- Portfolio: equal-weight mapped primary tickers.
- Benchmark: SPY.
- Primary horizon: 90 calendar days after the point-in-time availability date.
- Signal quarters: 9 (Q2–Q4 of 2023, 2024 and 2025).
- Selected policy-signals: 36.

## Point-in-time-correct results

| Horizon | Mean quarter-EW excess vs SPY | t p-value | sign-flip p-value |
|---|---:|---:|---:|
| 30d | +2.96% | 0.0737 | 0.1126 |
| 60d | +4.76% | 0.0570 | 0.0544 |
| **90d** | **+6.04%** | **0.0108** | **0.0160** |
| 180d | +10.30% | 0.0305 | 0.0275 |

At 90 days the ticker-level mean return was 7.68% versus 2.06% for SPY; mean ticker-level excess was +5.63%. Quarter-equal-weight excess was +6.04%.

### 90-day quarter portfolios

| Signal quarter | Portfolio | SPY | Excess |
|---|---:|---:|---:|
| 2023 Q2 | -7.46% | -4.52% | -2.94% |
| 2023 Q3 | +26.73% | +13.59% | +13.14% |
| 2023 Q4 | +8.74% | +2.74% | +6.00% |
| 2024 Q2 | +11.94% | +5.72% | +6.22% |
| 2024 Q3 | +11.84% | +3.68% | +8.17% |
| 2024 Q4 | -0.32% | -14.53% | +14.21% |
| 2025 Q2 | +8.98% | +7.06% | +1.92% |
| 2025 Q3 | +7.35% | +1.23% | +6.11% |
| 2025 Q4 | +6.37% | +4.88% | +1.49% |

Eight of nine 90-day signal quarters produced positive excess returns versus SPY.

## Interpretation / limitations

This is the first direct evidence in the project that Quant pressure acceleration may contain investable information. It is **not yet proof of an investable strategy**.

Important limitations:
1. only nine independent-ish signal quarters;
2. the ticker-policy map was frozen before this return test but was constructed in 2026, so it is not a historically contemporaneous security universe and may contain survivorship/selection hindsight;
3. ticker observations within a policy/quarter are correlated, so the quarter-level result is more meaningful than treating all ticker rows as independent;
4. SPY is the primary preregistered benchmark; sector-neutral robustness is still needed;
5. transaction costs, turnover, capacity and realistic portfolio construction are not yet modeled;
6. Q1 signals are unavailable because the current historical reconstruction begins in 2023 and does not have 2022 Q4 for a clean within-year delta.

No Quant weights or ticker mappings were changed in response to these results.

Accepted run: GitHub Actions 44156486928.
