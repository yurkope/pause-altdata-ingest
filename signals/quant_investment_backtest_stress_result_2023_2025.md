# Quant investment backtest adversarial stress test — 2023–2025

Accepted stress-test run: **44158639135**. This suite keeps the frozen Quant scores, frozen primary ticker-policy map, top-quartile acceleration rule, 90-day horizon and quarter-end + 20-day information lag unchanged.

## Results

- Top-quartile acceleration vs SPY: **+6.18%** mean quarter-equal-weight excess, t-test p=0.0100 (9 signal quarters).
- Top-quartile acceleration vs sector ETFs: **+5.17%** mean excess, p=0.0129.
- Top minus bottom quartile vs SPY: **+7.73%**, p=0.0522.
- Top minus bottom quartile vs sector ETFs: **+5.33%**, p=0.1651.
- Random-policy placebo (5,000 simulations): observed +6.18%; placebo median +1.38%; placebo 95th percentile +4.14%; one-sided empirical p=0.0026.

## Leave-one-quarter-out

Removing any single signal quarter leaves mean 90-day SPY excess positive, ranging from **+5.18% to +7.32%**.

## Leave-one-policy-out

Removing any single selected policy leaves mean 90-day SPY excess positive, ranging from **+5.21% to +7.01%**. Therefore no single policy explains the aggregate result.

## Concentration caveat

Individual policy results vary materially. Critical Minerals (+23.3%), Defense Appropriations (+14.2%), Cybersecurity (+11.7%), PBM (+9.5%), Bank Capital (+8.9%) and Broadband BEAD (+8.6%) were strong. Autonomous Vehicles (-8.4%), FAA Aviation (-2.8%) and Trade Tariffs (-0.6%) were negative. AI Policy was positive (+5.2%) but is the largest ticker-observation block.

## Interpretation

The original 90-day result survives several deliberately adversarial controls:
1. sector-neutral benchmarking;
2. removal of any single quarter;
3. removal of any single policy;
4. top-versus-bottom ranking;
5. random-policy placebo portfolios.

This materially reduces the likelihood that the first result was merely an SPY/sector beta artifact or one lucky policy/quarter. It still does **not** establish production-ready alpha because the sample is nine signal quarters and the 2026-constructed ticker-policy universe can contain survivorship/selection hindsight. A historically point-in-time security universe and additional years are the highest-value next validation steps.

No Quant weights, ticker-policy mappings, selection threshold or horizon were changed in response to these stress-test outcomes.
