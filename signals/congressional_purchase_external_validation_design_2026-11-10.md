# Congressional Purchases External Validation Design — 2026-11-10

## Purpose

Test whether Quant policy-pressure signals correlate with subsequent or contemporaneous securities
purchases disclosed by members of Congress. Congressional trades are an OUTCOME/VALIDATION layer,
not an input to v3.

## Timing

Use transaction_date, never filing/disclosure date, as the economic event date.
Retain filed_date and disclosure_lag_days for audit and point-in-time availability tests.

Run separately:
- same-quarter pressure vs buys;
- prior-quarter pressure -> next-quarter buys;
- pressure change vs same/next-quarter buys;
- 30/60/90-day event windows when daily policy signal is available.

## Trade filters

Primary test:
- purchases only;
- exchange-listed equities;
- member/self ownership separately from spouse/joint/dependent;
- deduplicate repeated parsed rows by filing/member/ticker/date/action/owner;
- retain disclosed amount range; do not invent exact transaction value.

Secondary tests:
- all beneficial-owner purchases;
- minimum disclosed amount weighting;
- unique-member counts rather than raw transaction counts.

## Exposure mapping

Ticker -> policy exposure must be frozen BEFORE looking at correlation results.
A ticker may map to multiple policies only when the economic exposure is defensible.
Report sensitivity with one-policy-only mappings.

Initial research buckets include:
AI_POLICY, CYBERSECURITY, DIGITAL_ASSETS, DEFENSE_APPROPRIATIONS, FAA_AVIATION,
BANK_CAPITAL, BROADBAND_BEAD, CLEAN_ENERGY_TAX_IRA, CRITICAL_MINERALS_SUPPLY_CHAIN,
DRUG_PRICING, PBM, AUTONOMOUS_VEHICLES, PATENT_POLICY, TRADE_TARIFFS.

## Preliminary sample result

A public normalized feed was used only for a feasibility pass. It exposed 175 2025 purchase rows,
which is too small/incomplete for a decision-grade conclusion.

On 56 policy-quarter observations from the initial exposure map:
- pressure level vs buy count: Pearson r approximately -0.02;
- quarter-over-quarter pressure change vs same-quarter buy count: approximately +0.22;
- prior-quarter pressure level vs next-quarter buy count: approximately -0.01.

Interpretation: no demonstrated relationship yet. The +0.22 change relationship is exploratory only.

Examples observed in the sample include Q4 purchases of NVDA/AMZN/GOOGL exposure while AI_POLICY
pressure was high, and a Q4 BTC purchase while DIGITAL_ASSETS pressure was elevated. These are
examples, not statistical evidence.

## Required next gate

Do not draw a correlation conclusion until a substantially more complete House + Senate STOCK Act
transaction history is acquired and coverage is audited against primary-source filing counts.

Then run:
1. frozen ticker-policy map;
2. deduplication and owner segmentation;
3. null/control ticker portfolios;
4. permutation test within quarter;
5. Pearson + Spearman correlations;
6. count regression (Poisson/negative binomial as appropriate);
7. member fixed effects if sample size permits;
8. lag tests;
9. multiple-testing correction.

No congressional trade data should be used to train or tune v3 before this external validation is
locked.


## Data-source decision

For the decision-grade test, prefer a full government-derived House + Senate history with
transaction-level provenance rather than the small feasibility feed.

Candidate research sources verified 2026-11-10:
- LuxAlgo/market-trackers-data: CC0 full snapshots parsed from Senate eFD + House Clerk, with a
  primary-source provenance URL on every row and manifest/source-health metadata.
- DataDawn openregulations-public: government-source stock_trades corpus reporting 46K+ parsed House
  transactions and 15K+ Senate transactions from 2012-present, with bioguide linkage and source URLs.
- House Clerk remains the primary source for House coverage auditing.

POC rule: keep raw transaction history off Neon unless a compact derived validation table is later
justified. Neon is near its storage ceiling.

## Pre-registration / anti-overfit rule

Before running the full correlation:
1. freeze ticker-policy exposure map;
2. freeze ownership and transaction filters;
3. freeze timing windows and primary statistic;
4. designate all other tests secondary/exploratory.

Primary hypothesis:
An increase in Quant economic policy pressure is positively associated with the subsequent rate of
purchases in economically exposed public companies.

Primary independent variable:
quarter-over-quarter change in economic_pressure_score (or the equivalent v2 economic score for the
2025 validation period).

Primary outcome:
unique-member purchase count in exposed tickers during the subsequent quarter.

Primary robustness outcomes:
raw purchase count and lower-bound disclosed purchase dollars.

Primary test:
Spearman rank correlation plus a count model; Pearson correlation is descriptive.

This external validation must not be used to tune policy-pressure weights, ticker exposure mappings,
or evidence-family definitions after results are observed.
