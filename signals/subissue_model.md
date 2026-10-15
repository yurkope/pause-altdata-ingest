# Canonical sub-issue model

The LDA general issue code is a parent category, not the final signal grain.

Target hierarchy:

```
LDA issue code -> canonical sub-issue -> specific policy/bill/program/entity
```

## Why

Broad categories such as BUD, HCR, TAX, DEF and TRD contain structurally high
activity. Ranking those categories mostly identifies large functions of
government. The research signal should instead detect pressure and acceleration
inside those categories.

## Evidence from 2025 Q4

Descriptions are sufficiently specific to recover meaningful sub-issues. Initial
deterministic probes found examples including:

- BUD: Community Project Funding / Congressionally Directed Spending,
  Transportation/HUD appropriations, Energy & Water appropriations,
  Interior/Environment appropriations, Defense appropriations.
- HCR: Medicare/CMS payment, Medicaid, 340B, drug pricing/pharmaceuticals,
  No Surprises/billing.
- TAX: corporate tax, international tax, energy tax/credits, R&D tax,
  digital-asset tax.
- DEF: NDAA, defense appropriations, acquisition/procurement.
- TRD: tariffs, export controls, China trade.

These are discovery labels, not a finished taxonomy.

## Classification design

Use a hybrid classifier rather than a single keyword list:

1. Extract explicit anchors first: bill/public-law numbers, named programs,
   agencies, appropriations accounts and recognizable policy terms.
2. Map high-confidence anchors to canonical sub-issues deterministically.
3. Classify remaining descriptions semantically against the controlled taxonomy.
4. Allow multiple sub-issues per activity when the text genuinely covers more
   than one policy.
5. Store classification method, confidence and evidence text so every mapping is
   auditable.
6. Keep UNKNOWN/OTHER rather than forcing low-confidence descriptions into a
   category.

## Spend

Do not assign the full filing amount to every sub-issue. First deduplicate
sub-issues within a filing, then allocate the disclosed filing amount across the
distinct mapped sub-issues. Preserve the original LDA parent issue alongside the
canonical sub-issue.

## Signals

At sub-issue x quarter:

- allocated_spend
- distinct_clients
- distinct_filings
- distinct_lobbyists
- government_entity_reach
- government_link_intensity
- concentration (top-client share / HHI)
- novelty (new-client share)
- pressure_score

After multiple comparable quarters are loaded:

- spend acceleration
- client-breadth acceleration
- agency-reach acceleration
- new-client acceleration
- composite policy acceleration

Do not use filing `dt_posted` as lobbying-event time. Filing deadlines create
large artificial posting spikes. Momentum/acceleration should compare equivalent
reporting periods unless a better underlying activity date becomes available.
