# Canonical policy taxonomy v1

This is a controlled, deterministic first-pass taxonomy for rolling related bill anchors into common policy signals.

## Principles
- Canonical policy is broader than a bill but narrower than an LDA general issue.
- A bill may map to multiple subissues when evidence genuinely supports it.
- Mapping evidence and method must remain inspectable.
- Unmatched bills remain UNKNOWN/OTHER; coverage is never improved by forced classification.
- Bill-title anchors are the first deterministic layer. Activity-text/program/agency anchors are the next layer.
- Financial amounts are filing-level disclosures. Canonical-policy amounts are analytical allocations, never direct spending on a bill, committee, or policy.

## Initial controlled labels
| Code | Canonical policy / subissue | Initial deterministic bill anchors |
|---|---|---|
| PERMITTING_ENV_REVIEW | Permitting & environmental review | permitting, NEPA, environmental/deployment review, regulatory clarity |
| FORESTS_WILDFIRE | Forests & wildfire | forest, wildfire, sequoia |
| ONLINE_SAFETY_PRIVACY | Online safety & children's privacy | Kids Online Safety, online privacy, children/teens, Parents Over Platforms, COPPA |
| AI_POLICY | Artificial intelligence policy | AI / artificial intelligence |
| HEALTHCARE_COSTS_PBM | Healthcare costs & PBMs | premiums, healthcare costs, PBM/pharmacy benefit, copays |
| DEFENSE_AUTHORIZATION | Defense authorization | NDAA / National Defense Authorization |
| DEFENSE_APPROPRIATIONS | Defense appropriations | Department of Defense appropriations |
| APPROPRIATIONS_OTHER | Other appropriations | appropriations / continuing appropriations not captured above |
| DIGITAL_ASSETS | Digital assets | digital assets, stablecoins, GENIUS Act, crypto |
| DRUG_BIOMEDICAL_POLICY | Drug & biomedical policy | FDA, orphan cures, Give Kids a Chance, drug/pharmaceutical |
| HOUSING | Housing policy | housing |
| AVIATION | Aviation policy | aviation / FAA |

This v1 intentionally leaves many bills unmapped. It is a seed taxonomy, not a claim of exhaustive policy coverage.
