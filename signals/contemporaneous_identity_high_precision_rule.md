# High-precision contemporaneous company identity rule — frozen before rerun

This rule tightens the preregistered LDA-client to SEC-company identity match after QA identified deterministic false-positive risk. It is defined without consulting security returns under the filtered universe.

A company identity is accepted only when all conditions hold:
1. normalized LDA client legal name has exactly one normalized match in the SEC company-ticker reference;
2. normalized name length is at least 5 characters;
3. ticker is a standard common-equity-like symbol matching ^[A-Z]{1,5}([.-][A-Z])?$;
4. the LDA client has contemporaneous substantive-policy evidence in the signal quarter;
5. ticker has price observations at both required endpoints under the unchanged +20-day / 90-day rule.

Rejected/ambiguous identities remain excluded. No manual aliases are permitted in this test.

This does NOT solve historical ticker changes/delistings. Results therefore remain a survivorship-sensitive validation layer, not a fully point-in-time CRSP-equivalent universe.

The signal definition, top-quartile policy rule, weighting, benchmark, timing, and horizon remain unchanged.
