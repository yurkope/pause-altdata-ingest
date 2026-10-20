"""Regression fixtures for Congress mention resolution.

Run from repository root:
    python -m unittest sources.congress.test_resolution
"""
import unittest
from sources.congress.resolve_bills import PATTERN, authoritative_law_congresses, mention_resolution


def resolve(text, title, congress=119):
    m = PATTERN.search(text)
    if not m:
        raise AssertionError("fixture has no bill token")
    return mention_resolution(text, m.start(), m.end(), congress, title, m.group(0))[:3]


class MentionResolutionTests(unittest.TestCase):
    def test_authoritative_law_congress_parser(self):
        self.assertEqual(authoritative_law_congresses({"laws": [{"type": "Public Law", "number": "119-21"}]}), {119})
        self.assertEqual(authoritative_law_congresses({"laws": [{"type": "Public Law", "number": "118-63"}]}), {118})

    def test_authoritative_law_blocks_conflicting_proximity(self):
        text = "H.R. 1, One Big Beautiful Bill Act (P.L.118-63)"
        m = PATTERN.search(text)
        status, method, _ = mention_resolution(
            text, m.start(), m.end(), 119,
            "An act to provide for reconciliation pursuant to title II of H. Con. Res. 14.",
            m.group(0),
            {"laws": [{"type": "Public Law", "number": "119-21"}]},
        )[:3]
        self.assertNotEqual(status, "rejected")
        self.assertNotEqual(method, "conflicting_public_law_local")

    def test_current_public_law_verifies(self):
        self.assertEqual(resolve("H.R. 1968 (P.L.119-4), Full-Year Continuing Appropriations.", "Full-Year Continuing Appropriations and Extensions Act, 2025")[:2],
                         ("verified", "public_law_congress_local"))

    def test_hr1_current_public_law_verifies(self):
        self.assertEqual(resolve("P.L. 119-21 (H.R. 1): One Big Beautiful Bill implementation", "An act to provide for reconciliation pursuant to title II of H. Con. Res. 14.")[:2],
                         ("verified", "public_law_congress_local"))

    def test_prior_public_law_rejects(self):
        self.assertEqual(resolve("H.R. 3935, FAA Reauthorization Act of 2024 (P.L.118-63)", "FAA Reauthorization Act of 2024")[:2],
                         ("rejected", "conflicting_public_law_local"))

    def test_unrelated_prior_law_does_not_reject_current_bill(self):
        status, method, _ = resolve("P.L.118-63, FAA Reauthorization Act; S.2465, Transportation, Housing and Urban Development Appropriations Act, 2026",
                                    "Transportation, Housing and Urban Development, and Related Agencies Appropriations Act, 2026")
        self.assertEqual(status, "candidate")
        self.assertIn(method, {"title_strong_local", "title_moderate_local"})

    def test_historical_congress_in_prior_clause_does_not_reject(self):
        status, method, _ = resolve("H.R. 7665 in the 118th Congress; S. 717, Increasing Access to Quality Cardiac Rehabilitation Care Act of 2025",
                                    "Increasing Access to Quality Cardiac Rehabilitation Care Act of 2025")
        self.assertEqual(status, "candidate")
        self.assertIn(method, {"title_strong_local", "title_moderate_local"})

    def test_same_clause_current_congress_verifies(self):
        self.assertEqual(resolve("H.R.1 - 119th Congress (2025-2026) reconciliation", "An act to provide for reconciliation pursuant to title II of H. Con. Res. 14.")[:2],
                         ("verified", "explicit_congress_local"))

    def test_same_number_historical_public_law_rejects(self):
        self.assertEqual(resolve("H.R.4366 - Consolidated Appropriations Act, 2024 (P.L. 118-42).",
                                 "Save Local Business Act")[:2],
                         ("rejected", "conflicting_public_law_local"))

    def test_unrelated_prior_law_before_current_bill_does_not_reject(self):
        status, method, _ = resolve("Inflation Reduction Act (P.L.117-169) implementation; H.R.1 One Big Beautiful Bill Act",
                                    "An act to provide for reconciliation pursuant to title II of H. Con. Res. 14.")
        self.assertEqual(status, "candidate")
        self.assertNotEqual(method, "conflicting_public_law_local")

    def test_public_law_belongs_to_previous_bill(self):
        status, method, _ = resolve("H.R.133, Consolidated Appropriations Act, 2021 (P.L.116-260); S.2420/H.R.4710, No Surprises Act Enforcement Act",
                                    "No Surprises Act Enforcement Act")
        self.assertEqual(status, "candidate")
        self.assertNotEqual(method, "conflicting_public_law_local")

    def test_nearest_bill_gets_public_law(self):
        status, method, _ = resolve("H.R.4552/S.2465, FY26 THUD; H.R.1, One Big Beautiful Bill Act (P.L.119-21)",
                                    "Transportation, Housing and Urban Development, and Related Agencies Appropriations Act, 2026")
        self.assertEqual(status, "candidate")
        self.assertNotEqual(method, "public_law_congress_local")


if __name__ == "__main__":
    unittest.main()
