#!/usr/bin/env python3
import importlib.util
from pathlib import Path

P=Path(__file__).with_name("export_validated_bill_targets.py")
spec=importlib.util.spec_from_file_location("targets",P)
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

def test_only_fully_validated_provisions_become_targets():
    rows=[
      {"owner":"HR 5039","validation_congress":"119","official_validation_status":"VALIDATED_BILL_PROVISION_CURRENT"},
      {"owner":"HR 5878","validation_congress":"119","official_validation_status":"SECTION_ROOT_EXISTS_SUBSECTION_UNVERIFIED"},
      {"owner":"S 99","validation_congress":"118","official_validation_status":"VALIDATED_BILL_PROVISION_HISTORICAL"},
      {"owner":"HR 1","validation_congress":"119","official_validation_status":"UNDERLYING_AUTHORITY_SECTION"},
    ]
    assert m.targets(rows)==[(118,"S",99),(119,"HR",5039)]

def test_targets_are_deduplicated():
    row={"owner":"HR 5039","validation_congress":"119","official_validation_status":"VALIDATED_BILL_PROVISION_CURRENT"}
    assert m.targets([row,row])==[(119,"HR",5039)]
