#!/usr/bin/env python3
from reference_normalization import normalize_section_token,section_root

def test_heading_suffix_is_not_swallowed():
    assert normalize_section_token("section 4-water quality technology availability")=="4"

def test_nested_reference_is_preserved():
    assert normalize_section_token("Sec. 4(a)(B)")=="4(a)(b)"

def test_hierarchical_root():
    assert section_root("4(a)(b)") == ("4(a)(b)","4",True)

def test_plain_section_root():
    assert section_root("12007") == ("12007","12007",False)

def test_alphanumeric_section():
    assert section_root("505g(c)") == ("505g(c)","505g",True)
