#!/usr/bin/env python3
"""Regression checks for targeted low-frequency bill resolution.

These tests are intentionally database/API free.
"""
import re

def parse_forced_bill(raw):
    token=raw.upper().replace(".","").replace("-","").replace(" ","")
    m=re.fullmatch(r"(HR|S)(\d+)",token)
    if not m:
        raise ValueError(raw)
    return m.group(1),int(m.group(2))

def build_queue(ranked,top,forced):
    queue=list(ranked[:top])
    for token in forced:
        if token not in queue:
            queue.append(token)
    return queue

def test_low_frequency_target_is_appended():
    ranked=[("HR",1),("S",2),("HR",3)]
    assert build_queue(ranked,2,[("HR",5039)]) == [("HR",1),("S",2),("HR",5039)]

def test_existing_target_is_not_duplicated():
    ranked=[("HR",5039),("HR",1)]
    assert build_queue(ranked,2,[("HR",5039)]) == ranked

def test_target_parser():
    assert parse_forced_bill("H.R. 5039") == ("HR",5039)
    assert parse_forced_bill("S-123") == ("S",123)

def test_invalid_target_rejected():
    try: parse_forced_bill("HJRES 1")
    except ValueError: return
    raise AssertionError("invalid bill type accepted")
