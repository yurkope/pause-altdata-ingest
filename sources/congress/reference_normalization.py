"""Shared normalization helpers for Congress bill/provision references."""
import re

SECTION_TOKEN_RE=re.compile(
    r"(?i)\b(?:section|sec\.?)\s*(?P<section>[0-9]+(?:\.[0-9]+)*(?:[A-Za-z]+)?(?:\([A-Za-z0-9]+\))*)"
)
HIERARCHICAL_RE=re.compile(r"^(?P<root>[0-9]+[A-Za-z]*)(?P<nested>\(.*)$")

def normalize_section_token(value):
    """Return a normalized section identifier, preserving nested subsections."""
    if value is None: return None
    s=str(value).strip().rstrip(".").lower()
    m=SECTION_TOKEN_RE.search(s)
    return (m.group("section") if m else s).rstrip(".").lower()

def section_root(value):
    """Return (full, root, hierarchical) for a normalized section reference."""
    full=normalize_section_token(value)
    if not full: return full,full,False
    m=HIERARCHICAL_RE.match(full)
    return full,(m.group("root") if m else full),bool(m)
