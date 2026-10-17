"""
Region fallback — fill domain_profiles.derived_region when SimilarWeb has no
primary region (~25% of the base).

The cascade runs ONLY when `sw_primary_region` is empty and stops at the first hit:
  №1 ccTLD           → country code from the domain's country TLD   (free, instant)
  №2 AI homepage     → 2-letter code the classifier inferred from    (rides the AI pass)
                       GEO-STABLE signals (phone code, currency,
                       address, shipping) — NOT the served UI language
  №3 Whois / RDAP    → registrant country                           (free, rate-limited)

Reconciliation: because `derive_region` returns ("", "") whenever SimilarWeb
already has a region, every profile rebuild (full sync WRITE_TRUNCATE, incremental
MERGE) automatically NULLs derived_region the moment a later SW screening fills
sw_primary_region. No separate cleanup job. The underlying ai_region / whois caches
are kept, so if SW ever loses the region again the fallback transparently returns.

All outputs are ISO alpha-2 UPPERCASE — the exact format SimilarWeb uses
(verified: codes are 2-letter upper, UK is represented as GB), so the effective
value COALESCE(sw_primary_region, derived_region) is uniform for the UI's
Intl.DisplayNames region labels.
"""

# 2-letter TLDs that are country codes on paper but are sold/used as GENERIC
# vanity domains, so they are NOT a reliable audience-region signal. Excluded
# from ccTLD derivation → such domains fall through to AI / Whois instead.
_GENERIC_USE_CC = {
    "co", "io", "me", "tv", "cc", "ai", "ly", "to", "ws", "fm", "gg", "im",
    "sh", "st", "nu", "cx", "gl", "la", "mn", "am", "fo", "pw", "tc", "ac",
    "so", "vc", "ms", "bz",
    # free-registration TLDs dominated by spam/parked sites (Freenom et al.)
    "tk", "ml", "ga", "cf", "gq",
}

# ccTLDs whose ISO/SimilarWeb code differs from the TLD label, or regional TLDs.
_SPECIAL_CC = {
    "uk": "GB",   # .uk → SimilarWeb/ISO uses GB
    "eu": "EU",   # European Union — a region, kept as a single bucket
}


def _tld(domain: str) -> str:
    """Last label of the domain, lowercased (the effective TLD).
    'shop.com.ua' → 'ua', 'example.co.uk' → 'uk', 'foo.com' → 'com'."""
    d = (domain or "").strip().lower().rstrip(".")
    if "." not in d:
        return ""
    return d.rsplit(".", 1)[-1]


def cctld_region(domain: str) -> str:
    """ISO alpha-2 (upper) region implied by a country-code TLD, or '' for a
    generic TLD / generically-used ccTLD."""
    tld = _tld(domain)
    if not tld:
        return ""
    if tld in _SPECIAL_CC:
        return _SPECIAL_CC[tld]
    if len(tld) == 2 and tld.isalpha() and tld not in _GENERIC_USE_CC:
        # National ccTLDs ARE the ISO alpha-2 code — map directly.
        return tld.upper()
    return ""


def _norm_code(code: str) -> str:
    """Accept a 2-letter alpha region code → UPPER, else ''."""
    c = (code or "").strip().upper()
    return c if (len(c) == 2 and c.isalpha()) else ""


def derive_region(domain: str, sw_region: str = "",
                  ai_region: str = "", whois_country: str = "") -> tuple[str, str]:
    """Run the fallback cascade. Returns (region_code, source) where source is one
    of 'cctld' / 'ai' / 'whois', or ('', '') when SW already has a region (so the
    derived column is nulled) or nothing could be inferred."""
    if (sw_region or "").strip():
        return "", ""                       # SW authoritative → no derived value
    cc = cctld_region(domain)               # №1
    if cc:
        return cc, "cctld"
    air = _norm_code(ai_region)             # №2
    if air:
        return air, "ai"
    wc = _norm_code(whois_country)          # №3
    if wc:
        return wc, "whois"
    return "", ""
