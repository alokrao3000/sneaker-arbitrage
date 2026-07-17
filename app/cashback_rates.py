"""
Manually maintained cashback rates — the best available rate across Rakuten,
TopCashback, RetailMeNot, and Honey for each supplier, plus which portal it
comes from. Edit this dict directly whenever a rate changes; nothing else in
the codebase needs to change to pick it up.

Keys must match the `name` column in data/suppliers.csv exactly. Rates below
are placeholders (0%, portal "none") until you fill in real current rates —
this file intentionally does not scrape cashback sites.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class CashbackRate:
    rate: float    # e.g. 0.08 for 8% — always the fraction, not the percent
    portal: str    # e.g. "Rakuten", "TopCashback", "RetailMeNot", "Honey", or "none"


DEFAULT_RATE = CashbackRate(rate=0.0, portal="none")

# One row per supplier name in data/suppliers.csv. Fill in real rates as you
# find them — anything left at 0.0/"none" just contributes no cashback yet.
CASHBACK_RATES: dict[str, CashbackRate] = {
    "Footlocker": CashbackRate(0.0, "none"),
    "Champs Sports": CashbackRate(0.0, "none"),
    "Eastbay": CashbackRate(0.0, "none"),
    "Hibbett": CashbackRate(0.0, "none"),
    "WSS": CashbackRate(0.0, "none"),
    "DTLR": CashbackRate(0.0, "none"),
    "Jimmy Jazz": CashbackRate(0.0, "none"),
    "Snipes": CashbackRate(0.0, "none"),
    "Shoe Palace": CashbackRate(0.0, "none"),
    "Renarts": CashbackRate(0.0, "none"),
    "SNS": CashbackRate(0.0, "none"),
    "Bodega": CashbackRate(0.0, "none"),
    "Slam Jam": CashbackRate(0.0, "none"),
    "Sneaker BAAS": CashbackRate(0.0, "none"),
    "CNCPTS": CashbackRate(0.0, "none"),
    "Corporate Got Em": CashbackRate(0.0, "none"),
    "Lapstone & Hammer": CashbackRate(0.0, "none"),
    "Nohble": CashbackRate(0.0, "none"),
    "Oneness": CashbackRate(0.0, "none"),
    "Packer": CashbackRate(0.0, "none"),
    "Premier": CashbackRate(0.0, "none"),
    "Sneaker Politics": CashbackRate(0.0, "none"),
    "Notre": CashbackRate(0.0, "none"),
    "Suede Store": CashbackRate(0.0, "none"),
    "UNKWN": CashbackRate(0.0, "none"),
    "Afew Store": CashbackRate(0.0, "none"),
    "Tactics": CashbackRate(0.0, "none"),
    "West NYC": CashbackRate(0.0, "none"),
    "Xhibition": CashbackRate(0.0, "none"),
    "Solestopp": CashbackRate(0.0, "none"),
    "Alumni NY": CashbackRate(0.0, "none"),
    "Feature": CashbackRate(0.0, "none"),
    "Sole Classics": CashbackRate(0.0, "none"),
    "Saint Alfred": CashbackRate(0.0, "none"),
    "Extra Butter": CashbackRate(0.0, "none"),
    "Kith": CashbackRate(0.0, "none"),
    "Above The Clouds": CashbackRate(0.0, "none"),
    "Addict Miami": CashbackRate(0.0, "none"),
    "Alife New York": CashbackRate(0.0, "none"),
    "Among Few": CashbackRate(0.0, "none"),
    "Anti Social Social Club": CashbackRate(0.0, "none"),
    "APB Store": CashbackRate(0.0, "none"),
    "BBC Ice Cream": CashbackRate(0.0, "none"),
    "BLKMKT": CashbackRate(0.0, "none"),
    "Blends": CashbackRate(0.0, "none"),
    "Bows and Arrows": CashbackRate(0.0, "none"),
    "Capsule Toronto": CashbackRate(0.0, "none"),
    "Commonwealth": CashbackRate(0.0, "none"),
    "Courtside Sneakers": CashbackRate(0.0, "none"),
    "Deadstock": CashbackRate(0.0, "none"),
    "Decade Store": CashbackRate(0.0, "none"),
    "Device One": CashbackRate(0.0, "none"),
    "Enter Nostalgia": CashbackRate(0.0, "none"),
    "Fear of God": CashbackRate(0.0, "none"),
    "Fice Gallery": CashbackRate(0.0, "none"),
    "Focus Pocus": CashbackRate(0.0, "none"),
    "Hanon Shop": CashbackRate(0.0, "none"),
    "High and Lows": CashbackRate(0.0, "none"),
    "Juice Store": CashbackRate(0.0, "none"),
    "Likelihood": CashbackRate(0.0, "none"),
    "Myfavoritethings": CashbackRate(0.0, "none"),
    "NRML": CashbackRate(0.0, "none"),
    "Noir Fonce": CashbackRate(0.0, "none"),
    "OAK": CashbackRate(0.0, "none"),
    "Oqium": CashbackRate(0.0, "none"),
    "Patta": CashbackRate(0.0, "none"),
    "RSVP Gallery": CashbackRate(0.0, "none"),
    "Slam City Skates": CashbackRate(0.0, "none"),
    "Sneaker Junkies": CashbackRate(0.0, "none"),
    "Space 23": CashbackRate(0.0, "none"),
    "Staple Pigeon": CashbackRate(0.0, "none"),
    "Stone Island": CashbackRate(0.0, "none"),
    "Stussy": CashbackRate(0.0, "none"),
    "The Closet Inc": CashbackRate(0.0, "none"),
    "Trophy Room": CashbackRate(0.0, "none"),
    "Undefeated": CashbackRate(0.0, "none"),
    "Union LA": CashbackRate(0.0, "none"),
    "Welcome Leeds": CashbackRate(0.0, "none"),
}


def get_cashback(supplier_name: str) -> CashbackRate:
    return CASHBACK_RATES.get(supplier_name, DEFAULT_RATE)
