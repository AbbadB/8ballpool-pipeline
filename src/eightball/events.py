"""Factories that build schema-valid 8 Ball Pool events.

`country` is emitted as an id string and `platform` lowercase on purpose, so the
downstream DQ rules (uppercase, id->name) have real work to do. See ADR-0003.
"""


def make_init(*, user_id: str, country_id: str, platform: str, time_ms: int) -> dict:
    return {"event-type": "init", "time": time_ms, "user-id": user_id,
            "country": country_id, "platform": platform}


def _postmatch(*, coins: int, level: int, device: str, platform: str) -> dict:
    return {"coin-balance-after-match": coins, "level-after-match": level,
            "device": device, "platform": platform}


def make_match(*, user_a: str, user_b: str, winner: str, time_ms: int,
               game_tier: int = 5, duration: int = 120,
               platform: str = "ios") -> dict:
    return {
        "event-type": "match", "time": time_ms,
        "user-a": user_a, "user-b": user_b, "winner": winner,
        "user-a-postmatch-info": _postmatch(coins=100, level=3,
                                            device="iphone", platform=platform),
        "user-b-postmatch-info": _postmatch(coins=80, level=2,
                                            device="android", platform=platform),
        "game-tier": game_tier, "duration": duration,
    }


def make_purchase(*, user_id: str, value: float, product_id: str,
                  time_ms: int) -> dict:
    return {"event-type": "in-app-purchase", "time": time_ms,
            "purchase_value": value, "user-id": user_id, "product-id": product_id}
