from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """防止测试意外请求实盘 API；模拟器可在单个测试覆盖此方法。"""
    def refused(*args, **kwargs):
        raise AssertionError("Network is disabled in tests")
    monkeypatch.setattr("requests.sessions.Session.request", refused)
    monkeypatch.setattr("socket.socket.connect", refused)

from entropy_bot.coins import resolve_markets

PERP_DEXS = [None] + [
    {"name": name, "fullName": full}
    for name, full in [
        ("xyz", "XYZ"),
        ("flx", "Felix Exchange"),
        ("vntl", "Ventuals"),
        ("hyna", "HyENA"),
        ("km", "Markets by Kinetiq"),
        ("abcd", "ABCDEx"),
        ("cash", "dreamcash"),
        ("para", "Paragon"),
        ("mkts", "Markets By Kinetiq"),
        ("io", "EntropyIO"),
    ]
]

IO_META = {
    "collateralToken": 0,
    "universe": [
        {
            "szDecimals": 3,
            "name": "io:OAI",
            "maxLeverage": 3,
            "onlyIsolated": True,
            "isDelisted": True,
            "marginMode": "noCross",
            "deployerFeeScale": "1.0",
        },
        {
            "szDecimals": 3,
            "name": "io:ANTH",
            "maxLeverage": 3,
            "onlyIsolated": True,
            "marginMode": "strictIsolated",
            "growthMode": "enabled",
            "deployerFeeScale": "1.0",
        },
        {
            "szDecimals": 4,
            "name": "io:SNDK",
            "maxLeverage": 10,
            "onlyIsolated": True,
            "marginMode": "strictIsolated",
            "growthMode": "enabled",
            "deployerFeeScale": "1.0",
        },
        {
            "szDecimals": 2,
            "name": "io:IONQ",
            "maxLeverage": 20,
            "onlyIsolated": True,
            "isDelisted": True,
            "marginMode": "strictIsolated",
            "deployerFeeScale": "1.0",
        },
    ],
}


@pytest.fixture
def perp_dexs() -> list:
    return PERP_DEXS


@pytest.fixture
def io_meta() -> dict:
    return IO_META


@pytest.fixture
def markets(perp_dexs, io_meta):
    return resolve_markets(perp_dexs, io_meta)
