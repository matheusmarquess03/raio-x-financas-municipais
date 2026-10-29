from raiox.ingestion.bronze_rgf import download_personnel_rgf
from raiox.ingestion.siconfi_client import SiconfiClient


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_client_percorre_todas_as_paginas(monkeypatch):
    paginas = [
        {"items": [{"id": 1}, {"id": 2}], "hasMore": True},
        {"items": [{"id": 3}], "hasMore": False},
    ]
    offsets = []

    def fake_get(url, params, timeout):
        offsets.append(params["offset"])
        return FakeResponse(paginas[len(offsets) - 1])

    client = SiconfiClient(pausa=0)
    monkeypatch.setattr(client.session, "get", fake_get)

    assert [r["id"] for r in client.get("rgf")] == [1, 2, 3]
    assert offsets == [0, 2]


def test_client_resposta_vazia(monkeypatch):
    client = SiconfiClient(pausa=0)
    vazio = FakeResponse({"items": [], "hasMore": False})
    monkeypatch.setattr(client.session, "get", lambda url, params, timeout: vazio)

    assert list(client.get("rgf")) == []


class FakeClient:
    def __init__(self, respostas):
        self.respostas = respostas
        self.chamadas = []

    def get(self, endpoint, **params):
        self.chamadas.append(params)
        return iter(self.respostas.get(params["in_periodicidade"], []))


def test_rgf_usa_semestral_quando_quadrimestral_vazio():
    client = FakeClient({"S": [{"valor": 10}]})

    assert download_personnel_rgf(client, 3300100, 2025) == [{"valor": 10}]
    assert [c["in_periodicidade"] for c in client.chamadas] == ["Q", "S"]
