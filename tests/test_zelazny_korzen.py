# -*- coding: utf-8 -*-
"""zelazny_korzen — o miejscu wpisu na forum i o typie w rejestrze decyduje TREŚĆ wpisu (decyzja
właścicielki 04.09.2026): nowa prośba zakłada nowy podwątek i nowe zlecenie; kontynuacja (druga próba,
ponowienie, ponaglenie, eskalacja, anulowanie, raport) staje pod NAJŚWIEŻSZĄ prośbą do tego samego
adresata i w rejestrze jest ponagleniem z korzeniem — bez limitu wieku.

Uprząż (bez sieci, bez sekretów, bez Firestore):
- forum_module.py importujemy z atrapami `streamlit` i `requests`, a `forum_write`/`forum_read` podmieniamy
  na atrapy, które ZAPISUJĄ, co poszłoby na forum (payload CreatePost) i oddają zadany wątek.
- app_vertex_ew.py jest skryptem Streamlit (nie da się go zaimportować) — pisarzy rejestru wycinamy po AST
  i wykonujemy w przestrzeni z atrapą Firestore, zamrożonym zegarem i pamięcią wątku z forum_module.

Przypadki = kształty z pomiaru na produkcji 15.08–04.09.2026 (numery spraw i wpisów ZMYŚLONE — repo jest
publiczne). Na kodzie sprzed zmiany (origin/main b86b797) większość jest CZERWONA (lista w raporcie).

Uruchomienie:  python3 -m pytest tests/ -q      (albo: python3 tests/test_zelazny_korzen.py)
Ścieżki plików można nadpisać zmiennymi SZTURCHACZ_APP i SZTURCHACZ_FORUM.
"""
import ast
import importlib.util
import os
import re
import sys
import types
import unittest
from datetime import datetime, timedelta

try:
    import pytz
except ImportError:                       # poza obrazem produkcyjnym → zoneinfo w tej samej roli
    import zoneinfo

    class _Strefa(zoneinfo.ZoneInfo):
        def localize(self, dt):
            return dt.replace(tzinfo=self)

    pytz = types.SimpleNamespace(timezone=lambda nazwa: _Strefa(nazwa))

_TU = os.path.dirname(os.path.abspath(__file__))
APP = os.environ.get("SZTURCHACZ_APP") or os.path.join(_TU, "..", "app_vertex_ew.py")
FORUM = os.environ.get("SZTURCHACZ_FORUM") or os.path.join(_TU, "..", "forum_module.py")
TZ = pytz.timezone("Europe/Warsaw")
TERAZ = TZ.localize(datetime(2026, 9, 4, 10, 0))          # zamrożony zegar testu
FUNKCJE = ("_dni_robocze_od", "_nasz_wpis", "_rekord_delegacji", "_korzen_delegacji",
           "ew_log_deleg", "ew_log_ponaglenia_bota", "_nasze_id_zlecen")
W_DE, W_FR, W_UKPL, W_REKL = 5690, 5689, 5691, 5688     # wątki forum jak w FORUM_THREADS (prod)


def _dzien(ile_dni_temu):
    return (TERAZ - timedelta(days=ile_dni_temu)).strftime("%Y-%m-%d")


def _czas(ile_dni_temu, godz="09:00"):
    return f"{_dzien(ile_dni_temu)} {godz}"


# ======================================================================
# ATRAPY: streamlit, requests
# ======================================================================
class _SessionState(dict):
    """st.session_state: słownik z dostępem po kropce (forum_module tak go używa)."""
    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k)

    def __setattr__(self, k, v):
        self[k] = v


def _atrapa_streamlit():
    return types.SimpleNamespace(
        session_state=_SessionState(),
        toast=lambda *a, **k: None, caption=lambda *a, **k: None, warning=lambda *a, **k: None,
        secrets={"FORUM_BEARER_TOKEN": "atrapa"},
        cache_resource=lambda f: f, cache=lambda f: f,
    )


def _atrapa_requests():
    def _brak_sieci(*a, **k):
        raise RuntimeError("test nie ma sieci — forum_write/forum_read mają być podmienione")
    return types.SimpleNamespace(post=_brak_sieci, get=_brak_sieci,
                                 exceptions=types.SimpleNamespace(HTTPError=Exception))


_LICZNIK = [0]


def zaladuj_forum():
    """Świeża instancja forum_module z atrapami (osobny stan pamięci sesji per test)."""
    st = _atrapa_streamlit()
    sys.modules["streamlit"] = st
    sys.modules["requests"] = _atrapa_requests()
    _LICZNIK[0] += 1
    spec = importlib.util.spec_from_file_location(f"forum_module_test_{_LICZNIK[0]}", FORUM)
    fm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fm)
    fm._TEST_ST = st
    return fm


# ======================================================================
# ATRAPA FIRESTORE: tylko to, czego używają pisarze
# ======================================================================
class _Dok:
    def __init__(self, magazyn, sciezka, dane):
        self._m, self._s, self._d = magazyn, sciezka, dane
        self.reference = self
        self.id = dane.get("_id", "dok")
        self.exists = True

    def to_dict(self):
        return dict(self._d)

    def update(self, zmiany):
        self._d.update(zmiany)


class _Zapytanie:
    def __init__(self, magazyn, sciezka, filtry=(), limit=None):
        self._m, self._s, self._f, self._l = magazyn, sciezka, list(filtry), limit

    def where(self, pole, op, wart):
        assert op == "==", op
        return _Zapytanie(self._m, self._s, self._f + [(pole, wart)], self._l)

    def limit(self, n):
        return _Zapytanie(self._m, self._s, self._f, n)

    def stream(self):
        wyn = [d for d in self._m.setdefault(self._s, [])
               if all(d.get(p) == w for p, w in self._f)]
        if self._l is not None:
            wyn = wyn[:self._l]
        return iter([_Dok(self._m, self._s, d) for d in wyn])

    def add(self, doc):
        d = dict(doc)
        d["_dzien"] = self._s[1]
        self._m.setdefault(self._s, []).append(d)
        return None, _Dok(self._m, self._s, d)

    def get(self):
        return types.SimpleNamespace(exists=False, to_dict=lambda: {})

    def document(self, nazwa):
        return _Zapytanie(self._m, self._s + (nazwa,))

    def collection(self, nazwa):
        return _Zapytanie(self._m, self._s + (nazwa,))


class AtrapaDB:
    def __init__(self):
        self.magazyn = {}

    def collection(self, nazwa):
        return _Zapytanie(self.magazyn, (nazwa,))

    def wsad(self, dzien, **pola):
        """Wcześniejszy rekord w ew_phone_log/{dzien}/delegacje — kształt 1:1 z produkcji."""
        d = {"numer_zamowienia": "", "typ": "zlecenie", "zlecil": "kasia_k", "grupa": "DE",
             "do_kogo": "", "id_postu": "", "link": "", "data_str": dzien, "godzina": "09:00"}
        d.update(pola)
        self.magazyn.setdefault(("test_ew_phone_log", dzien, "delegacje"), []).append(d)
        return d

    def delegacje(self, numer):
        return [d for k, v in self.magazyn.items()
                if k[0] == "test_ew_phone_log" and k[-1] == "delegacje"
                for d in v if d.get("numer_zamowienia") == numer]


class _ZegarTestu(datetime):
    @classmethod
    def now(cls, tz=None):
        return TERAZ.astimezone(tz) if tz else TERAZ.replace(tzinfo=None)


def zaladuj_pisarzy(db, fm):
    """Wycina pisarzy z app_vertex_ew.py po AST; pamięć wątku bierze z TEJ SAMEJ instancji forum_module."""
    with open(APP, encoding="utf-8") as f:
        zrodlo = f.read()
    drzewo = ast.parse(zrodlo)

    def _nazwa(n):
        if isinstance(n, ast.FunctionDef):
            return n.name
        if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
            return n.targets[0].id
        return None
    kawalki = [ast.get_source_segment(zrodlo, n) for n in drzewo.body if _nazwa(n) in FUNKCJE]
    ns = {
        "db": db, "col": lambda n: "test_" + n, "re": re, "pytz": pytz,
        "datetime": _ZegarTestu, "timedelta": timedelta,
        "firestore": types.SimpleNamespace(SERVER_TIMESTAMP="<ts>", Increment=lambda n: n,
                                           ArrayUnion=lambda x: x),
        "st": fm._TEST_ST,
        "ostatnie_posty": fm.ostatnie_posty,
        "otwiera_prosbe": getattr(fm, "otwiera_prosbe", lambda t: False),
    }
    exec("\n\n".join(kawalki), ns)
    return ns


# ======================================================================
# WPISY FORUM — dwa kształty: surowy z API (forum_read) i z pamięci sesji (_zapamietaj_posty)
# ======================================================================
DISCLAIMER = "<br><br>---<br><b>Jestem Chatoszturkiem AI, asystentem działu zwrotów.</b>"
DELEG = ("<b>Delegacja telefonu</b><br>Zamówienie: {nr}<br>Telefon: 000<br>Język: DE<br>Etap: PZ1 (brak odpowiedzi na WA)"
         "<br>Cel: termin odbioru po nieudanej próbie na WA.<br>Procedura: 2 próby")
PO_NIEUDANEJ = ("<b>Delegacja telefonu — po nieudanej próbie operatora</b><br>Zamówienie: {nr}<br>Telefon: 000"
                "<br>Pierwsza próba wykonana przez kasia_k — wynik: nie odebrał. PRÓBA 1.")
RECZNA = ("<p>&lt;b&gt;Delegacja telefonu &mdash; druga pr&oacute;ba&lt;/b&gt;&lt;br&gt;Zam&oacute;wienie: {nr}"
          "&lt;br&gt;Ponawiamy pro&#347;b&#281; o telefon.</p>")
PODB = "<b>Delegacja telefonu — druga próba</b><br>Zamówienie: {nr}<br>Ponawiamy prośbę o telefon."
BOT_PON = "<b>Ponawiamy prośbę o telefon</b><br>Zamówienie: {nr}<br>Prosimy o aktualizację statusu rozmowy."
PISMO = "<b>Prośba o pismo 5 etapu PDF</b><br>Zamówienie: {nr}<br>Etap: K4 (faza prawna)"
PON_PISMO = "<b>Ponaglenie (bump 1)</b><br>Zamówienie: {nr}<br>Ponawiamy prośbę o pismo 5 etapu."
RAPORT = "<b>Telefon wykonany — kasia_k</b><br>Zamówienie: {nr}<br>Wynik: nie odebrał"
ANUL = "<b>Anulowanie delegacji</b><br>Zamówienie: {nr}<br>Telefon nie jest już potrzebny."


def surowy(id_, do_odp, level_zero, autor, osoba, do_kogo, tekst, czas_utc, tytul=""):
    """Wpis w kształcie, jaki forum_module.forum_read oddaje z GetPostTree."""
    return {"Id": id_, "Do_Odpid": do_odp, "Text": tekst, "UserAddName": autor, "UserAddType": 2,
            "UserOdInGroup": osoba, "UserToName": do_kogo, "DateAdd": czas_utc,
            "Level": 0 if do_odp == 0 else 1, "LevelZero": level_zero,
            "Hierarchy": f"/0{level_zero}/" + ("" if do_odp == 0 else f"0{id_}/"), "Title": tytul}


def wpis(id_, do_odp, autor, osoba, do_kogo, tekst, czas, watek, level_zero=None):
    """Wpis w kształcie pamięci sesji (auto_load_forum_context → _zapamietaj_posty)."""
    lz = level_zero or (id_ if not do_odp else do_odp)
    m = re.match(r"\s*<b>(.*?)</b>", tekst, re.S)
    return {"Id": id_, "Do_Odpid": do_odp, "Hierarchy": f"/0{lz}/" + ("" if not do_odp else f"0{id_}/"),
            "Autor": autor, "Osoba": osoba, "Czas": czas, "Tekst": re.sub(r"<[^>]+>", " ", tekst)[:300],
            "Naglowek": (m.group(1) if m else re.sub(r"<[^>]+>", " ", tekst)[:150]).strip(),
            "Do_kogo": do_kogo, "LevelZero": lz, "Level": 0 if not do_odp else 1, "Watek": watek}


# ======================================================================
# CZĘŚĆ 1 — FORUM: kontynuacja pod najświeższą prośbą, nowa prośba = nowy podwątek
# ======================================================================
class MiejsceNaForum(unittest.TestCase):
    """execute_forum_actions dostaje marker AI; mierzymy, POD CZYM wpis stanąłby na forum (do_odp_id w CreatePost)."""

    def setUp(self):
        self.fm = zaladuj_forum()
        self.wyslane = []
        self.watki = {}                     # root_id → lista surowych wpisów (dla forum_read)
        self.nastepne_id = [990000]
        fm = self.fm

        def forum_write(post_id, do_odp_id, user_do, tresc, user_do_type=1, user_od=None, ai_user=None, tytul=None):
            self.nastepne_id[0] += 1
            nid = self.nastepne_id[0]
            self.wyslane.append({"post_id": post_id, "do_odp_id": do_odp_id, "user_do": user_do,
                                 "tresc": tresc, "new_id": nid})
            return {"success": True, "new_post_id": nid, "message": f"(id: {nid})",
                    "link": f"https://forum/Wpisy/detailWpis?id={post_id}&do_odpid={nid}#odp-{nid}"}

        def forum_read(branch_id=None, root_id=None, leaf_id=None, max_pages=5, **kw):
            posty = self.watki.get(root_id or branch_id or leaf_id)
            if posty is None:
                return {"success": False, "error": "atrapa: brak wątku"}
            return {"success": True, "posts": list(posty), "thread_title": "atrapa", "count": len(posty)}

        fm.forum_write = forum_write
        fm.forum_read = forum_read
        fm.discover_roots = lambda: {}
        fm.load_zastepstwa = lambda db, prefix="": {}

    def marker(self, cel, user_do, tresc, do_odp_id=None):
        extra = f"|do_odp_id={do_odp_id}" if do_odp_id else ""
        return f"Odpowiedź AI.\n[FORUM_WRITE|cel={cel}|user_do={user_do}{extra}|tresc={tresc}]\nKoniec."

    def wyslij(self, nr, cel, user_do, tresc, pamiec, do_odp_id=None, user_od="Operatorzy_DE", ai_user="kasia_k"):
        wynik = self.fm.execute_forum_actions(
            self.marker(cel, user_do, tresc, do_odp_id), forum_memory=pamiec, user_od=user_od, ai_user=ai_user,
            db=None, source_type="operator", diamond_prefix="test_", diamond_meta={"numer_zamowienia": nr})
        self.assertEqual(len(wynik["forum_writes"]), 1, "ma pójść dokładnie jeden wpis")
        self.assertTrue(wynik["forum_writes"][0].get("success"))
        return self.wyslane[-1]

    def rodzic(self):
        return self.wyslane[-1]["do_odp_id"]

    def tylko_tytul(self, w):
        self.watki[w] = [surowy(1489198, 0, 1489198, "sylwia", "", "EA", "<p>CZATOSZTUR 2</p>", "2026-04-03T06:44:58")]

    # --- F1: pamięć pod celem małymi literami („czatosztur_de") — 9 z 20 spraw luzem w próbce ---
    def test_F1_pamiec_pod_celem_malymi_literami(self):
        nr = "400101"
        pamiec = {"czatosztur_de": {"id": 900001, "data": _czas(4), "new_subthread": True}}
        self.tylko_tytul(W_DE)
        self.wyslij(nr, "CZATOSZTUR_DE", "Telefoniści_DE", PODB.format(nr=nr), pamiec)
        self.assertEqual(self.rodzic(), 900001, "podbicie ma stanąć pod prośbą z pamięci, nie luzem")
        self.assertEqual(self.wyslane[-1]["post_id"], W_DE)

    # --- F2: podbicie idzie pod NAJŚWIEŻSZĄ prośbę do tej grupy, nie pod pierwszą sprzed tygodni (307 w pomiarze) ---
    def test_F2_pod_najswiezsza_prosba(self):
        nr = "400102"
        self.fm._zapamietaj_posty(nr, [
            wpis(900001, 0, "OPERATORZY_DE", "ewelina_g", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(22), W_DE),
            wpis(900002, 900001, "TELEFONIŚCI_DE", "kinga", "OPERATORZY_DE", "nie odbiera", _czas(21), W_DE),
            wpis(900500, 0, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(3), W_DE),
        ])
        pamiec = {"CZATOSZTUR_DE": {"id": 900001, "data": _czas(22), "new_subthread": True}}
        self.wyslij(nr, "CZATOSZTUR_DE", "Telefoniści_DE", PODB.format(nr=nr), pamiec)
        self.assertEqual(self.rodzic(), 900500, "kontynuacja pod NAJŚWIEŻSZĄ prośbą, nie pod starą z pamięci")

    # --- F3: brak pamięci i pusta sesja — prośba znaleziona po numerze zamówienia w samym wątku ---
    def test_F3_bez_pamieci_po_numerze_w_watku(self):
        nr = "400103"
        self.watki[W_DE] = [
            surowy(1489198, 0, 1489198, "sylwia", "", "EA", "<p>CZATOSZTUR_DE 2</p>", "2026-04-03T06:44:58"),
            surowy(900001, 0, 900001, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr) + DISCLAIMER,
                   "2026-08-20T08:20:00", tytul=nr),
            surowy(900002, 900001, 900001, "TELEFONIŚCI_DE", "kinga", "OPERATORZY_DE", "tel_nieodebr", "2026-08-21T09:17:00"),
            surowy(950000, 0, 950000, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr="400999") + DISCLAIMER,
                   "2026-08-30T08:20:00", tytul="400999"),
        ]
        self.wyslij(nr, "CZATOSZTUR_DE", "Telefoniści_DE", PODB.format(nr=nr), {})
        self.assertEqual(self.rodzic(), 900001)

    # --- F4: jawne do_odp_id od AI zostaje (AI widzi kontekst) ---
    def test_F4_jawne_wskazanie_ai_zostaje(self):
        nr = "400104"
        self.fm._zapamietaj_posty(nr, [
            wpis(900001, 0, "OPERATORZY_UK/PL", "oliwia", "TELEFONIŚCI_ENG", DELEG.format(nr=nr), _czas(30), W_UKPL),
            wpis(900500, 0, "OPERATORZY_UK", "chatoszturek", "TELEFONIŚCI_ENG", DELEG.format(nr=nr), _czas(20), W_UKPL),
        ])
        pamiec = {"CZATOSZTUR_UK/PL": {"id": 900500, "data": _czas(20), "new_subthread": True}}
        self.wyslij(nr, "CZATOSZTUR_UKPL", "Telefoniści_ENG", PODB.format(nr=nr), pamiec, do_odp_id=900001,
                    user_od="Operatorzy_UK/PL")
        self.assertEqual(self.rodzic(), 900001)

    # --- F5: alias celu (CZATOSZTUR_UK / UK/PL / UKPL) — jeden wątek 5691, jedna pamięć ---
    def test_F5_alias_celu_ten_sam_watek(self):
        nr = "400105"
        pamiec = {"CZATOSZTUR_UK/PL": {"id": 910001, "data": _czas(5), "new_subthread": True}}
        self.tylko_tytul(W_UKPL)
        self.wyslij(nr, "CZATOSZTUR_UK", "Telefoniści_ENG", PODB.format(nr=nr), pamiec, user_od="Operatorzy_UK/PL")
        self.assertEqual(self.rodzic(), 910001)
        self.assertEqual(self.wyslane[-1]["post_id"], W_UKPL)

    # --- F6 (strażnik): sprawa bez żadnego wpisu w wątku → luzem ---
    def test_F6_sprawa_bez_wpisu_idzie_luzem(self):
        nr = "400106"
        self.tylko_tytul(W_DE)
        self.wyslij(nr, "CZATOSZTUR_DE", "Telefoniści_DE", PODB.format(nr=nr), {})
        self.assertIsNone(self.rodzic())

    # --- F7: druga próba w TEJ SAMEJ sesji (2h po pierwszej, AI pisze cel małymi) — pod pierwszą ---
    def test_F7_druga_proba_w_tej_samej_sesji(self):
        nr = "400107"
        pamiec = {}
        self.watki[W_FR] = []
        pierwszy = self.wyslij(nr, "CZATOSZTUR_FR", "Telefoniści_FR", DELEG.format(nr=nr), pamiec, user_od="Operatorzy_FR")
        self.assertIsNone(pierwszy["do_odp_id"])
        self.wyslij(nr, "czatosztur_fr", "Telefoniści_FR", PODB.format(nr=nr), pamiec, user_od="Operatorzy_FR")
        self.assertEqual(self.rodzic(), pierwszy["new_id"])
        self.assertTrue(any(str(p.get("Id")) == str(pierwszy["new_id"]) for p in self.fm.ostatnie_posty(nr)),
                        "wysłany wpis ma trafić do pamięci sesji")

    # --- F8 (strażnik): pamięć tylko z INNEGO wątku (reklamacje) nie kotwiczy wpisu w DE ---
    def test_F8_pamiec_innego_watku_nie_kotwiczy(self):
        nr = "400108"
        self.fm._zapamietaj_posty(nr, [
            wpis(920001, 0, "OPERATORZY_DE", "kinga", "DZIAŁ_EKSPERCKI",
                 "<b>Zapytanie o etap reklamacji</b><br>Zamówienie: " + nr, _czas(9), W_REKL)])
        pamiec = {"CZATOSZTUR_REKLAMACJE": {"id": 920001, "data": _czas(9), "new_subthread": True}}
        self.tylko_tytul(W_DE)
        self.wyslij(nr, "CZATOSZTUR_DE", "Telefoniści_DE", PODB.format(nr=nr), pamiec)
        self.assertIsNone(self.rodzic(), "wpis z wątku reklamacji nie jest kotwicą w wątku DE")
        self.assertEqual(self.wyslane[-1]["post_id"], W_DE)

    # --- F9: pamięć sesji z dwóch celów nie kasuje się nawzajem (dziś drugi odczyt nadpisuje pierwszy) ---
    def test_F9_pamiec_sesji_scala_dwa_watki(self):
        nr = "400109"
        self.fm._zapamietaj_posty(nr, [wpis(900001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(20), W_DE)])
        self.fm._zapamietaj_posty(nr, [wpis(920001, 0, "OPERATORZY_DE", "kinga", "DZIAŁ_EKSPERCKI", "Zamówienie: " + nr, _czas(9), W_REKL)])
        self.assertEqual({p["Id"] for p in self.fm.ostatnie_posty(nr)}, {900001, 920001})

    # --- F10: skan wątku przy braku pamięci widzi wpisy z konta grupy (OPERATORZY_*) i bierze NAJŚWIEŻSZĄ prośbę ---
    def test_F10_skan_watku_widzi_wpisy_operatorow(self):
        nr = "400110"
        self.watki[W_DE] = [
            surowy(1489198, 0, 1489198, "sylwia", "", "EA", "<p>CZATOSZTUR_DE 2</p>", "2026-04-03T06:44:58"),
            surowy(900001, 0, 900001, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr) + DISCLAIMER,
                   "2026-05-04T08:20:00", tytul=nr),
            surowy(900500, 0, 900500, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", DELEG.format(nr=nr) + DISCLAIMER,
                   "2026-08-30T01:20:00", tytul=nr),
            surowy(900600, 900500, 900500, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", BOT_PON.format(nr=nr) + DISCLAIMER,
                   "2026-09-01T01:20:00", tytul=nr),
        ]
        for w in (W_FR, W_UKPL, W_REKL, 5687, 5693, 5692, 5703):
            self.watki.setdefault(w, [])
        self.fm.save_forum_memory = lambda db, col_fn, n, cel, fid, co="": None
        found = self.fm._scan_forum_for_case(None, lambda x: "test_" + x, nr)
        self.assertTrue(found, "skan ma znaleźć sprawę po wpisie z konta grupy")
        self.assertEqual({str(v.get("id")) for v in found.values()}, {"900500"}, "pamięć = najświeższa prośba")

    # --- F11 (strażnik): wątek kurierski (AUTOS_KURIERZY 5687) NIE podlega regule — diament liczy się jak dotąd ---
    def test_F11_watek_kurierski_poza_regula(self):
        nr = "400111"
        self.watki[5687] = [
            surowy(900001, 0, 900001, "SPEDYCJA_REKLAMACJE", "", "OPERATORZY_DE", "<b>Kurier nie przyjechał</b><br>Zamówienie: " + nr,
                   "2026-08-01T08:00:00", tytul=nr)]
        self.wyslij(nr, "AUTOS_KURIERZY", "Operatorzy_DE", "<b>Zlecenie kuriera</b><br>Zamówienie: " + nr + "<br>Kurier: UPS", {})
        self.assertIsNone(self.rodzic(), "wpis kurierski nie podpina się pod cudzy wpis — nowy podwątek = diament")
        self.assertEqual(self.wyslane[-1]["post_id"], 5687)

    # --- F12: sesja pusta (auto_load nie zadziałał), pamięć stara → odczyt wątku znajduje najświeższą prośbę ---
    def test_F12_sesja_pusta_odczyt_watku(self):
        nr = "400112"
        self.watki[W_DE] = [
            surowy(1489198, 0, 1489198, "sylwia", "", "EA", "<p>CZATOSZTUR_DE 2</p>", "2026-04-03T06:44:58"),
            surowy(900001, 0, 900001, "OPERATORZY_DE", "ewelina_g", "TELEFONIŚCI_DE", DELEG.format(nr=nr) + DISCLAIMER,
                   "2026-08-13T12:46:00", tytul=nr),
            surowy(900500, 0, 900500, "OPERATORZY_DE", "marlena_b", "TELEFONIŚCI_DE", DELEG.format(nr=nr) + DISCLAIMER,
                   "2026-08-27T10:10:00", tytul=nr),
        ]
        pamiec = {"czatosztur_de": {"id": 900001, "data": "2026-08-13 14:46", "new_subthread": True}}
        self.wyslij(nr, "CZATOSZTUR_DE", "Telefoniści_DE", PODB.format(nr=nr), pamiec)
        self.assertEqual(self.rodzic(), 900500)
        self.assertTrue(any(p.get("Id") == 900500 for p in self.fm.ostatnie_posty(nr)),
                        "wpisy sprawy z odczytu wątku są już w pamięci sesji — pisarz rejestru je zobaczy")

    # --- F13: NOWA prośba (świeża „Delegacja telefonu") zakłada nowy podwątek na wierzchu, choć sprawa ma stary ---
    def test_F13_nowa_prosba_nowy_podwatek(self):
        nr = "400113"
        self.fm._zapamietaj_posty(nr, [
            wpis(900001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(40), W_DE),
            wpis(900002, 900001, "TELEFONIŚCI_DE", "kinga", "OPERATORZY_DE", "ustalono termin", _czas(39), W_DE),
        ])
        pamiec = {"CZATOSZTUR_DE": {"id": 900001, "data": _czas(40), "new_subthread": True}}
        nowy = self.wyslij(nr, "CZATOSZTUR_DE", "Telefoniści_DE", DELEG.format(nr=nr), pamiec)
        self.assertIsNone(self.rodzic(), "nowa prośba nie wraca pod stary podwątek")
        self.assertEqual(pamiec["CZATOSZTUR_DE"]["id"], nowy["new_id"], "pamięć wskazuje najświeższy podwątek")
        # a podbicie tej nowej prośby idzie już pod nią
        self.wyslij(nr, "CZATOSZTUR_DE", "Telefoniści_DE", PODB.format(nr=nr), pamiec)
        self.assertEqual(self.rodzic(), nowy["new_id"])

    # --- F14: raport po telefonie (do grupy operatorów) staje pod najświeższą prośbą sprawy ---
    def test_F14_raport_pod_najswiezsza_prosba(self):
        nr = "400114"
        self.fm._zapamietaj_posty(nr, [
            wpis(900001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(2), W_DE)])
        self.wyslij(nr, "CZATOSZTUR_DE", "Operatorzy_DE", RAPORT.format(nr=nr), {})
        self.assertEqual(self.rodzic(), 900001)

    # --- F16: „Delegacja telefonu — po nieudanej próbie operatora" to PIERWSZE zlecenie → nowy podwątek ---
    def test_F16_po_nieudanej_probie_to_nowa_prosba(self):
        nr = "400116"
        self.fm._zapamietaj_posty(nr, [
            wpis(900001, 0, "OPERATORZY_UK/PL", "oliwia", "OPERATORZY_UK/PL",
                 "<b>Delegacja zadania: Wiadomość eBay</b><br>Zamówienie: " + nr, _czas(9), W_UKPL)])
        self.wyslij(nr, "CZATOSZTUR_UKPL", "Telefoniści_ENG", PO_NIEUDANEJ.format(nr=nr), {}, user_od="Operatorzy_UK/PL")
        self.assertIsNone(self.rodzic(), "świeże zlecenie dla telefonistów nie staje pod cudzą prośbą")

    # --- F17: odmowa uprawnień (406) przy podpięciu → drugi zapis idzie NAPRAWDĘ luzem, nie w ten sam podwątek ---
    def test_F17_odmowa_uprawnien_naprawde_luzem(self):
        nr = "400117"
        fm = self.fm
        proby = []
        def forum_write(post_id, do_odp_id, user_do, tresc, user_do_type=1, user_od=None, ai_user=None, tytul=None):
            proby.append(do_odp_id)
            if do_odp_id:
                return {"success": False, "error": "406 Not Acceptable: Nie posiadasz uprawnień"}
            self.nastepne_id[0] += 1
            nid = self.nastepne_id[0]
            self.wyslane.append({"post_id": post_id, "do_odp_id": do_odp_id, "user_do": user_do, "tresc": tresc, "new_id": nid})
            return {"success": True, "new_post_id": nid, "message": f"(id: {nid})", "link": "x"}
        fm.forum_write = forum_write
        pamiec = {"CZATOSZTUR_DE": {"id": 900001, "data": _czas(3), "new_subthread": True}}
        self.fm._zapamietaj_posty(nr, [wpis(900001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(3), W_DE)])
        self.wyslij(nr, "CZATOSZTUR_DE", "Telefoniści_DE", PODB.format(nr=nr), pamiec)
        self.assertEqual(proby, [900001, None], "pierwsza próba pod prośbą, druga luzem")
        self.assertIsNone(self.rodzic())

    # --- F18: ręczna wklejka (encje HTML) rozpoznana jako kontynuacja i jako wpis sprawy ---
    def test_F18_reczna_wklejka_z_encjami(self):
        nr = "400118"
        self.assertFalse(self.fm.otwiera_prosbe(RECZNA.format(nr=nr)), "druga pr&oacute;ba = kontynuacja")
        self.assertTrue(self.fm.otwiera_prosbe("<p>&lt;b&gt;Delegacja telefonu&lt;/b&gt;&lt;br&gt;Zam&oacute;wienie: " + nr + "</p>"))
        self.assertEqual(self.fm._wpis_o_sprawie({"Text": "&lt;b&gt;Delegacja&lt;/b&gt; Zam&oacute;wienie: " + nr, "Title": ""}, nr), 1)

    # --- F15: ponaglenie o pismo (do justyny) idzie pod prośbę o pismo, nie pod nowszą delegację telefonu ---
    def test_F15_kontynuacja_do_tego_samego_adresata(self):
        nr = "400115"
        self.fm._zapamietaj_posty(nr, [
            wpis(900001, 0, "OPERATORZY_DE", "chatoszturek", "justyna", PISMO.format(nr=nr), _czas(9), W_DE),
            wpis(900500, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(2), W_DE),
        ])
        self.wyslij(nr, "CZATOSZTUR_DE", "justyna", PON_PISMO.format(nr=nr), {})
        self.assertEqual(self.rodzic(), 900001)


# ======================================================================
# CZĘŚĆ 2 — REJESTR: treść decyduje; podbicie = ponaglenie najświeższej prośby, z korzeniem
# ======================================================================
class KorzenWRejestrze(unittest.TestCase):
    def setUp(self):
        self.db = AtrapaDB()
        self.fm = zaladuj_forum()
        self.ns = zaladuj_pisarzy(self.db, self.fm)

    def deleguj(self, numer, do_kogo, id_postu, grupa="DE", **kw):
        arg = dict(op_name="kasia_k", numer=numer, grupa=grupa, do_kogo=do_kogo, jezyk=grupa,
                   pz="PZ2", id_postu=id_postu, link="", tresc=PODB.format(nr=numer), dzwoniacy=True)
        arg.update(kw)
        self.ns["ew_log_deleg"](**arg)
        nowe = [d for d in self.db.delegacje(numer) if str(d.get("id_postu")) == str(id_postu)]
        self.assertEqual(len(nowe), 1, "pisarz ma zapisać dokładnie jeden rekord")
        return nowe[0]

    def podbicie(self, d, korzen, dzien):
        self.assertEqual(d["typ"], "ponaglenie")
        self.assertEqual(str(d.get("korzen_post_id")), str(korzen))
        self.assertEqual(str(d.get("korzen_dzien")), dzien)

    # --- D1: kształt 385135 — delegacja 18.08, nowa delegacja automatu 02.09, ponaglenie 04.09 → korzeń = ta z 02.09 ---
    def test_D1_podbicie_najswiezszej_prosby_po_14_dniach(self):
        nr = "400201"
        self.db.wsad(_dzien(17), numer_zamowienia=nr, do_kogo="Telefoniści_FR", id_postu="800001", grupa="FR")
        self.db.wsad(_dzien(2), numer_zamowienia=nr, do_kogo="Telefoniści_FR", id_postu="800200", grupa="FR",
                     bot=True, zlecil="chatoszturek (automat)")
        self.fm._zapamietaj_posty(nr, [
            wpis(800001, 0, "OPERATORZY_FR", "sylwia", "TELEFONIŚCI_FR", DELEG.format(nr=nr), _czas(17), W_FR),
            wpis(800002, 800001, "TELEFONIŚCI_FR", "klaudia_k", "OPERATORZY_FR", "tel wakacje prosi o kontakt", _czas(17, "12:44"), W_FR),
            wpis(800200, 800001, "OPERATORZY_FR", "chatoszturek", "TELEFONIŚCI_FR", DELEG.format(nr=nr), _czas(2, "04:16"), W_FR),
        ])
        d = self.deleguj(nr, "Telefoniści_FR", "800300", grupa="FR",
                         tresc="<b>Delegacja telefonu — ponaglenie o zdjęcie</b><br>Zamówienie: " + nr)
        self.podbicie(d, "800200", _dzien(2))

    # --- D2: wcześniej tylko wpis automatu (kształt 384322) ---
    def test_D2_wczesniej_tylko_automat(self):
        nr = "400202"
        self.db.wsad(_dzien(6), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="810001",
                     bot=True, zlecil="chatoszturek (automat)")
        self.fm._zapamietaj_posty(nr, [
            wpis(810001, 0, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(6, "22:21"), W_DE),
            wpis(810002, 810001, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", BOT_PON.format(nr=nr), _czas(5, "20:52"), W_DE),
        ])
        d = self.deleguj(nr, "Telefoniści_DE", "810900")
        self.podbicie(d, "810001", _dzien(6))

    # --- D3: adresat innymi literami (kształt 384399: TELEFONIŚCI_DE vs Telefoniści_DE) ---
    def test_D3_adresat_innymi_literami(self):
        nr = "400203"
        self.db.wsad(_dzien(0), numer_zamowienia=nr, do_kogo="TELEFONIŚCI_DE", id_postu="820001", godzina="08:25")
        self.fm._zapamietaj_posty(nr, [
            wpis(820001, 0, "OPERATORZY_DE", "marlena_b", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(0, "08:25"), W_DE),
            wpis(820002, 820001, "TELEFONIŚCI_DE", "oliwia_m", "OPERATORZY_DE", "POCZTA", _czas(0, "08:37"), W_DE),
        ])
        d = self.deleguj(nr, "Telefoniści_DE", "820900")
        self.podbicie(d, "820001", _dzien(0))

    # --- D4: jedyna prośba sprzed czterech miesięcy (kształt 376774) — podbicie po treści dalej ją podbija ---
    def test_D4_korzen_sprzed_miesiecy(self):
        nr = "400204"
        self.db.wsad(_dzien(123), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="830001")
        self.fm._zapamietaj_posty(nr, [
            wpis(830001, 0, "OPERATORZY_DE", "OPERATORZY_DE", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(123), W_DE),
            wpis(830002, 830001, "OPERATORZY_DE", "OPERATORZY_DE", "TELEFONIŚCI_DE", PODB.format(nr=nr), _czas(122), W_DE),
            wpis(830003, 830001, "OPERATORZY_DE", "magda", "emilia", "<b>Zapytanie poboczne</b><br>Zamówienie: " + nr, _czas(50), W_DE),
        ])
        d = self.deleguj(nr, "Telefoniści_DE", "830900")
        self.podbicie(d, "830001", _dzien(123))

    # --- D5: pisarz ponagleń automatu — ponaglenie bota pod delegacją operatora → ponaglenie + korzeń ---
    def test_D5_pisarz_bota_z_korzeniem(self):
        nr = "400205"
        self.db.wsad(_dzien(3), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="840001")
        self.fm._zapamietaj_posty(nr, [
            wpis(840001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(3), W_DE),
            wpis(840100, 840001, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", BOT_PON.format(nr=nr), _czas(1, "01:31"), W_DE),
        ])
        ile = self.ns["ew_log_ponaglenia_bota"](nr, "DE")
        self.assertEqual(ile, 1)
        rek = [d for d in self.db.delegacje(nr) if str(d.get("id_postu")) == "840100"]
        self.assertEqual(len(rek), 1)
        self.assertTrue(rek[0].get("bot"))
        self.assertEqual(rek[0].get("do_kogo"), "TELEFONIŚCI_DE", "adresat z wpisu, nie zgadywany z grupy")
        self.podbicie(rek[0], "840001", _dzien(3))

    # --- D6: wpis bota sprzed 20 dni już zapisany → nie zapisuje się drugi raz (dziś dubel sprawdzany tylko w 14 dniach) ---
    def test_D6_dubel_bota_poza_14_dniami(self):
        nr = "400206"
        self.db.wsad(_dzien(25), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="850001")
        self.db.wsad(_dzien(20), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="850100", typ="ponaglenie",
                     bot=True, zlecil="chatoszturek (automat)", korzen_post_id="850001", korzen_dzien=_dzien(25))
        self.fm._zapamietaj_posty(nr, [
            wpis(850001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(25), W_DE),
            wpis(850100, 850001, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", BOT_PON.format(nr=nr), _czas(20, "02:00"), W_DE),
        ])
        ile = self.ns["ew_log_ponaglenia_bota"](nr, "DE")
        self.assertEqual(ile, 0)
        self.assertEqual(len([d for d in self.db.delegacje(nr) if str(d.get("id_postu")) == "850100"]), 1)

    # --- D7 (strażnik): pierwsza delegacja sprawy → zlecenie bez korzenia ---
    def test_D7_pierwsza_delegacja_to_zlecenie(self):
        nr = "400207"
        d = self.deleguj(nr, "Telefoniści_DE", "860001", tresc=DELEG.format(nr=nr))
        self.assertEqual(d["typ"], "zlecenie")
        self.assertFalse(d.get("korzen_post_id"))

    # --- D8 (strażnik): świeża delegacja do DE, gdy wcześniej była tylko do FR → zlecenie bez korzenia ---
    def test_D8_inna_grupa_bez_korzenia(self):
        nr = "400208"
        self.db.wsad(_dzien(2), numer_zamowienia=nr, do_kogo="Telefoniści_FR", id_postu="870001", grupa="FR")
        self.fm._zapamietaj_posty(nr, [
            wpis(870001, 0, "OPERATORZY_FR", "kasia_k", "TELEFONIŚCI_FR", DELEG.format(nr=nr), _czas(2), W_FR)])
        d = self.deleguj(nr, "Telefoniści_DE", "870900", tresc=DELEG.format(nr=nr))
        self.assertEqual(d["typ"], "zlecenie")
        self.assertFalse(d.get("korzen_post_id"))

    # --- D9: po anulowaniu delegacji nowa „Delegacja telefonu" = nowe zlecenie (dziś: 14-dniowa reguła robi z niej podbicie) ---
    def test_D9_po_anulowaniu_nowe_zlecenie(self):
        nr = "400209"
        self.db.wsad(_dzien(4), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="880001", anulowane=True)
        self.fm._zapamietaj_posty(nr, [
            wpis(880001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(4), W_DE),
            wpis(880002, 880001, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", ANUL.format(nr=nr), _czas(3), W_DE),
        ])
        d = self.deleguj(nr, "Telefoniści_DE", "880900", tresc=DELEG.format(nr=nr))
        self.assertEqual(d["typ"], "zlecenie")
        self.assertFalse(d.get("korzen_post_id"))

    # --- D10: pamięć sesji z dwóch wątków (reklamacje po DE) — prośba z DE dalej widoczna ---
    def test_D10_pamiec_dwoch_watkow(self):
        nr = "400210"
        self.db.wsad(_dzien(20), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="890001")
        self.fm._zapamietaj_posty(nr, [wpis(890001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(20), W_DE)])
        self.fm._zapamietaj_posty(nr, [wpis(890500, 0, "OPERATORZY_DE", "kinga", "DZIAŁ_EKSPERCKI", "Zamówienie: " + nr, _czas(9), W_REKL)])
        d = self.deleguj(nr, "Telefoniści_DE", "890900")
        self.podbicie(d, "890001", _dzien(20))

    # --- D11: ponaglenie automatu do OSOBY (justyna) dostaje PRAWDZIWEGO adresata, nie zgadywane „Telefoniści_DE" ---
    def test_D11_bot_prawdziwy_adresat(self):
        nr = "400211"
        self.fm._zapamietaj_posty(nr, [
            wpis(895001, 0, "OPERATORZY_DE", "chatoszturek", "justyna", PON_PISMO.format(nr=nr), _czas(1, "21:07"), W_DE)])
        ile = self.ns["ew_log_ponaglenia_bota"](nr, "DE")
        self.assertEqual(ile, 1)
        rek = self.db.delegacje(nr)
        self.assertEqual(len(rek), 1)
        self.assertEqual(rek[0].get("do_kogo"), "justyna", "adresat z wpisu — rejestr odsiewa osoby po grupie")

    # --- D12: wpis ANULUJĄCY (zapisywany po oflagowaniu rekordów) = ostatni krok łańcucha, z flagą, nie nowy wiersz ---
    def test_D12_wpis_anulujacy_domyka_lancuch(self):
        nr = "400212"
        self.db.wsad(_dzien(4), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="885001", anulowane=True)
        self.db.wsad(_dzien(3), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="885002", typ="ponaglenie",
                     anulowane=True)
        self.fm._zapamietaj_posty(nr, [
            wpis(885001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(4), W_DE),
            wpis(885002, 885001, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", PODB.format(nr=nr), _czas(3), W_DE),
            wpis(885900, 885001, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", ANUL.format(nr=nr), _czas(0), W_DE),
        ])
        d = self.deleguj(nr, "Telefoniści_DE", "885900", tresc=ANUL.format(nr=nr), anulowane=True)
        self.podbicie(d, "885001", _dzien(4))
        self.assertTrue(d.get("anulowane"))

    # --- D13: korzeń spoza okna 30 dni wchodzi do zbioru naszych wpisów (odpowiedź telefonistki pod korzeniem się wiąże) ---
    def test_D13_nasze_id_zlecen_z_korzeniem_spoza_okna(self):
        nr = "400213"
        self.db.wsad(_dzien(2), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="897100", typ="ponaglenie",
                     korzen_post_id="897001", korzen_dzien=_dzien(40))
        nasze = self.ns["_nasze_id_zlecen"](nr)
        self.assertIn("897001", nasze, "korzeń sprzed 40 dni ma być naszym zleceniem, choć rejestr czyta 30 dni")
        self.assertEqual(nasze["897001"]["typ"], "zlecenie")
        self.assertIn("897100", nasze)

    # --- D14: rekord automatu (zapisany w chwili publikacji, bez korzenia) zostaje NAPRAWIONY w miejscu, bez dubla ---
    def test_D14_naprawa_rekordu_automatu(self):
        nr = "400214"
        self.db.wsad(_dzien(3), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="898001")
        self.db.wsad(_dzien(1), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="898100", typ="zlecenie",
                     bot=True, zlecil="chatoszturek (automat)", link="https://forum/x", godzina="01:31")
        self.fm._zapamietaj_posty(nr, [
            wpis(898001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(3), W_DE),
            wpis(898100, 898001, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", BOT_PON.format(nr=nr), _czas(1, "01:31"), W_DE),
        ])
        ile = self.ns["ew_log_ponaglenia_bota"](nr, "DE")
        self.assertEqual(ile, 0, "bez drugiego rekordu")
        rek = [d for d in self.db.delegacje(nr) if str(d.get("id_postu")) == "898100"]
        self.assertEqual(len(rek), 1)
        self.podbicie(rek[0], "898001", _dzien(3))
        self.assertTrue(rek[0].get("korzen_sprawdzony"))
        self.assertEqual(self.ns["ew_log_ponaglenia_bota"](nr, "DE"), 0)

    # --- D15: świeża „Delegacja telefonu" 5 dni po poprzedniej = NOWE zlecenie (dziś: 14-dniowa reguła → podbicie) ---
    def test_D15_nowa_prosba_to_nowe_zlecenie(self):
        nr = "400215"
        self.db.wsad(_dzien(5), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="899001")
        self.fm._zapamietaj_posty(nr, [
            wpis(899001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(5), W_DE),
            wpis(899002, 899001, "TELEFONIŚCI_DE", "kinga", "OPERATORZY_DE", "ustalono termin na jutro", _czas(4), W_DE),
        ])
        d = self.deleguj(nr, "Telefoniści_DE", "899900", tresc=DELEG.format(nr=nr))
        self.assertEqual(d["typ"], "zlecenie", "nowa prośba nie dokleja się do starej")
        self.assertFalse(d.get("korzen_post_id"))

    # --- D16: dwie wcześniejsze prośby (20 i 3 dni temu) — podbicie wskazuje NAJŚWIEŻSZĄ, nie pierwszą ---
    def test_D16_podbicie_najswiezszej_z_dwoch(self):
        nr = "400216"
        self.db.wsad(_dzien(20), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="901001")
        self.db.wsad(_dzien(3), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="901500")
        self.fm._zapamietaj_posty(nr, [
            wpis(901001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(20), W_DE),
            wpis(901002, 901001, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", PODB.format(nr=nr), _czas(19), W_DE),
            wpis(901500, 0, "OPERATORZY_DE", "magda", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(3), W_DE),
        ])
        d = self.deleguj(nr, "Telefoniści_DE", "901900")
        self.podbicie(d, "901500", _dzien(3))

    # --- D18: „Delegacja telefonu — po nieudanej próbie operatora" = zlecenie, choć sprawa ma starą delegację do tej grupy ---
    def test_D18_po_nieudanej_probie_zlecenie(self):
        nr = "400218"
        self.db.wsad(_dzien(23), numer_zamowienia=nr, do_kogo="Telefoniści_ENG", id_postu="903001", grupa="UK")
        self.fm._zapamietaj_posty(nr, [
            wpis(903001, 0, "OPERATORZY_UK/PL", "oliwia", "TELEFONIŚCI_ENG", DELEG.format(nr=nr), _czas(23), W_UKPL)])
        d = self.deleguj(nr, "Telefoniści_ENG", "903900", grupa="UK", tresc=PO_NIEUDANEJ.format(nr=nr))
        self.assertEqual(d["typ"], "zlecenie")
        self.assertFalse(d.get("korzen_post_id"))

    # --- D19: kontynuacja bez czego podbijać (pierwszy wpis do grupy to „Ponaglenie") otwiera wiersz jak zlecenie ---
    def test_D19_kontynuacja_bez_prosby_otwiera_wiersz(self):
        nr = "400219"
        d = self.deleguj(nr, "Telefoniści_DE", "904001", tresc="<b>Ponaglenie</b><br>Zamówienie: " + nr)
        self.assertEqual(d["typ"], "zlecenie")
        self.assertFalse(d.get("korzen_post_id"))
        # a następne podbicie już ją podbija
        self.fm._zapamietaj_posty(nr, [wpis(904001, 0, "OPERATORZY_DE", "kasia_k", "TELEFONIŚCI_DE",
                                            "<b>Ponaglenie</b><br>Zamówienie: " + nr, _czas(1), W_DE)])
        d2 = self.deleguj(nr, "Telefoniści_DE", "904002")
        self.podbicie(d2, "904001", _dzien(1))

    # --- D20: zdublowane kopie rekordu automatu (stary dedup) naprawiane WSZYSTKIE, spójnie ---
    def test_D20_naprawa_wszystkich_kopii(self):
        nr = "400220"
        self.db.wsad(_dzien(3), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="905001")
        for _ in range(3):
            self.db.wsad(_dzien(1), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="905100", typ="zlecenie",
                         bot=True, zlecil="chatoszturek (automat)", godzina="01:31")
        self.fm._zapamietaj_posty(nr, [
            wpis(905001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(3), W_DE),
            wpis(905100, 905001, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", BOT_PON.format(nr=nr), _czas(1, "01:31"), W_DE),
        ])
        self.assertEqual(self.ns["ew_log_ponaglenia_bota"](nr, "DE"), 0)
        kopie = [d for d in self.db.delegacje(nr) if str(d.get("id_postu")) == "905100"]
        self.assertEqual(len(kopie), 3)
        for k in kopie:
            self.podbicie(k, "905001", _dzien(3))

    # --- D17: pisarz automatu — świeża „Delegacja telefonu" bota po starszej prośbie = zlecenie (dziś: ponaglenie, bo „był wpis") ---
    def test_D17_bot_nowa_prosba_to_zlecenie(self):
        nr = "400217"
        self.db.wsad(_dzien(9), numer_zamowienia=nr, do_kogo="Telefoniści_DE", id_postu="902001")
        self.fm._zapamietaj_posty(nr, [
            wpis(902001, 0, "OPERATORZY_DE", "kinga", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(9), W_DE),
            wpis(902100, 0, "OPERATORZY_DE", "chatoszturek", "TELEFONIŚCI_DE", DELEG.format(nr=nr), _czas(1, "22:59"), W_DE),
        ])
        self.assertEqual(self.ns["ew_log_ponaglenia_bota"](nr, "DE"), 1)
        rek = [d for d in self.db.delegacje(nr) if str(d.get("id_postu")) == "902100"]
        self.assertEqual(rek[0]["typ"], "zlecenie")
        self.assertFalse(rek[0].get("korzen_post_id"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
