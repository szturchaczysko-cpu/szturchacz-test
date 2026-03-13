"""
MODUŁ FORUM PMG — integracja Szturchacza z forum F15
Pisanie i czytanie postów przez API.

Używany przez:
- app_vertex_ew.py (Koordynator) — w trakcie sesji operatora
- app.py (Wieżowiec) — w autopilocie nocnym

Endpointy:
- POST /api/wpisy/CreatePost — tworzenie/edycja postów
- POST /api/wpisy/GetPostTree — czytanie podwątków

Nick bota: chatoszturek
"""

import re
import json
import requests
import streamlit as st


# --- KONFIGURACJA ---
FORUM_API_BASE = "https://f15.pmgtechnik.com"
FORUM_USER = "chatoszturek"

def _get_bearer():
    return st.secrets.get("FORUM_BEARER_TOKEN", "")

def _headers():
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_get_bearer()}"
    }


# ==========================================
# PISANIE — CreatePost
# ==========================================

def forum_write(post_id, do_odp_id, user_do, tresc, user_do_type=1, user_od=None):
    """
    Tworzy post na forum.
    
    Args:
        post_id: ID wątku (thread.id) — np. 5351 (AUTOS_KURIERZY)
        do_odp_id: ID wpisu na który odpowiadamy (subThread.id)
        user_do: nick odbiorcy (np. "justyna") lub nazwa grupy (np. "AUTOS_KURIERZY")
        tresc: treść HTML postu
        user_do_type: 1=user, 2=grupa
        user_od: nick nadawcy (domyślnie FORUM_USER)
    
    Returns:
        dict: {"success": True, "new_post_id": 1461172, "message": "..."} 
              lub {"success": False, "error": "..."}
    """
    if user_od is None:
        user_od = FORUM_USER
    
    payload = {
        "thread": {
            "id": post_id,
            "title": None,
            "fromUser": user_od,
            "fromUserType": None,
            "toUser": user_do,
            "toUserType": None,
            "private": None
        },
        "subThread": {
            "id": do_odp_id,
            "text": tresc,
            "fromUser": user_od,
            "fromUserType": 1,
            "toUser": user_do,
            "toUserType": user_do_type,
            "type": 0,
            "title": None,
            "private": False
        }
    }
    
    try:
        resp = requests.post(
            f"{FORUM_API_BASE}/api/wpisy/CreatePost",
            headers=_headers(),
            json=payload,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        
        if data.get("status") == "SUCCESS":
            # Wyciągnij ID nowego postu z message
            msg = data.get("message", "")
            # Próbuj różne formaty: (id: 123), id: 123, (id:123), "id": 123
            id_match = re.search(r'\(id:\s*(\d+)\)', msg)
            if not id_match:
                id_match = re.search(r'id[:\s]+(\d+)', msg, re.IGNORECASE)
            if not id_match:
                # Szukaj dowolnej liczby > 1000000 (typowe ID postów)
                id_match = re.search(r'(\d{7,})', msg)
            new_id = int(id_match.group(1)) if id_match else None
            
            # Debug: loguj co API zwróciło (widoczne w st.toast)
            if not new_id:
                import streamlit as _st
                _st.toast(f"⚠️ Forum API OK ale brak ID w: {msg[:200]}")
            
            return {
                "success": True,
                "new_post_id": new_id,
                "message": msg,
                "link": f"{FORUM_API_BASE}/Wpisy/detailWpis?id={post_id}&do_odpid={new_id}#odp-{new_id}" if new_id else None
            }
        else:
            return {"success": False, "error": data.get("message", "Nieznany błąd")}
    
    except Exception as e:
        return {"success": False, "error": str(e)}


# ==========================================
# CZYTANIE — GetPostTree
# ==========================================

def forum_read(branch_id=None, root_id=None, leaf_id=None, max_pages=5):
    """
    Czyta podwątek z forum.
    
    Args:
        branch_id: ID podwątku (poziom 0) — najczęściej używane
        root_id: ID całego wątku
        leaf_id: ID konkretnego wpisu
        max_pages: max stron do pobrania (paginacja)
    
    Returns:
        dict: {"success": True, "posts": [...], "thread_title": "..."} 
              lub {"success": False, "error": "..."}
    """
    all_posts = []
    thread_title = ""
    
    for page in range(1, max_pages + 1):
        payload = {
            "root": root_id,
            "branch": branch_id,
            "leaf": leaf_id,
            "WholePage": None,
            "login": FORUM_USER,
            "PagingInfo": {
                "CurrentPage": page
            }
        }
        
        try:
            resp = requests.post(
                f"{FORUM_API_BASE}/api/wpisy/GetPostTree",
                headers=_headers(),
                json=payload,
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("status") != "SUCCESS" or not data.get("tree"):
                if page == 1:
                    return {"success": False, "error": data.get("message", "Brak danych")}
                break
            
            tree = data["tree"]
            if page == 1:
                thread_title = tree.get("Title", "")
            
            post_list = tree.get("PostList", [])
            if not post_list:
                break
            
            for p in post_list:
                all_posts.append({
                    "Id": p.get("Id"),
                    "Do_Odpid": p.get("Do_Odpid"),
                    "Text": p.get("Text", ""),
                    "UserAddName": p.get("UserAddName", ""),
                    "UserToName": p.get("UserToName", ""),
                    "DateAdd": p.get("DateAdd", ""),
                    "Level": p.get("Level", 0),
                    "Hierarchy": p.get("Hierarchy", ""),
                })
            
            # Sprawdź czy są kolejne strony
            paging = tree.get("PagingInfo", {})
            total_pages = paging.get("TotalPages", 1)
            if page >= total_pages:
                break
        
        except Exception as e:
            if page == 1:
                return {"success": False, "error": str(e)}
            break
    
    return {
        "success": True,
        "posts": all_posts,
        "thread_title": thread_title,
        "count": len(all_posts)
    }


def forum_read_subtree(branch_id, from_post_id):
    """
    Czyta podwątek i filtruje posty od konkretnego wpisu w dół (po Hierarchy).
    
    Args:
        branch_id: ID podwątku (branch)
        from_post_id: ID wpisu od którego chcemy czytać w dół
    
    Returns:
        jak forum_read, ale przefiltrowane
    """
    result = forum_read(branch_id=branch_id)
    if not result["success"]:
        return result
    
    # Znajdź hierarchy startowego posta
    start_hierarchy = None
    for p in result["posts"]:
        if p["Id"] == from_post_id:
            start_hierarchy = p["Hierarchy"]
            break
    
    if not start_hierarchy:
        # Post nie znaleziony — zwróć wszystko
        return result
    
    # Filtruj: posty których Hierarchy zaczyna się od start_hierarchy
    filtered = [p for p in result["posts"] if p["Hierarchy"].startswith(start_hierarchy)]
    
    return {
        "success": True,
        "posts": filtered,
        "thread_title": result["thread_title"],
        "count": len(filtered)
    }


# ==========================================
# PARSOWANIE MARKERÓW Z ODPOWIEDZI AI
# ==========================================

# Markery w odpowiedzi AI (nowy format):
# [FORUM_WRITE|cel=AUTOS_KURIERZY|tresc=Zlecenie kurierskie nr 369710...]
# [FORUM_WRITE|cel=AUTOS_KURIERZY|do_odp_id=1234567|tresc=Kontynuacja...]
# [FORUM_WRITE|cel=AUTOS_KURIERZY|user_do=justyna|tresc=...]
# [FORUM_READ|forum_id=1234567]  (czytaj od FORUM_ID — status sprawy)
# [FORUM_READ|cel=AUTOS_KURIERZY]  (czytaj cały wątek)

# Elastyczne parsowanie — key=value pary
FORUM_MARKER_PATTERN = re.compile(r'\[FORUM_(WRITE|READ)\|([^\]]+)\]', re.DOTALL)


def parse_forum_markers(ai_response):
    """Parsuje markery forum z odpowiedzi AI."""
    markers = []
    
    for m in FORUM_MARKER_PATTERN.finditer(ai_response):
        action = m.group(1).lower()  # "write" lub "read"
        params_str = m.group(2)
        
        # Parsuj key=value pary (tresc może zawierać |)
        params = {}
        if action == "write" and "|tresc=" in params_str:
            # Specjalne parsowanie: tresc jest ostatnia i może zawierać |
            before_tresc, tresc = params_str.split("|tresc=", 1)
            params["tresc"] = tresc.strip()
            for part in before_tresc.split("|"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k.strip()] = v.strip()
        else:
            for part in params_str.split("|"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k.strip()] = v.strip()
        
        marker = {"type": action, "raw": m.group(0), "params": params}
        
        if action == "write":
            marker["cel"] = params.get("cel", "")
            marker["tresc"] = params.get("tresc", "")
            marker["do_odp_id"] = int(params["do_odp_id"]) if "do_odp_id" in params else None
            marker["user_do"] = params.get("user_do", None)
        elif action == "read":
            marker["forum_id"] = int(params["forum_id"]) if "forum_id" in params else None
            marker["cel"] = params.get("cel", "")
        
        markers.append(marker)
    
    return markers


def execute_forum_actions(ai_response, forum_memory=None):
    """
    Parsuje markery → wykonuje API calls → zwraca podmienioną odpowiedź.
    
    forum_memory: dict {cel: {id, data, co}} z pamięci trwałej (opcjonalne).
    Gdy FORUM_WRITE bez do_odp_id → sprawdza forum_memory → kontynuacja lub nowy podwątek.
    """
    markers = parse_forum_markers(ai_response)
    
    if not markers:
        return {
            "response": ai_response,
            "forum_reads": [],
            "forum_writes": [],
            "had_actions": False
        }
    
    # Inicjalizuj forum_memory jeśli None
    if forum_memory is None:
        forum_memory = {}
    
    modified_response = ai_response
    forum_reads = []
    forum_writes = []
    
    for marker in markers:
        if marker["type"] == "write":
            cel = marker.get("cel", "")
            tresc = marker.get("tresc", "")
            do_odp_id = marker.get("do_odp_id")
            user_do = marker.get("user_do")
            
            result = forum_write_to_thread(
                cel=cel,
                tresc=tresc,
                user_do=user_do,
                do_odp_id=do_odp_id,
                forum_memory=forum_memory,
            )
            result["cel"] = cel
            result["tresc_skrot"] = tresc[:100] if tresc else ""
            forum_writes.append(result)
            
            if result.get("success"):
                # Aktualizuj lokalne forum_memory żeby kolejne markery widziały nowy ID
                forum_memory[cel] = {"id": result.get("FORUM_ID"), "new_subthread": USE_NEW_SUBTHREADS}
                replacement = (
                    f"✅ Wysłałem na forum ({cel}). "
                    f"Link: {result.get('link', '?')} "
                    f"FORUM_ID={result.get('FORUM_ID', '?')}"
                )
            else:
                replacement = f"❌ Błąd wysyłki na forum ({cel}): {result.get('error', '?')}"
            
            modified_response = modified_response.replace(marker["raw"], replacement)
        
        elif marker["type"] == "read":
            forum_id = marker.get("forum_id")
            cel = marker.get("cel", "")
            
            if forum_id:
                # Czytaj od konkretnego FORUM_ID
                if USE_NEW_SUBTHREADS:
                    result = forum_read(branch_id=forum_id, max_pages=3)
                else:
                    result = forum_read(leaf_id=forum_id, max_pages=3)
            elif cel and forum_memory and cel in forum_memory:
                # Mamy FORUM_ID w pamięci dla tego celu → czytaj TYLKO ten branch
                mem_id = forum_memory[cel].get("id")
                if mem_id:
                    if USE_NEW_SUBTHREADS:
                        result = forum_read(branch_id=mem_id, max_pages=3)
                    else:
                        thread_info = FORUM_THREADS.get(cel)
                        if thread_info and thread_info.get("korzen_id"):
                            result = forum_read_subtree(branch_id=thread_info["korzen_id"], from_post_id=mem_id)
                        else:
                            result = forum_read(leaf_id=mem_id, max_pages=3)
                else:
                    result = {"success": False, "error": f"Brak ID w pamięci dla {cel}"}
            elif cel:
                # Brak pamięci — NIE czytaj całego wątku (zwróciłby wszystkie sprawy)
                result = {
                    "success": True,
                    "posts": [],
                    "thread_title": "",
                    "count": 0
                }
            else:
                result = {"success": False, "error": "Brak forum_id i cel"}
            
            if result.get("success"):
                if result["count"] == 0:
                    cel_name = cel or "forum"
                    forum_reads.append(f"[FORUM_CONTEXT: {cel_name}] Brak wcześniejszych wpisów chatoszturka dla tego zamówienia. Jeśli trzeba pisać na forum — użyj FORUM_WRITE.")
                    replacement = f"📖 Forum ({cel_name}): brak wcześniejszych wpisów dla tego zamówienia."
                else:
                    context_parts = [f"[FORUM_CONTEXT] ({result['count']} postów)"]
                    for p in result["posts"]:
                        date_str = p['DateAdd'][:10] if p.get('DateAdd') else '?'
                        context_parts.append(
                            f"[{date_str}] {p['UserAddName']} → {p['UserToName']}: "
                            f"{_strip_html(p['Text'][:500])}"
                        )
                    forum_reads.append("\n".join(context_parts))
                    replacement = f"📖 Pobrano {result['count']} postów z forum (kontekst wstrzyknięty)."
            else:
                forum_reads.append(f"[FORUM_CONTEXT] Błąd: {result.get('error', '?')}")
                replacement = f"❌ Błąd czytania z forum: {result.get('error', '?')}"
            
            modified_response = modified_response.replace(marker["raw"], replacement)
    
    return {
        "response": modified_response,
        "forum_reads": forum_reads,
        "forum_writes": forum_writes,
        "had_actions": True
    }


def _strip_html(text):
    """Usuń tagi HTML z tekstu."""
    return re.sub(r'<[^>]+>', ' ', text).strip()


# ==========================================
# MAPOWANIE WĄTKÓW FORUM (znane post_id)
# ==========================================
# Workaround: chatoszturek pisze pod korzeniem podwątku (do_odp_id=korzeń_id).
# Docelowo: do_odp_id=0 = nowy podwątek per case (po zmianach kb).

# --- TEST MODE (przełącznik) ---
# True = pisze na wątek testowy 5670 (bezpieczne, nie zaśmieca produkcji)
# False = pisze na prawdziwe wątki (produkcja)
FORUM_TEST_MODE = True

# Nowe podwątki per case (subThread.id = null)
# True = każdy case dostaje własny podwątek (docelowo, po testach)
# False = workaround, pisze pod stałym korzeniem (testowe wątki z ręcznymi korzeniami)
USE_NEW_SUBTHREADS = False

_FORUM_THREADS_PROD = {
    "AUTOS_KURIERZY": {
        "post_id": 5443, "korzen_id": None,
        "grupa": "Team_Atomowki", "grupa_type": 2,
        "opis": "Zlecenie kuriera/etykiety/atomówki (§11.4)",
    },
    "SPEDYCJA_REKLAMACJE": {
        "post_id": 5615, "korzen_id": None,
        "grupa": "SPEDYCJA_REKLAMACJE", "grupa_type": 2,
        "opis": "Problemy po zleceniu kuriera (§10.4)",
    },
    "CZATOSZTUR_REKLAMACJE": {
        "post_id": 5616, "korzen_id": None,
        "grupa": "Dział_ekspercki", "grupa_type": 2,
        "opis": "Reklamacja 'co dalej / można szturchać' (§5.3)",
    },
    "NIEPOZAMYKANE_AUSTAUSCHE": {
        "post_id": 3730, "korzen_id": None,
        "grupa": "Niepozamykane_Austausche", "grupa_type": 2,
        "opis": "Niezamknięte Austausche / zielonka (§10.5)",
    },
    "CZATOSZTUR_DE": {
        "post_id": 5617, "korzen_id": None,
        "grupa": "Operatorzy_DE", "grupa_type": 2,
        "opis": "Czatosztur DE — delegacje TEL, zapytania (§8.3)",
    },
    "CZATOSZTUR_FR": {
        "post_id": 5618, "korzen_id": None,
        "grupa": "Operatorzy_FR", "grupa_type": 2,
        "opis": "Czatosztur FR (§8.3)",
    },
    "CZATOSZTUR_UKPL": {
        "post_id": 5619, "korzen_id": None,
        "grupa": "Operatorzy_UK/PL", "grupa_type": 2,
        "opis": "Czatosztur UK/PL (§8.3)",
    },
}

_FORUM_THREADS_TEST = {
    "AUTOS_KURIERZY": {
        "post_id": 5670, "korzen_id": 1464547,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Zlecenie kuriera/etykiety/atomówki",
    },
    "SPEDYCJA_REKLAMACJE": {
        "post_id": 5670, "korzen_id": 1464548,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Problemy po zleceniu kuriera",
    },
    "CZATOSZTUR_REKLAMACJE": {
        "post_id": 5670, "korzen_id": 1464549,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Reklamacja",
    },
    "NIEPOZAMYKANE_AUSTAUSCHE": {
        "post_id": 5670, "korzen_id": 1464550,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Niezamknięte Austausche",
    },
    "CZATOSZTUR_DE": {
        "post_id": 5670, "korzen_id": 1464551,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Czatosztur DE",
    },
    "CZATOSZTUR_FR": {
        "post_id": 5670, "korzen_id": 1464552,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Czatosztur FR",
    },
    "CZATOSZTUR_UKPL": {
        "post_id": 5670, "korzen_id": 1464553,
        "grupa": "Sylwia", "grupa_type": 1,
        "opis": "TEST: Czatosztur UKPL",
    },
}

FORUM_THREADS = _FORUM_THREADS_TEST if FORUM_TEST_MODE else _FORUM_THREADS_PROD


def discover_roots():
    """Odkryj korzenie (pierwszy post z Do_Odpid=0) każdego wątku.
    Cache w session_state."""
    cached = st.session_state.get("_forum_roots", {})
    if cached:
        for key, kid in cached.items():
            if key in FORUM_THREADS:
                FORUM_THREADS[key]["korzen_id"] = kid
        return cached
    
    roots = {}
    for key, info in FORUM_THREADS.items():
        result = forum_read(root_id=info["post_id"], max_pages=1)
        if result["success"] and result["posts"]:
            for p in result["posts"]:
                if p["Do_Odpid"] == 0:
                    roots[key] = p["Id"]
                    info["korzen_id"] = p["Id"]
                    break
            if key not in roots and result["posts"]:
                roots[key] = result["posts"][0]["Id"]
                info["korzen_id"] = result["posts"][0]["Id"]
    
    st.session_state["_forum_roots"] = roots
    return roots


def get_thread_info(cel):
    """Zwraca info o wątku. Odkrywa korzeń jeśli nieznany."""
    info = FORUM_THREADS.get(cel)
    if not info:
        return None
    if not info.get("korzen_id"):
        discover_roots()
    return info


def forum_write_to_thread(cel, tresc, user_do=None, do_odp_id=None, forum_memory=None):
    """Pisze na forum do wątku po nazwie celu (np. 'AUTOS_KURIERZY').
    
    Logika do_odp_id:
    1. do_odp_id podany explicite → użyj (kontynuacja konkretnego postu)
    2. forum_memory ma FORUM_ID dla tego celu → użyj (kontynuacja sprawy)
    3. USE_NEW_SUBTHREADS=True → None (nowy podwątek)
    4. USE_NEW_SUBTHREADS=False → korzen_id z mapy (workaround)
    """
    info = get_thread_info(cel)
    if not info:
        return {"success": False, "error": f"Nieznany cel: {cel}"}
    
    # Ustal do_odp_id
    if do_odp_id:
        target_do_odp = do_odp_id
    elif forum_memory and cel in forum_memory:
        # Kontynuacja — pisze pod istniejącym postem chatoszturka
        target_do_odp = forum_memory[cel].get("id")
    elif USE_NEW_SUBTHREADS:
        # Nowy podwątek (subThread.id = null)
        target_do_odp = None
    else:
        # Workaround — pisze pod stałym korzeniem
        target_do_odp = info.get("korzen_id")
        if not target_do_odp:
            return {"success": False, "error": f"Brak korzenia dla {cel}. Uruchom discover_roots()."}
    
    target_user = user_do or info.get("grupa", "EA")
    target_type = info.get("grupa_type", 1) if not user_do else (2 if target_user.isupper() or "_" in target_user else 1)
    
    # Dodaj disclaimer
    tresc_with_disclaimer = tresc + CHATOSZTUREK_DISCLAIMER
    
    result = forum_write(
        post_id=info["post_id"],
        do_odp_id=target_do_odp,
        user_do=target_user,
        tresc=tresc_with_disclaimer,
        user_do_type=target_type,
    )
    
    if result.get("success"):
        result["FORUM_ID"] = result["new_post_id"]
    
    return result


def forum_read_by_forum_id(forum_id):
    """Czyta podwątek od konkretnego FORUM_ID (Id postu chatoszturka).
    Używane do sprawdzenia statusu sprawy (np. czy jest etykieta)."""
    # FORUM_ID to Id postu — użyj jako leaf żeby pobrać kontekst
    # Potem użyj branch z tego samego podwątku żeby zobaczyć odpowiedzi
    result = forum_read(leaf_id=forum_id, max_pages=1)
    if result["success"] and result["posts"]:
        # Znajdź LevelZero (branch) z pierwszego postu
        # i pobierz cały branch żeby widzieć odpowiedzi
        first = result["posts"][0]
        branch = first.get("Id") if first.get("Do_Odpid") == 0 else None
        if not branch:
            # Pobierz cały wątek i filtruj
            return result
    return result


CHATOSZTUREK_DISCLAIMER = (
    '<br><br>---<br>'
    '<b>Jestem Chatoszturkiem AI, asystentem działu zwrotów.</b> '
    'Jeśli ta wiadomość wymaga korekty — odpisz tutaj.'
)


# ==========================================
# PAMIĘĆ FORUMOWA (przetrwa czyszczenie casów)
# ==========================================
# Kolekcja: test_forum_memory / forum_memory
# Klucz: numer zamówienia (stały)
# Wartość: forum_posts = {cel: {id, data, co}}

def save_forum_memory(db, col_fn, numer_zamowienia, cel, forum_id, co=""):
    """Zapisz forum_id do pamięci trwałej (po numerze zamówienia)."""
    from datetime import datetime
    import pytz
    tz_pl = pytz.timezone('Europe/Warsaw')
    data = datetime.now(tz_pl).strftime("%Y-%m-%d %H:%M")
    
    doc_ref = db.collection(col_fn("forum_memory")).document(str(numer_zamowienia))
    doc_ref.set({
        f"forum_posts.{cel}": {
            "id": forum_id,
            "data": data,
            "co": co[:100] if co else "",
            "new_subthread": USE_NEW_SUBTHREADS,
        }
    }, merge=True)


def load_forum_memory(db, col_fn, numer_zamowienia):
    """Wczytaj pamięć forumową dla zamówienia. Zwraca dict {cel: {id, data, co}} lub {}."""
    try:
        doc = db.collection(col_fn("forum_memory")).document(str(numer_zamowienia)).get()
        if doc.exists:
            return doc.to_dict().get("forum_posts", {})
    except Exception:
        pass
    return {}


def auto_load_forum_context(db, col_fn, numer_zamowienia):
    """Automatycznie odpytaj forum dla wszystkich zapamiętanych postów zamówienia.
    Zwraca string z kontekstem do wstrzyknięcia do AI (lub pusty string)."""
    memory = load_forum_memory(db, col_fn, numer_zamowienia)
    if not memory:
        return ""
    
    context_parts = []
    for cel, info in memory.items():
        forum_id = info.get("id")
        if not forum_id:
            continue
        
        is_new_subthread = info.get("new_subthread", USE_NEW_SUBTHREADS)
        
        if is_new_subthread:
            result = forum_read(branch_id=forum_id, max_pages=2)
        else:
            thread_info = FORUM_THREADS.get(cel)
            if thread_info and thread_info.get("korzen_id"):
                result = forum_read_subtree(
                    branch_id=thread_info["korzen_id"],
                    from_post_id=forum_id
                )
            else:
                result = forum_read(leaf_id=forum_id, max_pages=2)
        
        if result.get("success") and result.get("posts"):
            co = info.get("co", cel)
            posts = result["posts"][-10:]
            
            other_posts = [p for p in posts if p.get("UserAddName") != FORUM_USER or p.get("Id") != forum_id]
            
            if other_posts:
                context_parts.append(f"[FORUM_CONTEXT: {cel}] ({co}, {result['count']} postów)")
            else:
                context_parts.append(f"[FORUM_CONTEXT: {cel}] ({co}, brak nowych odpowiedzi)")
            
            for p in posts:
                date_str = p['DateAdd'][:10] if p.get('DateAdd') else '?'
                context_parts.append(
                    f"  [{date_str}] {p['UserAddName']} → {p['UserToName']}: "
                    f"{_strip_html(p['Text'][:400])}"
                )
    
    if context_parts:
        return "\n".join(context_parts)
    return ""
