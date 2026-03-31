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

# --- DEBUG LOG ---
FORUM_DEBUG = True  # True = loguj wszystko do session_state

def _flog(msg):
    """Loguj do session_state (widoczne w UI) + print (logi Streamlit Cloud)"""
    if not FORUM_DEBUG:
        return
    if "forum_debug_log" not in st.session_state:
        st.session_state.forum_debug_log = []
    st.session_state.forum_debug_log.append(msg)
    print(f"[FORUM_DEBUG] {msg}")

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
    
    # Ustal typ autora: grupa (2) jeśli wygląda jak grupa, user (1) jeśli wygląda jak nick
    from_user_type = 2 if (user_od.isupper() or "_" in user_od) and user_od != FORUM_USER else 1
    
    _flog(f"WRITE: post_id={post_id}, do_odp_id={do_odp_id}, user_do={user_do}, type={user_do_type}")
    _flog(f"WRITE: user_od={user_od}, from_type={from_user_type}")
    _flog(f"WRITE: tresc={tresc[:80]}...")
    
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
            "fromUserType": from_user_type,
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
            
            _flog(f"WRITE RESULT: success=True, new_id={new_id}, msg={msg[:100]}")
            
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
                    _flog(f"READ FAIL: status={data.get('status')}, msg={data.get('message', '?')}, has_tree={bool(data.get('tree'))}")
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


def execute_forum_actions(ai_response, forum_memory=None, user_od=None):
    """
    Parsuje markery → wykonuje API calls → zwraca podmienioną odpowiedź.
    
    forum_memory: dict {cel: {id, data, co}} z pamięci trwałej (opcjonalne).
    user_od: nick autora wpisu (zalogowany operator). Domyślnie FORUM_USER (chatoszturek).
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
                user_od=user_od,
            )
            result["cel"] = cel
            result["tresc_skrot"] = tresc[:100] if tresc else ""
            forum_writes.append(result)
            
            if result.get("success"):
                # Aktualizuj lokalne forum_memory TYLKO jeśli cel nie był jeszcze znany
                # (nie nadpisuj pierwotnego ID — subtree czytamy zawsze od pierwszego wpisu)
                if cel not in forum_memory:
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
        "grupa": "TEAM_ATOMOWKI", "grupa_type": 2,
        "opis": "Zlecenie kuriera/etykiety/atomówki (§11.4)",
    },
    "SPEDYCJA_REKLAMACJE": {
        "post_id": 5615, "korzen_id": None,
        "grupa": "SPEDYCJA_REKLAMACJE", "grupa_type": 2,
        "opis": "Problemy po zleceniu kuriera (§10.4)",
    },
    "CZATOSZTUR_REKLAMACJE": {
        "post_id": 5616, "korzen_id": None,
        "grupa": "DZIAŁ_EKSPERCKI", "grupa_type": 2,
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
        "post_id": 5443, "korzen_id": None,
        "grupa": "TEAM_ATOMOWKI", "grupa_type": 2,
        "opis": "Zlecenie kuriera/etykiety/atomówki (§11.4)",
    },
    "SPEDYCJA_REKLAMACJE": {
        "post_id": 5615, "korzen_id": None,
        "grupa": "SPEDYCJA_REKLAMACJE", "grupa_type": 2,
        "opis": "Problemy po zleceniu kuriera (§10.4)",
    },
    "CZATOSZTUR_REKLAMACJE": {
        "post_id": 5616, "korzen_id": None,
        "grupa": "DZIAŁ_EKSPERCKI", "grupa_type": 2,
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


def forum_write_to_thread(cel, tresc, user_do=None, do_odp_id=None, forum_memory=None, user_od=None):
    """Pisze na forum do wątku po nazwie celu (np. 'AUTOS_KURIERZY').
    
    Logika do_odp_id:
    1. do_odp_id podany explicite → użyj (kontynuacja konkretnego postu)
    2. forum_memory ma FORUM_ID dla tego celu → użyj (kontynuacja sprawy)
    3. USE_NEW_SUBTHREADS=True → None (nowy podwątek)
    4. USE_NEW_SUBTHREADS=False → korzen_id z mapy (workaround)
    
    user_od: nick autora (zalogowany operator). Domyślnie FORUM_USER (chatoszturek).
    """
    info = get_thread_info(cel)
    if not info:
        _flog(f"WRITE_TO_THREAD: cel={cel} → NIEZNANY CEL")
        return {"success": False, "error": f"Nieznany cel: {cel}"}
    
    _flog(f"WRITE_TO_THREAD: cel={cel}, do_odp_id={do_odp_id}, USE_NEW={USE_NEW_SUBTHREADS}")
    _flog(f"  thread_info: post_id={info.get('post_id')}, korzen_id={info.get('korzen_id')}")
    _flog(f"  forum_memory: {forum_memory}")
    
    # Ustal do_odp_id
    if do_odp_id:
        target_do_odp = do_odp_id
        _flog(f"  DECYZJA: explicit do_odp_id={do_odp_id}")
    elif forum_memory and cel in forum_memory:
        # Kontynuacja — pisze pod istniejącym postem chatoszturka
        target_do_odp = forum_memory[cel].get("id")
        _flog(f"  DECYZJA: kontynuacja z forum_memory, target={target_do_odp}")
    elif USE_NEW_SUBTHREADS:
        # Nowy podwątek (subThread.id = null)
        target_do_odp = None
        _flog(f"  DECYZJA: NOWY PODWĄTEK (USE_NEW=True, do_odp_id=None)")
    else:
        # Workaround — pisze pod stałym korzeniem
        target_do_odp = info.get("korzen_id")
        if target_do_odp:
            _flog(f"  DECYZJA: workaround korzen_id={target_do_odp}")
        else:
            # Brak korzenia = nowy wątek bez podwątków, piszemy bezpośrednio (do_odp_id=0)
            target_do_odp = 0
            _flog(f"  DECYZJA: brak korzenia → nowy post w wątku (do_odp_id=0)")
    
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
        user_od=user_od,
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

# Marker do rozpoznawania wpisów AI (niezależnie od autora)
AI_DISCLAIMER_MARKER = "Jestem Chatoszturkiem AI"

def _is_ai_post(post):
    """Sprawdza czy post został napisany przez AI (po disclaimerze w treści)."""
    text = post.get("Text", "")
    return AI_DISCLAIMER_MARKER in text


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
    data_str = datetime.now(tz_pl).strftime("%Y-%m-%d %H:%M")
    
    _flog(f"SAVE_MEMORY: nrzam={numer_zamowienia}, cel={cel}, forum_id={forum_id}")
    _flog(f"  kolekcja: {col_fn('forum_memory')}")
    
    entry = {
        "id": forum_id,
        "data": data_str,
        "co": co[:100] if co else "",
        "new_subthread": USE_NEW_SUBTHREADS,
    }
    
    doc_ref = db.collection(col_fn("forum_memory")).document(str(numer_zamowienia))
    try:
        existing = doc_ref.get()
        if existing.exists:
            existing_posts = existing.to_dict().get("forum_posts", {})
            if cel in existing_posts:
                # Już istnieje — nie nadpisuj pierwotnego ID, tylko loguj
                _flog(f"  → JUŻ ISTNIEJE (nie nadpisuję, pierwotny id={existing_posts[cel].get('id')})")
                return
        # Nowy wpis — zapisz
        doc_ref.update({f"forum_posts.{cel}": entry})
        _flog(f"  → ZAPISANO (update)")
    except Exception:
        # Dokument nie istnieje — utwórz
        doc_ref.set({"forum_posts": {cel: entry}})
        _flog(f"  → ZAPISANO (set — nowy dokument)")


def load_forum_memory(db, col_fn, numer_zamowienia):
    """Wczytaj pamięć forumową dla zamówienia."""
    _flog(f"LOAD_MEMORY: nrzam={numer_zamowienia}, kolekcja={col_fn('forum_memory')}")
    try:
        doc = db.collection(col_fn("forum_memory")).document(str(numer_zamowienia)).get()
        if doc.exists:
            result = doc.to_dict().get("forum_posts", {})
            _flog(f"  → ZNALEZIONO: {json.dumps({k: v.get('id') for k,v in result.items()}) if result else 'puste'}")
            return result
        else:
            _flog(f"  → BRAK dokumentu w Firestore")
    except Exception as e:
        _flog(f"  → BŁĄD: {e}")
    return {}


def auto_load_forum_context(db, col_fn, numer_zamowienia):
    """Automatycznie odpytaj forum. Fallback: scan po numerze zamówienia."""
    _flog(f"AUTO_LOAD: start, nrzam={numer_zamowienia}")
    memory = load_forum_memory(db, col_fn, numer_zamowienia)
    
    # Fallback: pamięć pusta → scan forum
    if not memory:
        _flog(f"AUTO_LOAD: pamięć pusta → scan forum po nrzam={numer_zamowienia}")
        memory = _scan_forum_for_case(db, col_fn, str(numer_zamowienia))
    
    if not memory:
        _flog(f"AUTO_LOAD: scan też pusty → zwracam pusty kontekst")
        return ""
    
    _flog(f"AUTO_LOAD: mam dane: {list(memory.keys())}")
    
    context_parts = []
    for cel, info in memory.items():
        forum_id = info.get("id")
        if not forum_id:
            continue
        
        is_new_subthread = info.get("new_subthread", USE_NEW_SUBTHREADS)
        _flog(f"AUTO_LOAD: czytam {cel}, forum_id={forum_id}, new_sub={is_new_subthread}")
        
        if is_new_subthread:
            result = forum_read(branch_id=forum_id, max_pages=2)
        else:
            thread_info = FORUM_THREADS.get(cel)
            if thread_info and thread_info.get("korzen_id"):
                _flog(f"  → subtree: branch={thread_info['korzen_id']}, from={forum_id}")
                result = forum_read_subtree(
                    branch_id=thread_info["korzen_id"],
                    from_post_id=forum_id
                )
            else:
                # Brak korzenia — próbuj odczyt po ID wpisu
                _flog(f"  → próba 1: leaf_id={forum_id}")
                result = forum_read(leaf_id=forum_id, max_pages=2)
                _flog(f"  → próba 1 wynik: success={result.get('success')}, count={result.get('count', 0)}, error={result.get('error', 'none')}")
                
                if not result.get("success") or not result.get("posts"):
                    _flog(f"  → próba 2: branch_id={forum_id}")
                    result = forum_read(branch_id=forum_id, max_pages=2)
                    _flog(f"  → próba 2 wynik: success={result.get('success')}, count={result.get('count', 0)}, error={result.get('error', 'none')}")
        
        _flog(f"  → wynik: success={result.get('success')}, count={result.get('count', 0)}")
        
        if result.get("success") and result.get("posts"):
            co = info.get("co", cel)
            posts = result["posts"][-10:]
            
            other_posts = [p for p in posts if not _is_ai_post(p) or p.get("Id") != forum_id]
            
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


def load_forum_context_by_id(db, col_fn, numer_zamowienia, cel, forum_id):
    """
    Czyta wątek forum po podanym ID (niezależnie od usera który pisał).
    Używane gdy operator poda ID ręcznie (stare wpisy sprzed memory).
    Zapisuje do forum_memory żeby kolejnym razem działało automatycznie.
    """
    _flog(f"LOAD_BY_ID: nrzam={numer_zamowienia}, cel={cel}, forum_id={forum_id}")

    thread_info = FORUM_THREADS.get(cel)
    if thread_info and thread_info.get("korzen_id"):
        _flog(f"  → subtree: korzen={thread_info['korzen_id']}, from={forum_id}")
        result = forum_read_subtree(
            branch_id=thread_info["korzen_id"],
            from_post_id=forum_id
        )
    else:
        _flog(f"  → leaf: forum_id={forum_id}")
        result = forum_read(leaf_id=forum_id, max_pages=3)

    _flog(f"  → wynik: success={result.get('success')}, count={result.get('count', 0)}")

    context_parts = []
    if result.get("success") and result.get("posts"):
        posts = result["posts"][-10:]
        context_parts.append(f"[FORUM_CONTEXT: {cel}] (wczytano po ID={forum_id}, {result['count']} postów)")
        for p in posts:
            date_str = p['DateAdd'][:10] if p.get('DateAdd') else '?'
            context_parts.append(
                f"  [{date_str}] {p['UserAddName']} → {p['UserToName']}: "
                f"{_strip_html(p['Text'][:400])}"
            )
        # Zapisz do memory żeby następnym razem działało automatycznie
        try:
            save_forum_memory(db, col_fn, numer_zamowienia, cel, forum_id, f"manual: {cel}")
            _flog(f"  → zapisano do forum_memory: {cel}={forum_id}")
        except Exception as e:
            _flog(f"  → błąd zapisu memory: {e}")
    else:
        # Fallback — nie znaleziono treści ale ID zapisujemy
        context_parts.append(
            f"[FORUM_CONTEXT: {cel}] (wpis id={forum_id}, brak treści do odczytu — "
            f"NIE generuj FORUM_WRITE, czekaj na odpowiedź)"
        )
        try:
            save_forum_memory(db, col_fn, numer_zamowienia, cel, forum_id, f"manual_empty: {cel}")
        except Exception:
            pass

    return "\n".join(context_parts)


def _scan_forum_for_case(db, col_fn, numer_zamowienia):
    """Fallback: przeszukaj wątki forum po numerze zamówienia."""
    found = {}
    nrzam = str(numer_zamowienia)
    _flog(f"SCAN: szukam {nrzam} w wątkach forum")
    
    scanned_roots = set()
    for cel, info in FORUM_THREADS.items():
        post_id = info.get("post_id")
        if post_id in scanned_roots:
            continue
        scanned_roots.add(post_id)
        
        try:
            result = forum_read(root_id=post_id, max_pages=3)
            if not result.get("success") or not result.get("posts"):
                continue
            
            _flog(f"SCAN: wątek {post_id} → {result['count']} postów")
            
            for post in result["posts"]:
                if not _is_ai_post(post):
                    continue
                text = post.get("Text", "")
                if nrzam not in text:
                    continue
                
                post_forum_id = post.get("Id")
                if not post_forum_id:
                    continue
                
                text_lower = text.lower()
                matched_cel = None
                for c, cinfo in FORUM_THREADS.items():
                    if cinfo.get("post_id") != post_id:
                        continue
                    if "AUTOS_KURIERZY" == c and ("kurier" in text_lower or "zlecenie kuri" in text_lower or "etykiet" in text_lower):
                        matched_cel = c
                        break
                    elif "CZATOSZTUR_" in c and ("delegacja" in text_lower or "telefon" in text_lower):
                        matched_cel = c
                        break
                    elif "SPEDYCJA" in c and ("spedycj" in text_lower or "reklamacj" in text_lower):
                        matched_cel = c
                        break
                    elif "NIEPOZAMYKANE" in c and ("austausch" in text_lower or "zielonk" in text_lower):
                        matched_cel = c
                        break
                
                if not matched_cel:
                    for c, cinfo in FORUM_THREADS.items():
                        if cinfo.get("post_id") == post_id:
                            matched_cel = c
                            break
                
                if matched_cel and matched_cel not in found:
                    is_root = post.get("Do_Odpid") == 0 or post.get("Level") == 0
                    _flog(f"SCAN FOUND: {matched_cel} → forum_id={post_forum_id}, is_root={is_root}")
                    found[matched_cel] = {
                        "id": post_forum_id,
                        "new_subthread": is_root,
                        "co": f"scan: {matched_cel}",
                    }
                    try:
                        save_forum_memory(db, col_fn, nrzam, matched_cel, post_forum_id, f"scan: {matched_cel}")
                    except Exception:
                        pass
        except Exception as e:
            _flog(f"SCAN ERROR: {e}")
            continue
    
    _flog(f"SCAN RESULT: {list(found.keys()) if found else 'nic'}")
    return found if found else None
