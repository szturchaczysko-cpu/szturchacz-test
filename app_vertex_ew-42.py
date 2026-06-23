import requests
import streamlit as st
import vertexai
from vertexai.generative_models import GenerativeModel, ChatSession, Content, Part
from vertexai.preview import caching as vertex_caching
import google.auth
from google.oauth2 import service_account
from datetime import datetime, timedelta
import locale, time, json, re, pytz, hashlib, random
import firebase_admin
from firebase_admin import credentials, firestore
from streamlit_cookies_manager import EncryptedCookieManager

# --- MODUŁ FORUM ---
try:
    from forum_module import execute_forum_actions, discover_roots, auto_load_forum_context, save_forum_memory, load_forum_memory
    FORUM_ENABLED = True
except ImportError:
    FORUM_ENABLED = False

# --- TEST MODE ---
# True = kolekcje z prefixem "test_" (nie dotyka produkcji)
# False = produkcja (normalne kolekcje)
TEST_MODE = True
_COL_PREFIX = "test_" if TEST_MODE else ""
def col(name):
    """Prefixuje nazwę kolekcji w trybie testowym."""
    return f"{_COL_PREFIX}{name}"

# --- 0. KONFIGURACJA ŚRODOWISKA ---
try: locale.setlocale(locale.LC_TIME, "pl_PL.UTF-8")
except: pass

# --- 1. POŁĄCZENIA (TEST — bez routera) ---
if not firebase_admin._apps:
    creds_dict = json.loads(st.secrets["FIREBASE_CREDS"])
    cred = credentials.Certificate(creds_dict)
    firebase_admin.initialize_app(cred)
db = firestore.client()
cookies = None

# Pobieranie listy projektów z Secrets
try:
    GCP_PROJECTS = st.secrets["GCP_PROJECT_IDS"]
    if isinstance(GCP_PROJECTS, str): GCP_PROJECTS = [GCP_PROJECTS]
    GCP_PROJECTS = list(GCP_PROJECTS)
except:
    st.error("🚨 Błąd: Brak listy GCP_PROJECT_IDS w secrets!")
    st.stop()

# --- BRAMKA LOGOWANIA Z HASŁEM ---
if "operator" not in st.session_state:
    st.set_page_config(page_title="🧪 Szturchacz EW TEST", layout="wide", page_icon="🧪")
    st.title("🧪 Szturchacz EW TEST")
    
    _login_ops = [
        "Sylwia",
        "Emilia", "Oliwia", "Magda", "Ewelina", "Iwona", "Marlena",
        "EwelinaG", "Andrzej", "Marta", "Klaudia", "Kasia", "Romana",
        "oliwia_m",
    ]
    
    op_login = st.selectbox("Operator:", _login_ops, key="login_op_select")
    pw_login = st.text_input("Hasło:", type="password", key="login_pw_input")
    
    if st.button("🔓 Zaloguj", type="primary"):
        if not pw_login:
            st.error("❌ Wpisz hasło.")
        else:
            try:
                _cfg_data = db.collection(col("operator_configs")).document(op_login).get().to_dict() or {}
                _stored_pw = _cfg_data.get("password", "")
            except Exception:
                _stored_pw = ""
            
            if not _stored_pw:
                st.error("❌ Hasło nie zostało jeszcze nadane. Skontaktuj się z Sylwią.")
            elif pw_login != _stored_pw:
                st.error("❌ Nieprawidłowe hasło.")
            else:
                st.session_state.operator = op_login
                st.rerun()
    
    st.stop()

# ==========================================
# 🔑 CONFIG I TOŻSAMOŚĆ (identyczne jak prod)
# ==========================================
op_name = st.session_state.operator
cfg_ref = db.collection(col("operator_configs")).document(op_name)
cfg = cfg_ref.get().to_dict() or {}

# --- AUTO-SEED (test mode) — TYLKO jeśli operator nie ma jeszcze configu ---
# WAŻNE: nie nadpisuj istniejących ról! Inaczej każdy operator dostaje FR przy logowaniu.
if TEST_MODE and not cfg.get("role"):
    cfg = {
        "role": "Operatorzy_DE",  # default DE dla nowych operatorów
        "assigned_key_index": 1,
        "tel": OPERATORS_TEL.get(op_name, False) if "OPERATORS_TEL" in dir() else False,
    }
    cfg_ref.set(cfg, merge=True)

# --- PROJEKT GCP ---
fixed_key_idx = int(cfg.get("assigned_key_index", 1))
if fixed_key_idx < 1 or fixed_key_idx > len(GCP_PROJECTS):
    fixed_key_idx = 1
    st.warning(f"⚠️ Nieprawidłowy indeks projektu. Domyślnie: 1.")

project_index = fixed_key_idx - 1
current_gcp_project = GCP_PROJECTS[project_index]
st.session_state.vertex_project_index = project_index

# --- URL PROMPTU (kaskada: override → default warstwy B → cfg fallback) ---
# 1. Sprawdź czy operator ma override w panelu Prompty
PROMPT_URL = ""
PROMPT_NAME = ""
PROMPT_FILENAME = ""
PROMPT_GITHUB_LINK = ""
PROMPT_LAYER = ""

try:
    _ovr_doc = db.collection(col("operator_overrides")).document(op_name).get().to_dict() or {}
    if _ovr_doc.get("prompt_url"):
        PROMPT_URL = _ovr_doc.get("prompt_url", "")
        PROMPT_NAME = _ovr_doc.get("prompt_name", "")
        PROMPT_FILENAME = _ovr_doc.get("prompt_filename", "")
        PROMPT_GITHUB_LINK = _ovr_doc.get("prompt_github_link", "")
        PROMPT_LAYER = "C (override)"
except Exception:
    pass

# 2. Jeśli brak override — sprawdź default warstwy B
if not PROMPT_URL:
    try:
        _def_doc = db.collection(col("admin_config")).document("default_prompt").get().to_dict() or {}
        if _def_doc.get("prompt_url"):
            PROMPT_URL = _def_doc.get("prompt_url", "")
            PROMPT_NAME = _def_doc.get("prompt_name", "")
            PROMPT_FILENAME = _def_doc.get("prompt_filename", "")
            PROMPT_GITHUB_LINK = _def_doc.get("prompt_github_link", "")
            PROMPT_LAYER = "B (default)"
    except Exception:
        pass

# 3. Fallback do cfg (stara logika, dla operatorów którzy mają cfg.prompt_url ustawione)
if not PROMPT_URL:
    PROMPT_URL = cfg.get("prompt_url", "")
    PROMPT_NAME = cfg.get("prompt_name", "Nieprzypisany")
    PROMPT_LAYER = "A (prod fallback)"

# Zaktualizuj cfg żeby było dostępne w innych miejscach
cfg["prompt_url"] = PROMPT_URL
cfg["prompt_name"] = PROMPT_NAME
cfg["prompt_filename"] = PROMPT_FILENAME
cfg["prompt_github_link"] = PROMPT_GITHUB_LINK
cfg["prompt_layer"] = PROMPT_LAYER

# Toast przy zmianie prompta
_last_prompt = st.session_state.get("_last_prompt_url", None)
if _last_prompt and _last_prompt != PROMPT_URL:
    st.toast(f"📄 Zmieniono prompt na: {PROMPT_NAME}", icon="🔄")
st.session_state["_last_prompt_url"] = PROMPT_URL

if not PROMPT_URL:
    st.error("🚨 Brak przypisanego promptu! Poproś admina o ustawienie w Wieżowcu (zakładka Prompty).")
    st.stop()

# Inicjalizacja Vertex AI
if 'vertex_init_done' not in st.session_state or st.session_state.get('last_project') != current_gcp_project:
    try:
        creds_info = json.loads(st.secrets["FIREBASE_CREDS"])
        creds = service_account.Credentials.from_service_account_info(creds_info)
        vertexai.init(
            project=current_gcp_project,
            location=st.secrets["GCP_LOCATION"],
            credentials=creds
        )
        st.session_state.vertex_init_done = True
        st.session_state.last_project = current_gcp_project
    except Exception as e:
        st.error(f"Błąd inicjalizacji Vertex AI ({current_gcp_project}): {e}")
        st.stop()

# --- MAPOWANIE GRUPY OPERATORA ---
ROLE_TO_GRUPA = {
    "Operatorzy_DE": "DE",
    "Operatorzy_FR": "FR",
    "Operatorzy_UK/PL": "UKPL",
}
operator_grupa = ROLE_TO_GRUPA.get(cfg.get("role", "Operatorzy_DE"), "DE")

# --- FUNKCJE POMOCNICZE (identyczne jak prod) ---
def parse_pz(text):
    if not text: return None
    match = re.search(r'(PZ\d+)', text, re.IGNORECASE)
    if match: return match.group(1).upper()
    return None

def log_stats(op_name, start_pz, end_pz, proj_idx):
    tz_pl = pytz.timezone('Europe/Warsaw')
    today = datetime.now(tz_pl).strftime("%Y-%m-%d")
    time_str = datetime.now(tz_pl).strftime("%H:%M")
    doc_ref = db.collection(col("stats")).document(today).collection("operators").document(op_name)
    upd = {
        "sessions_completed": firestore.Increment(1),
        "session_times": firestore.ArrayUnion([time_str])
    }
    if start_pz and end_pz:
        upd[f"pz_transitions.{start_pz}_to_{end_pz}"] = firestore.Increment(1)
        if end_pz == "PZ6":
            db.collection(col("global_stats")).document("totals").collection("operators").document(op_name).set({"total_diamonds": firestore.Increment(1)}, merge=True)
    doc_ref.set(upd, merge=True)
    db.collection(col("key_usage")).document(today).set({str(proj_idx + 1): firestore.Increment(1)}, merge=True)


# ==========================================
# 🏢 FUNKCJE WIEŻOWCA (NOWE)
# ==========================================
OPERATORS_TEL = {
    "Emilia": True, "Oliwia": True, "Magda": True, "Ewelina": True,
    "Marta": True, "Klaudia": True, "Kasia": True, "oliwia_m": True,
    "Iwona": False, "Marlena": False, "Sylwia": False,
    "EwelinaG": False, "Andrzej": False, "Romana": False,
}

OPERATORS_TEL_JEZYKI = {
    "Emilia": ["DE"], "Magda": ["DE"], "Ewelina": ["DE"], "Klaudia": ["DE"],
    "Oliwia": ["EN"], "Marta": ["PL"], "Kasia": ["FR"], "oliwia_m": ["DE"],
}

MAIL_INICJALY = {
    "Emilia": "EM", "Oliwia": "OL", "Magda": "MG", "Ewelina": "EW",
    "Marta": "MA", "Klaudia": "KL", "Kasia": "KS", "oliwia_m": "OM",
    "Iwona": "IW", "Marlena": "ML", "Sylwia": "SY",
    "EwelinaG": "EG", "Andrzej": "AN", "Romana": "RO",
}

# Mapowanie: nazwa operatora w Stats/UI → nick używany na forum i w prompcie
# Niektórzy operatorzy mają inne nicki na forum (np. Kasia loguje się jako "Kasia", ale na forum jest "kasia_k")
OPERATOR_FORUM_NICK = {
    "Kasia": "kasia_k",
    # oliwia_m loguje się tak samo jak na forum — nick = login, mapowanie nie potrzebne
}

def get_forum_nick(op_name):
    """Zwraca nick forum dla operatora. Jeśli nie ma mapowania → używa op_name bezpośrednio."""
    return OPERATOR_FORUM_NICK.get(op_name, op_name)

# Pełna lista operatorów do dropdown logowania
ALL_OPERATORS = [
    "Sylwia",  # admin też loguje się jako operator
    "Emilia", "Oliwia", "Magda", "Ewelina", "Iwona", "Marlena",
    "EwelinaG", "Andrzej", "Marta", "Klaudia", "Kasia", "Romana",
    "oliwia_m",  # nowa — operator + sprzedawca
]


def ew_get_next_case(grupa, op_name):
    skipped_ids = st.session_state.get("ew_skipped_ids", set())
    my_tel = OPERATORS_TEL.get(op_name, False)
    
    try:
        q = (db.collection(col("ew_cases"))
             .where("grupa", "==", grupa)
             .where("status", "==", "wolny")
             .limit(500))
        all_free_raw = q.get()
        all_free_raw = sorted(all_free_raw, key=lambda d: d.to_dict().get("score", 0), reverse=True)
        all_free = [d for d in all_free_raw if d.id not in skipped_ids]
    except Exception:
        return None
    
    if not all_free:
        return None
    
    prio1 = []  
    prio2 = []  
    prio3 = []  
    prio4 = []  
    
    for d in all_free:
        data = d.to_dict()
        # Pomiń forum_action — czeka na uwolnienie nocnych ruchów
        if data.get("autopilot_status") == "forum_action":
            continue
        assigned_op = data.get("autopilot_assigned_to", "")
        is_calculated = data.get("autopilot_status") == "calculated"
        
        if is_calculated and assigned_op == op_name:
            prio1.append(d)
        elif is_calculated and assigned_op:
            other_tel = OPERATORS_TEL.get(assigned_op, False)
            if other_tel == my_tel:
                prio2.append(d)
            else:
                # Bez względu na TEL — case przypisany komuś innemu trafia do prio4
                prio4.append(d)
        else:
            prio3.append(d)
    
    doc = None
    for candidates in [prio1, prio2, prio3, prio4]:
        if candidates:
            doc = candidates[0] 
            break
    
    if not doc:
        return None
    
    db.collection(col("ew_cases")).document(doc.id).update({
        "status": "przydzielony",
        "assigned_to": op_name,
        "assigned_at": firestore.SERVER_TIMESTAMP,
    })
    data = doc.to_dict()
    data["_doc_id"] = doc.id
    return data

def ew_get_fresh_case(grupa, op_name):
    """Pobierz case ŚWIEŻAK.
    
    Logika:
    1. Najpierw szuka NIEPRZELICZONEGO (autopilot_status=null/"")
    2. Jeśli brak — fallback do CALCULATED (przelicz ponownie)
    3. Pomija forum_action (czeka na uwolnienie)
    """
    skipped_ids = st.session_state.get("ew_skipped_ids", set())
    
    try:
        q = (db.collection(col("ew_cases"))
             .where("grupa", "==", grupa)
             .where("status", "==", "wolny")
             .limit(500))
        all_free_raw = q.get()
        all_free_raw = sorted(all_free_raw, key=lambda d: d.to_dict().get("score", 0), reverse=True)
        all_free = [d for d in all_free_raw if d.id not in skipped_ids]
    except Exception:
        return None
    
    # Krok 1: nieprzeliczone (priorytet)
    doc = None
    fresh_source = None
    for d in all_free:
        data = d.to_dict()
        ap_status = data.get("autopilot_status")
        if ap_status in (None, "", "bumped"):
            doc = d
            fresh_source = "nieprzeliczony"
            break
    
    # Krok 2: fallback do calculated (przelicz ponownie)
    if not doc:
        for d in all_free:
            data = d.to_dict()
            ap_status = data.get("autopilot_status")
            if ap_status == "calculated":
                doc = d
                fresh_source = "calculated_recalc"
                break
    
    if not doc:
        return None
    
    update_data = {
        "status": "przydzielony",
        "assigned_to": op_name,
        "assigned_at": firestore.SERVER_TIMESTAMP,
        "processed_via": "swiezak_pending",
    }
    if fresh_source == "calculated_recalc":
        update_data["autopilot_messages"] = firestore.DELETE_FIELD
    
    db.collection(col("ew_cases")).document(doc.id).update(update_data)
    data = doc.to_dict()
    data["_doc_id"] = doc.id
    data["_is_swiezak"] = True
    data["_swiezak_source"] = fresh_source
    if fresh_source == "calculated_recalc":
        data.pop("autopilot_messages", None)
    return data


def ew_count_fresh_available(grupa):
    """Licz nieprzeliczone + calculated casy w grupie."""
    try:
        q = (db.collection(col("ew_cases"))
             .where("grupa", "==", grupa)
             .where("status", "==", "wolny")
             .limit(500))
        all_free = q.get()
        unprocessed = 0
        calculated = 0
        for d in all_free:
            ap = d.to_dict().get("autopilot_status")
            if ap in (None, "", "bumped"):
                unprocessed += 1
            elif ap == "calculated":
                calculated += 1
        return {"unprocessed": unprocessed, "calculated": calculated, "total": unprocessed + calculated}
    except Exception:
        return {"unprocessed": 0, "calculated": 0, "total": 0}


def ew_get_pool_diagnostics(grupa, op_name):
    """Diagnostyka puli casów — co się dzieje gdy operator dostaje 'Brak wolnych'."""
    skipped_ids = st.session_state.get("ew_skipped_ids", set())
    diag = {
        "grupa": grupa,
        "total_wolne": 0,
        "forum_action": 0,
        "nieprzeliczone": 0,
        "calculated_dla_mnie": 0,
        "calculated_dla_innych_TEL_ten_sam": 0,
        "calculated_dla_innych_TEL_inny": 0,
        "skipped_w_sesji": len(skipped_ids),
    }
    my_tel = OPERATORS_TEL.get(op_name, False)
    try:
        q = (db.collection(col("ew_cases"))
             .where("grupa", "==", grupa)
             .where("status", "==", "wolny")
             .limit(500))
        all_free = q.get()
        diag["total_wolne"] = len(all_free)
        
        for d in all_free:
            if d.id in skipped_ids:
                continue
            data = d.to_dict()
            ap = data.get("autopilot_status")
            assigned = data.get("autopilot_assigned_to", "")
            
            if ap == "forum_action":
                diag["forum_action"] += 1
            elif ap in (None, "", "bumped"):
                diag["nieprzeliczone"] += 1
            elif ap == "calculated":
                if assigned == op_name:
                    diag["calculated_dla_mnie"] += 1
                elif assigned:
                    other_tel = OPERATORS_TEL.get(assigned, False)
                    if other_tel == my_tel:
                        diag["calculated_dla_innych_TEL_ten_sam"] += 1
                    else:
                        diag["calculated_dla_innych_TEL_inny"] += 1
                else:
                    diag["nieprzeliczone"] += 1
    except Exception:
        pass
    return diag


def ew_restore_active_case(grupa, op_name):
    try:
        q = (db.collection(col("ew_cases"))
             .where("assigned_to", "==", op_name)
             .limit(10))
        results = q.get()
        for doc in results:
            data = doc.to_dict()
            if data.get("status") in ("przydzielony", "w_toku"):
                data["_doc_id"] = doc.id
                return data
    except Exception:
        pass
    return None

def ew_complete_case(case_doc_id, result_tag=None, result_pz=None):
    upd = {"status": "zakonczony", "completed_at": firestore.SERVER_TIMESTAMP}
    if result_tag: upd["result_tag"] = result_tag
    if result_pz: upd["result_pz"] = result_pz
    db.collection(col("ew_cases")).document(case_doc_id).update(upd)

def ew_release_case(case_doc_id):
    db.collection(col("ew_cases")).document(case_doc_id).update({
        "status": "wolny",
        "assigned_to": None,
        "assigned_at": None,
    })

def ew_count_available(grupa):
    return len(db.collection(col("ew_cases"))
               .where("grupa", "==", grupa)
               .where("status", "==", "wolny")
               .limit(500).get())

def _norm_ymd(v):
    """Data (str / date / Timestamp) → 'YYYY-MM-DD' albo None."""
    if not v:
        return None
    if isinstance(v, str):
        return v.strip()[:10]
    try:
        return v.strftime("%Y-%m-%d")
    except Exception:
        return str(v)[:10]

def _get_case_data_obrobki(doc_id):
    """Pobiera data_obrobki case'a z puli (do klasyfikacji z planu / poza planem)."""
    if not doc_id:
        return None
    try:
        d = db.collection(col("ew_cases")).document(doc_id).get()
        if getattr(d, "exists", False):
            return (d.to_dict() or {}).get("data_obrobki")
    except Exception:
        pass
    return None

def ew_log_completion(op_name, channel="standard", grupa=None, data_obrobki=None):
    """Trwała kronika zakończenia ruchu (przeżywa czyszczenie puli ew_cases).

    channel: 'standard' | 'wa' | 'mail' | 'forum' — rozróżnia ruch standardowy od
             wsadu odwrotnego (WA/MAIL/FORUM). cases_completed liczy WSZYSTKIE ruchy
             (standard+odwrotne) — to mianownik skuteczności; pola kanałowe to rozbicie.
    grupa:   DE/FR/UKPL (grupa CASE'a) — do trwałego licznika grupowego gz_{grupa}.
    data_obrobki: dzień PLANU case'a ('YYYY-MM-DD'). == dziś → ruch „z planu" (gz_ rośnie).
             ≠ dziś lub brak → ruch „poza planem": operatorowi rośnie poza_planem (per RUCH,
             ten sam case kilka razy = tyle razy), a gz_ NIE rośnie — dzięki temu „% z planu
             na dziś" nie przekracza 100% (zaległości nie wpadają do dzisiejszego planu).

    Pola istniejące (cases_completed, completion_times) NIETKNIĘTE — tylko dokładamy.
    """
    tz_pl = pytz.timezone('Europe/Warsaw')
    today = datetime.now(tz_pl).strftime("%Y-%m-%d")
    time_str = datetime.now(tz_pl).strftime("%H:%M")
    ch = channel if channel in ("standard", "wa", "mail", "forum") else "standard"
    on_plan = (_norm_ymd(data_obrobki) == today)   # case zaplanowany na DZIŚ?
    upd = {
        "cases_completed": firestore.Increment(1),
        "completion_times": firestore.ArrayUnion([time_str]),
        f"cases_completed_{ch}": firestore.Increment(1),
    }
    if grupa:
        upd["grupa"] = grupa
    if not on_plan:
        upd["poza_planem"] = firestore.Increment(1)   # ruch na case spoza DZISIEJSZEGO planu
    db.collection(col("ew_operator_stats")).document(today).collection("operators").document(op_name).set(upd, merge=True)
    # Trwały licznik grupowy (parent doc) — TYLKO gdy case był w DZISIEJSZYM planie.
    if grupa in ("DE", "FR", "UKPL") and on_plan:
        try:
            db.collection(col("ew_operator_stats")).document(today).set(
                {f"gz_{grupa}": firestore.Increment(1)}, merge=True
            )
        except Exception:
            pass

def ew_log_taken(op_name):
    tz_pl = pytz.timezone('Europe/Warsaw')
    today = datetime.now(tz_pl).strftime("%Y-%m-%d")
    db.collection(col("ew_operator_stats")).document(today).collection("operators").document(op_name).set({
        "cases_taken": firestore.Increment(1),
    }, merge=True)

def ew_log_skipped(op_name, grupa=None):
    tz_pl = pytz.timezone('Europe/Warsaw')
    today = datetime.now(tz_pl).strftime("%Y-%m-%d")
    db.collection(col("ew_operator_stats")).document(today).collection("operators").document(op_name).set({
        "cases_skipped": firestore.Increment(1),
    }, merge=True)
    # Trwały licznik grupowy pominięć (parent doc) — wg grupy CASE'a
    if grupa in ("DE", "FR", "UKPL"):
        try:
            db.collection(col("ew_operator_stats")).document(today).set(
                {f"gp_{grupa}": firestore.Increment(1)}, merge=True
            )
        except Exception:
            pass

def ew_log_phone(op_name, numer, wynik, grupa=None, kurier_ustalony=False,
                 zrodlo="operator_dzwoniacy", pz=None, typ_towaru=None, kurier_przewoznik=None):
    """Trwały log TELEFONU — osobny ruch, NIE wpada do cases_completed („przerobione").
    Źródło prawdy modułu Telefony. Liczniki naliczają się od wdrożenia (brak historii).

    wynik: 'brak_kontaktu' | 'kontakt_bez_konkretu' | 'przelozenie' | 'konkret'
           (efektywny = 'konkret'; diamentofon = konkret + kurier — dopinany po numerze w Wieżowcu)
    kurier_ustalony: telefon dobił do zlecenia kuriera (kandydat na diamentofon).
    zrodlo: 'operator_dzwoniacy' (śledzony imiennie) | 'telefonista_zewn' (kubełek per kraj).
    """
    tz_pl = pytz.timezone('Europe/Warsaw')
    today = datetime.now(tz_pl).strftime("%Y-%m-%d")
    time_str = datetime.now(tz_pl).strftime("%H:%M")
    wyniki_ok = ("brak_kontaktu", "kontakt_bez_konkretu", "przelozenie", "konkret")
    w = wynik if wynik in wyniki_ok else "kontakt_bez_konkretu"
    zr = zrodlo if zrodlo in ("operator_dzwoniacy", "telefonista_zewn") else "operator_dzwoniacy"
    doc = {
        "numer_zamowienia": str(numer or "").strip(),
        "operator": op_name,
        "grupa": grupa or "?",
        "wynik": w,
        "efektywny": (w == "konkret"),
        "kurier_ustalony": bool(kurier_ustalony) and (w == "konkret"),
        "zrodlo": zr,
        "pz": pz or "",
        "typ_towaru": typ_towaru or "",
        "kurier_przewoznik": kurier_przewoznik or "",
        "data_str": today,
        "godzina": time_str,
        "created_at": firestore.SERVER_TIMESTAMP,
    }
    db.collection(col("ew_phone_log")).document(today).collection("calls").add(doc)


# Checklisty telefoniczne per PZ — LUSTRO §8.5 promptu (te same pytania, które AI wstawia
# na forum dla telefonisty, widzi operator dzwoniący w aplikacji). ★ = punkt posuwający.
PHONE_CHECKLISTS = {
    "PZ1": [
        "Czy otrzymał skrzynię z nową częścią? (rozpoznawczy)",
        "★ Kiedy odbiór starej części — KONKRETNA data lub kotwica?",
        "Czy adres odbioru = adres dostawy nowej?",
        "ℹ️ Jest odpowiedni sposób pakowania",
        "ℹ️ Wyśle podsumowanie (mail/WA)",
        "★ KOLEKTOR/UPS: kurier po odbiór czy etykieta do punktu?",
    ],
    "PZ2": [
        "Czy zapoznał się z instrukcjami pakowania? (rozpoznawczy)",
        "★ KONKRETNY termin odbioru kuriera",
        "★ Jeśli nie poda daty — kotwica (kiedy poda)",
        "ℹ️ Wyśle podsumowanie (mail/WA)",
        "★ KOLEKTOR/UPS: odbiór czy punkt (jeśli nieustalone)",
    ],
    "PZ3A": [
        "Postęp pakowania — zdąży na termin?",
        "★ (FedEx) kiedy prześle zdjęcie spakowanej skrzyni",
        "★ Potwierdź ustaloną datę odbioru",
        "★ Jeśli nie zdąży — nowa data odbioru",
        "ℹ️ Wyśle podsumowanie. NIE wracaj do pakowania (było w PZ2)",
    ],
    "PZ3B": [
        "★ KONKRETNA data odbioru kuriera",
        "Jeśli dalej nie może — nowa kotwica",
        "ℹ️ Wyśle podsumowanie (mail/WA)",
    ],
    "PZ4": [
        "(UPS) telefon zwykle ZBĘDNY — czekamy na PZ6",
        "★ (FedEx) kiedy prześle zdjęcie",
        "ℹ️ Bez zdjęcia nie zlecimy kuriera",
        "★ (FedEx) KONKRETNA data dosłania zdjęcia",
        "ℹ️ Wyśle podsumowanie (mail/WA)",
    ],
    "PZ5": [
        "ℹ️ Co nie tak ze zdjęciem (brak dekla / zła skrzynia / złe zabezpieczenie)",
        "ℹ️ Co poprawić",
        "★ Kiedy prześle NOWE zdjęcie",
        "ℹ️ Wyśle podsumowanie (mail/WA)",
    ],
    "PZ6": ["Brak telefonu — etap wewnętrzny (atomówki)"],
    "PZ7": ["Brak telefonu — etap wewnętrzny"],
    "PZ8": ["Brak telefonu w standardzie — wysyłka mail/WA"],
    "PZ9": [
        "Szczegóły problemu (np. odmówił kuriera)",
        "★ KONKRETNA data nowej próby odbioru",
        "(UPS) która próba z 3",
        "ℹ️ Wyśle podsumowanie (mail/WA)",
    ],
    "PZ10": ["Brak telefonu — kurier odebrał"],
    "PZ11": [
        "Tylko gdy magazyn zgłosi problem:",
        "ℹ️ Problem z otrzymanym towarem",
        "★ Czy wysłał WŁAŚCIWĄ część?",
    ],
    "PZ12": ["Brak telefonu — rozliczone"],
}

def _phone_checklist_for(pz):
    key = (pz or "").upper().strip().replace(" ", "")
    if key in PHONE_CHECKLISTS:
        return PHONE_CHECKLISTS[key]
    return PHONE_CHECKLISTS["PZ1"]  # PZ0/brak → jak pierwszy kontakt


    m = re.search(r'(C#:\d{2}\.\d{2};PZ=\S+?;DRABE[S]?=\S+)', text)
    if m:
        tag = m.group(1)
        pz_m = re.search(r'PZ=(\S+?)(?:[;\s]|$)', tag)
        return tag, pz_m.group(1) if pz_m else None
    m = re.search(r'(C#:\d{2}\.\d{2};PZ=\S+)', text)
    if m:
        tag = m.group(1)
        pz_m = re.search(r'PZ=(\S+?)(?:[;\s]|$)', tag)
        return tag, pz_m.group(1) if pz_m else None
    m = re.search(r'(C#:\d{2}\.\d{2}_\S+_\d{2}\.\d{2})', text)
    if m:
        return m.group(1), parse_pz(text)
    return None, None

def ew_find_case_by_nrzam(nrzam, op_name):
    results = db.collection(col("ew_cases")).where("numer_zamowienia", "==", nrzam).limit(5).get()
    if not results:
        return None, "not_found"
    
    best = None
    best_status = None
    for doc in results:
        data = doc.to_dict()
        data["_doc_id"] = doc.id
        status = data.get("status", "wolny")
        
        if status == "wolny":
            db.collection(col("ew_cases")).document(doc.id).update({
                "status": "przydzielony",
                "assigned_to": op_name,
                "assigned_at": firestore.SERVER_TIMESTAMP,
            })
            return data, "reserved"
        elif status in ("przydzielony", "w_toku") and data.get("assigned_to") == op_name:
            return data, "already_mine"
        elif status in ("przydzielony", "w_toku"):
            best = data
            best_status = "taken_by_other"
        elif status == "zakonczony":
            if not best:
                best = data
                best_status = "completed"
    
    return best, best_status


# ==========================================
# INICJALIZACJA STANÓW EW
# ==========================================
if "ew_current_case" not in st.session_state:
    restored = ew_restore_active_case(operator_grupa, op_name)
    if restored:
        st.session_state.ew_current_case = restored
    else:
        st.session_state.ew_current_case = None
if "ew_wsad_ready" not in st.session_state:
    st.session_state.ew_wsad_ready = ""          
if "ew_skipped_ids" not in st.session_state:
    st.session_state.ew_skipped_ids = set()      
if "chat_nrzam" not in st.session_state:
    st.session_state.chat_nrzam = None


# ==========================================
# 🚀 SIDEBAR
# ==========================================
global_cfg = db.collection(col("admin_config")).document("global_settings").get().to_dict() or {}
show_diamonds = global_cfg.get("show_diamonds", True)
caching_enabled = global_cfg.get("context_caching_enabled", False)


def get_or_create_cached_model(model_id, system_prompt):
    import hashlib
    from datetime import timedelta as td
    
    cache_key = hashlib.md5(f"{model_id}:{system_prompt[:500]}".encode()).hexdigest()[:12]
    session_key = f"vertex_cache_{cache_key}"
    
    cached_name = st.session_state.get(session_key)
    if cached_name:
        try:
            cached_content = vertex_caching.CachedContent(cached_content_name=cached_name)
            model = GenerativeModel.from_cached_content(cached_content=cached_content)
            return model
        except Exception:
            st.session_state.pop(session_key, None)
    
    try:
        cached_content = vertex_caching.CachedContent.create(
            model_name=model_id,
            system_instruction=system_prompt,
            contents=[],  
            ttl=td(minutes=60),
            display_name=f"ew-{cache_key}",
        )
        st.session_state[session_key] = cached_content.name
        model = GenerativeModel.from_cached_content(cached_content=cached_content)
        return model
    except Exception as e:
        st.toast(f"⚠️ Cache niedostępny: {str(e)[:100]}. Tryb normalny.")
        return None

with st.sidebar:
    st.title(f"👤 {op_name}")
    
    if st.button("🧹 Reset (czysty start)"):
        st.session_state.ew_current_case = None
        st.session_state.ew_wsad_ready = ""
        st.session_state.messages = []
        st.session_state.chat_started = False
        st.session_state.current_start_pz = None
        st.session_state._autopilot_loaded = False
        st.session_state.forum_debug_log = []
        st.session_state.chat_nrzam = None
        st.rerun()

    st.markdown(f"**🔑 Projekt:** `{current_gcp_project}`")
    st.markdown(f"**📄 Prompt:** `{PROMPT_NAME}`")
    st.markdown("---")

    if show_diamonds:
        tz_pl = pytz.timezone('Europe/Warsaw')
        today_s = datetime.now(tz_pl).strftime("%Y-%m-%d")
        today_data = db.collection(col("stats")).document(today_s).collection("operators").document(op_name).get().to_dict() or {}
        today_diamonds = sum(v for k, v in today_data.get("pz_transitions", {}).items() if k.endswith("_to_PZ6"))
        global_data = db.collection(col("global_stats")).document("totals").collection("operators").document(op_name).get().to_dict() or {}
        all_time_diamonds = global_data.get("total_diamonds", 0)
        st.markdown(f"### 💎 Zamówieni kurierzy\n**Dziś:** {today_diamonds} | **Łącznie:** {all_time_diamonds}")
        st.markdown("---")

    st.markdown("---")
    TRYBY_DICT = {"Standard": "od_szturchacza", "WA": "WA", "MAIL": "MAIL", "FORUM": "FORUM"}
    st.selectbox("Tryb Startowy:", list(TRYBY_DICT.keys()), key="tryb_label")
    wybrany_tryb_kod = TRYBY_DICT[st.session_state.tryb_label]
    
    st.markdown("---")
    czyste_okno = st.checkbox("🧪 Czyste okno (wklej wsad ręcznie)", value=False, key="czyste_okno")
    if czyste_okno:
        if st.button("🗑️ Wyczyść debug log + chat"):
            st.session_state.forum_debug_log = []
            st.session_state.messages = []
            st.session_state.chat_started = False
            st.session_state.ew_current_case = None
            st.session_state.ew_wsad_ready = ""
            st.session_state.current_start_pz = None
            st.session_state._autopilot_loaded = False
            st.session_state.chat_nrzam = None
            st.rerun()
        st.caption("Wieżowiec wyłączony. Wklej wsad w głównym panelu.")
    
    if not czyste_okno:
        st.subheader(f"🏢 Wieżowiec ({operator_grupa})")
        avail = ew_count_available(operator_grupa)
        st.caption(f"Wolne casy: **{avail}**")

        ew_today = db.collection(col("ew_operator_stats")).document(
            datetime.now(pytz.timezone('Europe/Warsaw')).strftime("%Y-%m-%d")
        ).collection("operators").document(op_name).get().to_dict() or {}
        st.caption(f"🏢 Zakończone dziś: **{ew_today.get('cases_completed', 0)}**")

        autopilot_on = cfg.get("autopilot_enabled", False)
        if wybrany_tryb_kod in ("WA", "MAIL", "FORUM"):
            st.caption(f"🤖 Autopilot: **OFF** (tryb {wybrany_tryb_kod})")
        elif autopilot_on:
            st.caption("🤖 Autopilot: **ON** — nocne przeliczenia będą ładowane")
        else:
            st.caption("🤖 Autopilot: **OFF** — każdy case od zera")

        current_case = st.session_state.ew_current_case
        show_current_case = current_case and (
            wybrany_tryb_kod == "od_szturchacza"  
            or current_case.get("_reverse_mode", False)  
        )
        
        if current_case and wybrany_tryb_kod in ("WA", "MAIL", "FORUM") and not current_case.get("_reverse_mode", False):
            st.caption(f"ℹ️ Case {current_case.get('numer_zamowienia', '?')} czeka (tryb Standard). Przełącz na Standard by kontynuować.")
        
        if show_current_case:
            case = st.session_state.ew_current_case
            is_reverse = case.get("_reverse_mode", False)
            is_swiezak = case.get("_is_swiezak") or case.get("processed_via") == "swiezak_pending"
            reverse_label = f" 📨 {case.get('_reverse_type', '')}" if is_reverse else ""
            autopilot_label = " 🤖" if case.get("autopilot_status") == "calculated" else ""
            swiezak_label = " ✨ ŚWIEŻAK" if is_swiezak else ""
            st.info(f"📌 Case: **{case.get('numer_zamowienia', '?')}**{reverse_label}{autopilot_label}{swiezak_label}\n"
                    f"{case.get('priority_icon', '')} [{case.get('score', 0)}]")
            if is_swiezak:
                _sw_prompt = cfg.get("prompt_name", "?")
                st.success(f"✨ **Świeżak** — będzie liczony promptem: **{_sw_prompt}**")
            if case.get("autopilot_status") == "calculated" and not is_swiezak:
                if is_reverse:
                    st.caption(f"🤖 Przeliczone nocą, ale tryb **{case.get('_reverse_type', '')}** → start od zera (nowa instancja kanałowa)")
                elif cfg.get("autopilot_enabled", False):
                    st.caption("🤖 Pierwszy ruch przeliczony — kliknij ▶️ by załadować gotową analizę")
                else:
                    st.caption("🤖 Przeliczone nocą (autopilot OFF — będzie liczone od zera)")

            if not st.session_state.get("chat_started"):
                if is_reverse:
                    st.caption("📨 Edytuj wsad i kliknij **🚀 Rozpocznij analizę** w głównym panelu →")
                else:
                    _col_start, _col_swiezak = st.columns(2)
                    _start_clicked = False
                    _swiezak_clicked = False
                    with _col_start:
                        if st.button("▶️ Rozpocznij ten case", type="primary", key="start_assigned_case"):
                            _start_clicked = True
                    with _col_swiezak:
                        if st.button("✨ Pobierz świeżaka", type="secondary", key="swiezak_on_assigned",
                                    help="Przelicz ten case od nowa świeżym promptem (zamiast nocnej analizy)"):
                            _swiezak_clicked = True
                    
                    if _swiezak_clicked:
                        case["_is_swiezak"] = True
                        case["processed_via"] = "swiezak_pending"
                        if case.get("_doc_id"):
                            db.collection(col("ew_cases")).document(case["_doc_id"]).update({
                                "processed_via": "swiezak_pending",
                                "autopilot_messages": firestore.DELETE_FIELD,
                            })
                        case.pop("autopilot_messages", None)
                        st.session_state.ew_current_case = case
                        st.toast("✨ Case będzie przeliczony świeżym promptem", icon="✨")
                        _start_clicked = True
                    
                    if _start_clicked:
                        wsad = case.get("pelna_linia_szturchacza", "")
                        if wsad:
                            nrzam = str(case.get("numer_zamowienia", ""))
                            # Reset forum debug log
                            st.session_state.forum_debug_log = []
                            st.session_state.chat_nrzam = nrzam
                            
                            _is_swiezak_case = case.get("_is_swiezak") or case.get("processed_via") == "swiezak_pending"
                            
                            if FORUM_ENABLED and nrzam:
                                forum_ctx = auto_load_forum_context(db, col, nrzam)
                                if forum_ctx:
                                    wsad = wsad + "\n\n" + forum_ctx
                                    st.toast(f"📖 Forum: załadowano kontekst dla {nrzam}")
                            
                            if case.get("_doc_id"):
                                db.collection(col("ew_cases")).document(case["_doc_id"]).update({
                                    "status": "w_toku",
                                    "started_at": firestore.SERVER_TIMESTAMP,
                                })

                            autopilot_msgs = case.get("autopilot_messages") if not _is_swiezak_case else None
                            operator_autopilot_on = cfg.get("autopilot_enabled", False)
                            if (operator_autopilot_on
                                    and not is_reverse
                                    and not _is_swiezak_case
                                    and case.get("autopilot_status") == "calculated"
                                    and autopilot_msgs and len(autopilot_msgs) >= 2):
                                st.session_state.current_start_pz = parse_pz(wsad) or "PZ_START"
                                st.session_state.messages = autopilot_msgs
                                st.session_state.chat_started = True
                                st.session_state.ew_wsad_ready = ""
                                st.session_state._autopilot_loaded = True
                            else:
                                st.session_state.current_start_pz = parse_pz(wsad) or "PZ_START"
                                st.session_state.messages = [{"role": "user", "content": wsad}]
                                st.session_state.chat_started = True
                                st.session_state.ew_wsad_ready = ""
                                st.session_state._autopilot_loaded = False

                            if is_reverse:
                                st.session_state.ew_forced_tryb = case.get("_reverse_type", "WA")
                            else:
                                # standard — usuń ewentualny marker odwrotny z poprzedniej sesji
                                for _k in ("ew_reverse_active", "ew_reverse_no_pool", "ew_reverse_doc_id", "ew_reverse_grupa"):
                                    st.session_state.pop(_k, None)
                            st.rerun()
                        else:
                            st.error("Case nie ma wsadu!")

            st.markdown("---")
            skip_reason = st.text_area("💬 Powód pominięcia:", key="ew_skip_reason", max_chars=500, height=80, placeholder="np. brak danych, czekam na forum, klient nie odbiera...")
            if st.button("⏭️ Pomiń case"):
                if not skip_reason or not skip_reason.strip():
                    st.error("⚠️ Wpisz powód pominięcia — nie można pominąć bez komentarza!")
                else:
                    if case.get("_doc_id") and not case.get("_reverse_no_pool"):
                        db.collection(col("ew_cases")).document(case["_doc_id"]).update({
                            "status": "pominiety",
                            "assigned_to": None,
                            "assigned_at": None,
                            "skip_reason": skip_reason.strip(),
                            "skipped_by": op_name,
                            "skipped_at": firestore.SERVER_TIMESTAMP,
                        })
                        ew_log_skipped(op_name, grupa=case.get("grupa"))
                    st.session_state.ew_current_case = None
                    st.session_state.ew_wsad_ready = ""
                    st.session_state.messages = []
                    st.session_state.chat_started = False
                    st.session_state._autopilot_loaded = False
                    st.session_state.current_start_pz = None
                    st.session_state.chat_nrzam = None
                    st.rerun()

            st.markdown("---")
            # === ZAKOŃCZENIE ===
            # Reverse (WA/MAIL/FORUM): przycisk jest w GŁÓWNYM panelu (oparty na trwałym
            # markerze ew_reverse_active) — działa nawet gdy ew_current_case zniknie / case spoza puli.
            # Tu (sidebar) zostaje TYLKO standard, z auto-pobraniem następnego.
            _is_rev = case.get("_reverse_mode", False)
            _rev_type = case.get("_reverse_type", "")  # "WA" | "MAIL" | "FORUM"
            if _is_rev:
                st.info(f"📨 Tryb odwrotny ({_rev_type}). Po decyzji AI z TAG-KOPERTĄ kliknij "
                        f"**✅ Zakończ wsad odwrotny** pod rozmową w głównym panelu →")
            elif st.button("✅ Zakończ → Następny"):
                tag, pz = None, None
                msgs = st.session_state.get("messages", [])
                for m in reversed(msgs):
                    if m.get("role") == "model":
                        tag, pz = detect_tag_in_response(m.get("content", ""))
                        if tag:
                            break

                if tag:
                    if case.get("_doc_id"):
                        ew_complete_case(case["_doc_id"], result_tag=tag, result_pz=pz)
                    start_pz = st.session_state.get("current_start_pz", None)
                    end_pz = pz
                    proj_idx = st.session_state.get("current_project_idx", 0)
                    log_stats(op_name, start_pz, end_pz, proj_idx)
                    ew_log_completion(op_name, channel="standard", grupa=case.get("grupa"),
                                      data_obrobki=case.get("data_obrobki") or _get_case_data_obrobki(case.get("_doc_id")))
                    st.session_state.messages = []
                    st.session_state.chat_started = False
                    st.session_state.current_start_pz = None
                    st.session_state._autopilot_loaded = False
                    st.session_state.chat_nrzam = None
                    st.session_state.ew_wsad_ready = ""
                    new_case = ew_get_next_case(operator_grupa, op_name)
                    st.session_state.ew_current_case = new_case
                    if new_case:
                        ew_log_taken(op_name)
                        st.rerun()
                    else:
                        st.success("✅ Case zakończony! Brak kolejnych casów w puli.")
                        st.rerun()
                else:
                    st.error("❌ Brak TAGu w odpowiedzi AI — nie można zakończyć. Kontynuuj rozmowę z AI.")

        if not show_current_case:
            if wybrany_tryb_kod in ("WA", "MAIL", "FORUM"):
                st.markdown(f"📨 **Wsad odwrotny: {wybrany_tryb_kod}**")
                nrzam_input = st.text_input("Podaj NrZam:", key="ew_reverse_nrzam", placeholder="np. 369771")
                
                if st.button(f"🔍 Szukaj case'a ({wybrany_tryb_kod})", type="primary"):
                    if nrzam_input.strip():
                        case_data, status = ew_find_case_by_nrzam(nrzam_input.strip(), op_name)
                        
                        if status == "reserved":
                            case_data["_reverse_mode"] = True
                            case_data["_reverse_type"] = wybrany_tryb_kod
                            st.session_state.ew_current_case = case_data
                            st.success(f"✅ Znaleziono case **{nrzam_input}** — zarezerwowany!")
                            st.rerun()
                        elif status == "already_mine":
                            case_data["_reverse_mode"] = True
                            case_data["_reverse_type"] = wybrany_tryb_kod
                            st.session_state.ew_current_case = case_data
                            st.info(f"📌 Case **{nrzam_input}** już jest Twój.")
                            st.rerun()
                        elif status == "taken_by_other":
                            case_data["_reverse_mode"] = True
                            case_data["_reverse_type"] = wybrany_tryb_kod
                            case_data["_reverse_no_pool"] = True  # nie ruszamy cudzego przydziału
                            st.session_state.ew_current_case = case_data
                            st.warning(f"⚠️ Case **{nrzam_input}** jest przydzielony do: **{case_data.get('assigned_to', '?')}**. "
                                       f"Możesz rozliczyć ruch odwrotny — wklej/edytuj wsad w prawym panelu (przydział kolegi nietknięty).")
                            st.rerun()
                        elif status == "completed":
                            case_data["_reverse_mode"] = True
                            case_data["_reverse_type"] = wybrany_tryb_kod
                            case_data["_reverse_no_pool"] = True  # już zakończony — nie liczymy grupy ponownie
                            st.session_state.ew_current_case = case_data
                            st.info(f"ℹ️ Case **{nrzam_input}** jest zakończony. Ruch odwrotny zostanie rozliczony Tobie "
                                    f"(kanał {wybrany_tryb_kod}) — wklej/edytuj wsad w prawym panelu.")
                            st.rerun()
                        else:
                            # Nie znaleziono w puli (np. pula wyczyszczona) — sesja ręczna, bez dokumentu.
                            _manual_case = {
                                "numer_zamowienia": nrzam_input.strip(),
                                "pelna_linia_szturchacza": "",
                                "priority_icon": "📨",
                                "priority_label": "wsad odwrotny (ręczny)",
                                "score": 0,
                                "_reverse_mode": True,
                                "_reverse_type": wybrany_tryb_kod,
                                "_reverse_no_pool": True,
                            }
                            st.session_state.ew_current_case = _manual_case
                            st.warning(f"🔍 Nie znaleziono **{nrzam_input}** w puli — sesja ręczna. "
                                       f"Wklej wsad w prawym panelu; po zakończeniu ruch odwrotny ({wybrany_tryb_kod}) zostanie rozliczony.")
                            st.rerun()
                    else:
                        st.error("Podaj numer zamówienia!")
            
            else:
                if avail > 0:
                    _fresh_info = ew_count_fresh_available(operator_grupa)
                    _fresh_unproc = _fresh_info["unprocessed"]
                    _fresh_recalc = _fresh_info["calculated"]
                    _fresh_total = _fresh_info["total"]
                    
                    _col_norm, _col_fresh = st.columns(2)
                    with _col_norm:
                        if st.button("📥 Pobierz case", type="primary", key="get_normal_case"):
                            case = ew_get_next_case(operator_grupa, op_name)
                            if case:
                                ew_log_taken(op_name)
                                st.session_state.ew_current_case = case
                                st.rerun()
                            else:
                                _diag = ew_get_pool_diagnostics(operator_grupa, op_name)
                                _msg = (
                                    f"❌ **Brak wolnych casów do pobrania**, mimo że w grupie {operator_grupa} jest {_diag['total_wolne']} wolnych.\n\n"
                                    f"**Co się dzieje z casami:**\n"
                                    f"- 🌙 Czekają na uwolnienie nocnych ruchów: **{_diag['forum_action']}**\n"
                                    f"- ⚪ Nieprzeliczone (do dolania): **{_diag['nieprzeliczone']}**\n"
                                    f"- 🤖 Przeliczone dla Ciebie: **{_diag['calculated_dla_mnie']}**\n"
                                    f"- 🤖 Przeliczone dla operatora z tym samym TEL: **{_diag['calculated_dla_innych_TEL_ten_sam']}**\n"
                                    f"- 🤖 Przeliczone dla operatora z innym TEL: **{_diag['calculated_dla_innych_TEL_inny']}**\n"
                                    f"- ⏭️ Pominięte przez Ciebie w tej sesji: **{_diag['skipped_w_sesji']}**\n\n"
                                    f"💡 Użyj **'✨ Pobierz świeżaka'** żeby przeliczyć nieprzeliczonego od zera."
                                )
                                st.warning(_msg)
                    with _col_fresh:
                        if _fresh_unproc > 0:
                            _fresh_label = f"✨ Pobierz świeżaka ({_fresh_unproc})"
                            _fresh_help = f"Nieprzeliczonych: {_fresh_unproc}. Świeżak liczy świeżym promptem operatora."
                        elif _fresh_recalc > 0:
                            _fresh_label = f"✨ Pobierz świeżaka (przelicz)"
                            _fresh_help = f"Brak nieprzeliczonych. Świeżak weźmie przeliczonego i policzy od nowa świeżym promptem ({_fresh_recalc} dostępnych)."
                        else:
                            _fresh_label = "✨ Pobierz świeżaka"
                            _fresh_help = "Brak casów do przeliczenia świeżakiem."
                        
                        if st.button(_fresh_label, type="secondary", key="get_fresh_case", help=_fresh_help):
                            if _fresh_total == 0:
                                st.warning("Brak nieprzeliczonych i przeliczonych. Wszystkie wolne czekają na uwolnienie nocnego forum.")
                            else:
                                case = ew_get_fresh_case(operator_grupa, op_name)
                                if case:
                                    ew_log_taken(op_name)
                                    st.session_state.ew_current_case = case
                                    _src = case.get("_swiezak_source", "")
                                    if _src == "calculated_recalc":
                                        st.toast("✨ Przeliczam ponownie świeżym promptem", icon="✨")
                                    st.rerun()
                                else:
                                    st.warning("Brak casów do świeżaka.")
                else:
                    st.caption("🔍 Brak wolnych casów w Twojej grupie.")

        st.markdown("---")

    admin_msg = cfg.get("admin_message", "")
    if admin_msg and not cfg.get("message_read", False):
        st.error(f"📢 **WIADOMOŚĆ:**\n\n{admin_msg}")
        if st.button("✅ Odczytałem"):
            db.collection(col("operator_configs")).document(op_name).update({"message_read": True})
            st.rerun()

    st.markdown("---")
    ALL_MODELS = {
        "gemini-2.5-pro": "Gemini 2.5 Pro",
        "gemini-3-pro-preview": "Gemini 3 Pro (Preview)",
        "gemini-3.1-pro-preview": "Gemini 3.1 Pro (Preview)",
    }
    FALLBACK_CHAIN = ["gemini-2.5-pro", "gemini-3-pro-preview", "gemini-3.1-pro-preview"]
    
    allowed_models = global_cfg.get("allowed_models", ["gemini-2.5-pro", "gemini-3-pro-preview"])
    if isinstance(allowed_models, str):
        allowed_models = [allowed_models]
    allowed_models = [m for m in allowed_models if m in ALL_MODELS]
    if not allowed_models:
        allowed_models = ["gemini-2.5-pro"]
    
    model_labels = [ALL_MODELS[m] for m in allowed_models]
    st.radio("Model AI:", model_labels, key="selected_model_label")
    label_to_id = {v: k for k, v in ALL_MODELS.items()}
    active_model_id = label_to_id.get(st.session_state.selected_model_label, allowed_models[0])

    st.subheader("🧪 Funkcje Eksperymentalne")
    st.toggle("Tryb NOTAG (Tag-Koperta)", key="notag_val", value=True)
    st.toggle("Tryb ANALIZBIOR (Wsad zbiorczy)", key="analizbior_val", value=False)

    cache_icon = "⚡" if caching_enabled else ""
    st.caption(f"🧠 Model: `{active_model_id}` {cache_icon}")
    if caching_enabled:
        st.caption("⚡ Context Caching aktywny")

    st.markdown("---")

    # Slot na przycisk „Zakończ wsad odwrotny" — kontener tworzony TU (sidebar, nad „Nowa sprawa"),
    # ale wypełniany PÓŹNIEJ w skrypcie (po odpowiedzi AI), żeby widział TAG z ostatniej odpowiedzi.
    _rev_finish_container = st.container()

    if st.button("🚀 Nowa sprawa / Reset", type="primary"):
        _rc = st.session_state.get("ew_current_case")
        if _rc and _rc.get("_doc_id") and not _rc.get("_reverse_no_pool"):
            try:
                status = db.collection(col("ew_cases")).document(_rc["_doc_id"]).get().to_dict().get("status")
                if status == "przydzielony":
                    ew_release_case(_rc["_doc_id"])
            except Exception:
                pass
        st.session_state.ew_current_case = None
        st.session_state.messages = []
        st.session_state.chat_started = False
        st.session_state.current_start_pz = None
        st.session_state.ew_wsad_ready = ""
        st.session_state.chat_nrzam = None
        for _k in ("ew_reverse_active", "ew_reverse_no_pool", "ew_reverse_doc_id", "ew_reverse_grupa"):
            st.session_state.pop(_k, None)
        st.rerun()

    # ── 📞 Woreczek telefoniczny (moduł Telefony) ──
    with st.expander("📞 Woreczek telefoniczny", expanded=False):
        st.caption("Sprawy z delegowanym telefonem (FORUM_TEL=czekam_wynik). Zadzwoń wg checklisty, "
                   "potem zarejestruj wynik niżej i usuń z woreczka, gdy domknięte.")
        try:
            _woreczek = list(db.collection(col("ew_cases")).where("telefon_do_wykonania", "==", True).limit(30).stream())
        except Exception as _e:
            _woreczek = []
            st.caption(f"(nie udało się odczytać woreczka: {_e})")
        if not _woreczek:
            st.info("Woreczek pusty — brak spraw z delegowanym telefonem.")
        else:
            st.caption(f"📞 {len(_woreczek)} spraw do obdzwonienia:")
            for _wd in _woreczek:
                _w = _wd.to_dict() or {}
                _wnum = _w.get("numer_zamowienia", "?")
                _wgrp = _w.get("grupa", "?")
                _wpz = (_w.get("telefon_pz") or "").upper().strip()
                st.markdown(f"**{_wnum}** · {_wgrp}" + (f" · {_wpz}" if _wpz else ""))
                for _line in _phone_checklist_for(_wpz):
                    st.markdown(f"&nbsp;&nbsp;• {_line}")
                if st.button("✅ Usuń z woreczka", key=f"_wor_done_{_wd.id}"):
                    try:
                        db.collection(col("ew_cases")).document(_wd.id).update({"telefon_do_wykonania": False})
                        st.success(f"Usunięto {_wnum} z woreczka.")
                        st.rerun()
                    except Exception as _e:
                        st.error(f"Błąd: {_e}")
                st.markdown("---")

    # ── 📞 Rejestracja telefonu (moduł Telefony — Etap 1: liczenie) ──
    with st.expander("📞 Zarejestruj telefon", expanded=False):
        st.caption("Telefon to OSOBNY ruch — nie wlicza się do „przerobionych”. Zapisz wynik PO rozmowie.")
        _pc = st.session_state.get("ew_current_case") or {}
        _num_default = st.session_state.get("chat_nrzam") or _pc.get("numer_zamowienia") or ""
        _grp_default = _pc.get("grupa") or "DE"
        _ph_num = st.text_input("Numer zamówienia:", value=str(_num_default), key="_ph_num")
        _grp_opts = ["DE", "FR", "UKPL"]
        _ph_grp = st.selectbox("Grupa:", _grp_opts,
                               index=_grp_opts.index(_grp_default) if _grp_default in _grp_opts else 0,
                               key="_ph_grp")
        _ph_zrodlo_label = st.radio("Kto dzwonił:", [
            "Ja (operator dzwoniący)",
            "Telefonista (spoza systemu — wciągam wynik z forum)",
        ], key="_ph_zrodlo")
        _ph_wynik_label = st.radio("Wynik rozmowy:", [
            "📵 Nieodebrany / brak kontaktu",
            "🗣️ Odebrany — nic nie ustalono",
            "🔁 Przełożenie (kotwica — oddzwonić)",
            "✅ Konkret — sprawa ruszyła (data odbioru / zdjęcia / nadania / decyzja odbiór-punkt)",
        ], key="_ph_wynik")
        _ph_kurier = st.checkbox("💎📞 Telefon doprowadził do ZLECENIA KURIERA (diamentofon)", key="_ph_kurier")
        if st.button("💾 Zapisz telefon", type="primary", key="_ph_save"):
            _wmap = {
                "📵 Nieodebrany / brak kontaktu": "brak_kontaktu",
                "🗣️ Odebrany — nic nie ustalono": "kontakt_bez_konkretu",
                "🔁 Przełożenie (kotwica — oddzwonić)": "przelozenie",
                "✅ Konkret — sprawa ruszyła (data odbioru / zdjęcia / nadania / decyzja odbiór-punkt)": "konkret",
            }
            _w = _wmap.get(_ph_wynik_label, "kontakt_bez_konkretu")
            _zr = "operator_dzwoniacy" if _ph_zrodlo_label.startswith("Ja") else "telefonista_zewn"
            if not _ph_num.strip():
                st.error("Podaj numer zamówienia.")
            else:
                try:
                    ew_log_phone(op_name, _ph_num.strip(), _w, grupa=_ph_grp,
                                 kurier_ustalony=_ph_kurier, zrodlo=_zr)
                    _extra = " + 💎📞 diamentofon" if (_ph_kurier and _w == "konkret") else ""
                    st.success(f"✅ Zapisano telefon {_ph_num.strip()} ({_ph_grp}) — wynik: {_w}{_extra}")
                except Exception as _e:
                    st.error(f"❌ Błąd zapisu: {_e}")

    if st.button("🚪 Wyloguj"):
        if st.session_state.get("ew_current_case"):
            case = st.session_state.ew_current_case
            try:
                status = db.collection(col("ew_cases")).document(case["_doc_id"]).get().to_dict().get("status")
                if status in ("przydzielony", "w_toku"):
                    ew_release_case(case["_doc_id"])
            except:
                pass
        st.session_state.clear()
        cookies.clear()
        cookies.save()
        st.rerun()


# ==========================================
# GŁÓWNY INTERFEJS (prawie identyczny jak prod)
# ==========================================
st.title(f"🧪 Szturchacz EW TEST (forum)")

if st.session_state.get("czyste_okno", False) and not st.session_state.get("chat_started", False):
    st.subheader("🧪 Czyste okno — wklej wsad ręcznie")
    czyste_wsad = st.text_area("Wklej wsad:", height=200, key="czyste_wsad_input", placeholder="Wklej tu pełną linię szturchacza...")
    if st.button("🚀 Analizuj", type="primary"):
        if czyste_wsad.strip():
            nrzam = None
            import re as _re
            _match = _re.search(r'(\d{5,7})', czyste_wsad.strip())
            if _match:
                nrzam = _match.group(1)
            st.session_state.chat_nrzam = nrzam
            
            st.session_state.forum_debug_log = [] 
            
            if FORUM_ENABLED and nrzam:
                forum_ctx = auto_load_forum_context(db, col, str(nrzam))
                if forum_ctx:
                    czyste_wsad = czyste_wsad.strip() + "\n\n" + forum_ctx
                    st.toast(f"📖 Forum: kontekst załadowany dla {nrzam}")

            st.session_state.current_start_pz = parse_pz(czyste_wsad) or "PZ_START"
            st.session_state.messages = [{"role": "user", "content": czyste_wsad.strip()}]
            st.session_state.chat_started = True
            st.session_state.ew_wsad_ready = ""
            st.session_state._autopilot_loaded = False
            for _k in ("ew_reverse_active", "ew_reverse_no_pool", "ew_reverse_doc_id", "ew_reverse_grupa"):
                st.session_state.pop(_k, None)
            st.rerun()
        else:
            st.error("Wsad jest pusty!")

if "chat_started" not in st.session_state: st.session_state.chat_started = False

@st.cache_data(ttl=3600)
def get_remote_prompt(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except Exception as e:
        st.error(f"Błąd pobierania promptu z GitHub: {e}")
        return ""


if not st.session_state.chat_started:
    case = st.session_state.ew_current_case
    wybrany_tryb_main = st.session_state.get("tryb_label", "Standard")
    tryb_kod_main = {"Standard": "od_szturchacza", "WA": "WA", "MAIL": "MAIL", "FORUM": "FORUM"}.get(wybrany_tryb_main, "od_szturchacza")
    show_case_main = case and (
        tryb_kod_main == "od_szturchacza"
        or case.get("_reverse_mode", False)
    )
    
    if show_case_main and case.get("_reverse_mode", False):
        reverse_type = case.get("_reverse_type", "FORUM")
        st.subheader(f"📨 {reverse_type} — {case.get('numer_zamowienia', '?')}")
        st.caption(f"{case.get('priority_icon', '')} [{case.get('score', 0)}] {case.get('priority_label', '')}")
        st.warning(f"💡 Tryb {reverse_type}: Wklej Tabelkę + Kopertę + Rolkę.")
        st.code(f"ROLKA_START_{reverse_type}")
        
        default_wsad = case.get("pelna_linia_szturchacza", "")
        wsad_input = st.text_area(
            "Wklej/edytuj dane tutaj:",
            value=default_wsad,
            height=350,
            key="ew_reverse_wsad_edit",
        )
        
        if st.button("🚀 Rozpocznij analizę", type="primary"):
            if wsad_input and wsad_input.strip():
                st.session_state.forum_debug_log = [] 
                
                nrzam = case.get("numer_zamowienia", "")
                if not nrzam:
                    import re as _re
                    _nrzam_match = _re.match(r'(\d{5,7})', wsad_input.strip())
                    if _nrzam_match:
                        nrzam = _nrzam_match.group(1)
                
                st.session_state.chat_nrzam = str(nrzam) if nrzam else None
                
                if FORUM_ENABLED and nrzam:
                    forum_ctx = auto_load_forum_context(db, col, str(nrzam))
                    if forum_ctx:
                        wsad_input = wsad_input + "\n\n" + forum_ctx
                        st.toast(f"📖 Forum: załadowano kontekst dla {nrzam}")

                if case.get("_doc_id") and not case.get("_reverse_no_pool"):
                    db.collection(col("ew_cases")).document(case["_doc_id"]).update({
                        "status": "w_toku",
                        "started_at": firestore.SERVER_TIMESTAMP,
                    })
                st.session_state.current_start_pz = parse_pz(wsad_input) or "PZ_START"
                st.session_state.messages = [{"role": "user", "content": wsad_input}]
                st.session_state.chat_started = True
                st.session_state.ew_wsad_ready = ""
                st.session_state.ew_forced_tryb = reverse_type
                st.session_state._autopilot_loaded = False
                # TRWAŁY marker sesji odwrotnej — przycisk "Zakończ wsad odwrotny" w głównym
                # panelu opiera się na nim, więc działa nawet gdy ew_current_case == None
                # (case spoza puli / ręczny) i niezależnie od stanu sidebara.
                st.session_state.ew_reverse_active = reverse_type
                st.session_state.ew_reverse_no_pool = bool(case.get("_reverse_no_pool"))
                st.session_state.ew_reverse_doc_id = case.get("_doc_id")
                st.session_state.ew_reverse_grupa = case.get("grupa")
                st.rerun()
            else:
                st.error("Wsad jest pusty!")
    
    elif show_case_main:
        st.info(f"🏢 Case z Wieżowca: **{case.get('numer_zamowienia', '?')}** — "
                f"{case.get('priority_icon', '')} [{case.get('score', 0)}] {case.get('priority_label', '')}\n\n"
                f"Kliknij **▶️ Rozpocznij ten case** w panelu bocznym.")
    elif case and tryb_kod_main in ("WA", "MAIL", "FORUM"):
        st.info(f"📨 Tryb **{tryb_kod_main}** — wklej wsad poniżej lub wyszukaj case w panelu bocznym.\n\n"
                f"_(Case {case.get('numer_zamowienia', '?')} czeka w trybie Standard)_")
    else:
        st.info("👈 Pobierz case z Wieżowca (panel boczny).")

else:
    SYSTEM_PROMPT = get_remote_prompt(PROMPT_URL)

    if not SYSTEM_PROMPT:
        st.error("Nie udało się załadować promptu. Sprawdź URL w konfiguracji admina.")
        st.stop()

    tz_pl = pytz.timezone('Europe/Warsaw')
    now = datetime.now(tz_pl)

    p_notag = "TAK" if st.session_state.notag_val else "NIE"
    p_analizbior = "TAK" if st.session_state.analizbior_val else "NIE"

    aktualny_tryb = st.session_state.pop("ew_forced_tryb", None) or wybrany_tryb_kod

    parametry_startowe = f"""
# PARAMETRY STARTOWE
domyslny_operator={op_name}
domyslna_data={now.strftime('%d.%m')}
Grupa_Operatorska={cfg.get('role', 'Operatorzy_DE')}
domyslny_tryb={aktualny_tryb}
notag={p_notag}
analizbior={p_analizbior}
"""
    FULL_PROMPT = SYSTEM_PROMPT + parametry_startowe

    def get_vertex_history():
        vh = []
        for m in st.session_state.messages[:-1]:
            role = "user" if m["role"] == "user" else "model"
            vh.append(Content(role=role, parts=[Part.from_text(m["content"])]))
        return vh

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]): st.markdown(msg["content"])

    if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
        with st.chat_message("model"):
            with st.spinner("Analiza przez Vertex AI..."):
                models_to_try = [active_model_id]
                for fb in FALLBACK_CHAIN:
                    if fb != active_model_id and fb not in models_to_try:
                        models_to_try.append(fb)
                
                success = False
                used_model = None
                
                for model_id in models_to_try:
                    is_fallback = (model_id != active_model_id)
                    if is_fallback:
                        st.toast(f"🔄 Przełączam na {ALL_MODELS.get(model_id, model_id)}...")
                    
                    for attempt in range(5):
                        try:
                            cached_model = None
                            if caching_enabled:
                                cached_model = get_or_create_cached_model(model_id, FULL_PROMPT)
                            
                            if cached_model:
                                model = cached_model
                            else:
                                model = GenerativeModel(model_id, system_instruction=FULL_PROMPT)
                            
                            history = get_vertex_history()
                            chat = model.start_chat(history=history)
                            response = chat.send_message(
                                st.session_state.messages[-1]["content"],
                                generation_config={"temperature": 0.0}
                            )

                            ai_text = response.text
                            
                            # --- E2: FORUM INTEGRATION ---
                            if FORUM_ENABLED and ("[FORUM_WRITE|" in ai_text or "[FORUM_READ|" in ai_text):
                                _nrzam_e2 = st.session_state.get("chat_nrzam")
                                
                                if not _nrzam_e2 and st.session_state.messages:
                                    import re as _re
                                    _nrzam_match = _re.search(r'(\d{5,7})', st.session_state.messages[0]["content"])
                                    if _nrzam_match:
                                        _nrzam_e2 = _nrzam_match.group(1)
                                        st.session_state.chat_nrzam = _nrzam_e2

                                _fm_e2 = load_forum_memory(db, col, _nrzam_e2) if _nrzam_e2 else {}
                                
                                # === DIAMOND DETECTION (cel=AUTOS_KURIERZY + PZ=PZ6 + bump=0) ===
                                # Parsujemy bump i PZ z tagu C# w odpowiedzi AI — bump i PZ NIE są w markerze FORUM_WRITE.
                                # Moduł forum dostaje gotową decyzję is_diamond_candidate=True/False.
                                _pz_match = re.search(r'PZ\s*=\s*(PZ\d+)', ai_text)
                                _bump_match = re.search(r'bump\s*=\s*(\d+)', ai_text)
                                _detected_pz = _pz_match.group(1) if _pz_match else None
                                _detected_bump = int(_bump_match.group(1)) if _bump_match else None
                                _is_diamond_op = (_detected_pz == "PZ6" and _detected_bump == 0)
                                
                                # Wyciągnij kurier i kategorię z USTALEŃ jeśli są (best-effort)
                                _kurier_match = re.search(r'KURIER_PRZEWOZNIK\s*=\s*([A-Z_]+)', ai_text)
                                _towar_match = re.search(r'TOWAR_TYP\s*=\s*([A-Z_]+)', ai_text)
                                
                                _diamond_meta_op = {
                                    "numer_zamowienia": _nrzam_e2,
                                    "operator": op_name,
                                    "kurier": _kurier_match.group(1) if _kurier_match else None,
                                    "kategoria_towaru": _towar_match.group(1) if _towar_match else None,
                                    "grupa": operator_grupa,
                                    "pz": _detected_pz,
                                    "bump": _detected_bump,
                                }
                                
                                forum_result = execute_forum_actions(
                                    ai_text,
                                    forum_memory=_fm_e2,
                                    user_od=cfg.get("role", "Operatorzy_DE"),
                                    ai_user=get_forum_nick(op_name),
                                    db=db,
                                    source_type="operator",
                                    diamond_prefix=_COL_PREFIX,
                                    is_diamond_candidate=_is_diamond_op,
                                    diamond_meta=_diamond_meta_op,
                                )
                                ai_text = forum_result["response"]
                                
                                if forum_result["forum_writes"]:
                                    _any_success_e2 = False
                                    for fw in forum_result["forum_writes"]:
                                        if fw.get("success"):
                                            _any_success_e2 = True
                                            st.toast(f"✅ Forum: post {fw.get('FORUM_ID', '?')} wysłany")
                                            if _nrzam_e2 and fw.get("FORUM_ID") and fw.get("cel"):
                                                save_forum_memory(db, col, _nrzam_e2, fw["cel"], fw.get("FORUM_ID"), fw.get("tresc_skrot", ""))
                                        else:
                                            st.toast(f"❌ Forum: {fw.get('error', '?')}")
                                    
                                    # --- v1.5.7c: last_action_source w ew_cases ---
                                    # Rozpoznajemy tryb wsadu z forced (wsad odwrotny) lub z bramki Standard.
                                    if _any_success_e2 and _nrzam_e2:
                                        try:
                                            _tryb_for_source = st.session_state.get("ew_forced_tryb") or aktualny_tryb
                                            _source_map_e2 = {
                                                "od_szturchacza": "standard",
                                                "Standard": "standard",
                                                "WA": "wa",
                                                "MAIL": "mail",
                                                "FORUM": "forum",
                                            }
                                            _last_action_source_e2 = _source_map_e2.get(_tryb_for_source, "standard")
                                            _case_for_update_e2 = st.session_state.get("ew_current_case") or {}
                                            _case_doc_id_e2 = _case_for_update_e2.get("_doc_id")
                                            if _case_doc_id_e2:
                                                db.collection(col("ew_cases")).document(_case_doc_id_e2).update({
                                                    "last_action_source": _last_action_source_e2,
                                                    "last_action_at": firestore.SERVER_TIMESTAMP,
                                                })
                                        except Exception:
                                            pass  # nie wywróć sesji
                                
                                if forum_result["forum_reads"]:
                                    forum_context = "\n\n".join(forum_result["forum_reads"])
                                    st.session_state.messages.append({"role": "model", "content": ai_text})
                                    st.session_state.messages.append({"role": "user", "content": forum_context})
                                    st.toast("📖 Forum: pobrano kontekst, AI analizuje...")
                                    st.rerun()
                            # --- KONIEC E2 ---
                            
                            st.markdown(ai_text)
                            st.session_state.messages.append({"role": "model", "content": ai_text})
                            used_model = model_id

                            if is_fallback:
                                st.info(f"⚡ Odpowiedź z **{ALL_MODELS.get(model_id, model_id)}** — główny model przeciążony")

                            if (';pz=' in ai_text.lower() or 'cop#' in ai_text.lower()) and 'c#' in ai_text.lower():
                                log_stats(op_name, st.session_state.current_start_pz, parse_pz(ai_text) or "PZ_END", project_index)

                            # Delegacja telefonu → flaga do woreczka telefonicznego (moduł Telefony)
                            try:
                                if "forum_tel=czekam_wynik" in ai_text.lower():
                                    _cc = st.session_state.get("ew_current_case") or {}
                                    _cc_doc = _cc.get("_doc_id")
                                    if _cc_doc and not _cc.get("_reverse_no_pool"):
                                        db.collection(col("ew_cases")).document(_cc_doc).update({
                                            "telefon_do_wykonania": True,
                                            "telefon_pz": parse_pz(ai_text) or _cc.get("pz") or "",
                                            "telefon_jezyk": _cc.get("jezyk") or _cc.get("kraj") or "",
                                            "telefon_flagged_at": firestore.SERVER_TIMESTAMP,
                                        })
                            except Exception:
                                pass  # nie wywracaj sesji

                            success = True
                            break
                        except Exception as e:
                            err_str = str(e)
                            if "429" in err_str or "Quota" in err_str or "ResourceExhausted" in err_str or "503" in err_str or "unavailable" in err_str.lower():
                                wait_time = min(5 * (attempt + 1), 10)  
                                model_label = ALL_MODELS.get(model_id, model_id)
                                st.toast(f"⏳ {model_label}: próba {attempt+1}/5, czekam {wait_time}s...")
                                time.sleep(wait_time)
                            else:
                                st.error(f"Błąd Vertex AI ({model_id}): {err_str[:300]}")
                                break
                    
                    if success:
                        break
                
                if not success:
                    st.error("❌ Wszystkie modele niedostępne (2.5 Pro + 3 Pro + 3.1 Pro). Spróbuj za chwilę.")

    # === ZAKOŃCZENIE WSADU ODWROTNEGO — wypełnienie slotu w SIDEBARZE (nad „Nowa sprawa") ===
    # Kod biegnie PO wygenerowaniu odpowiedzi AI, więc widzi TAG z ostatniej odpowiedzi.
    # Renderuje się w panelu bocznym (kontener _rev_finish_container utworzony w sidebarze).
    # Przycisk pojawia się TYLKO gdy w ostatniej odpowiedzi AI jest TAG (C#:…) — bez TAGu
    # case NIE jest zakończony, więc nie wolno go domykać.
    _rev_active = st.session_state.get("ew_reverse_active")
    _cur_rev = st.session_state.get("ew_current_case") or {}
    if not _rev_active and _cur_rev.get("_reverse_mode"):
        _rev_active = _cur_rev.get("_reverse_type", "WA")
    if not _rev_active:
        _tl = st.session_state.get("tryb_label", "Standard")
        if _tl in ("WA", "MAIL", "FORUM"):
            _rev_active = _tl
    if _rev_active and st.session_state.get("chat_started") and globals().get("_rev_finish_container") is not None:
        _rt = _rev_active
        # TAG z OSTATNIEJ odpowiedzi AI (nie szukamy głębiej — liczy się aktualna decyzja).
        _last_tag, _last_pz = None, None
        for m in reversed(st.session_state.get("messages", [])):
            if m.get("role") == "model":
                _last_tag, _last_pz = detect_tag_in_response(m.get("content", ""))
                break
        with _rev_finish_container:
            if _last_tag:
                if st.button(f"✅ Zakończ wsad odwrotny ({_rt}) → następny", type="primary", key="rev_finish_side"):
                    _ch = {"WA": "wa", "MAIL": "mail", "FORUM": "forum"}.get(_rt, "standard")
                    _np = st.session_state.get("ew_reverse_no_pool", _cur_rev.get("_reverse_no_pool", False))
                    _doc = st.session_state.get("ew_reverse_doc_id", _cur_rev.get("_doc_id"))
                    _grp = None if _np else st.session_state.get("ew_reverse_grupa", _cur_rev.get("grupa"))
                    if _doc and not _np:
                        ew_complete_case(_doc, result_tag=_last_tag, result_pz=_last_pz)
                    log_stats(op_name, st.session_state.get("current_start_pz"), _last_pz, st.session_state.get("current_project_idx", 0))
                    ew_log_completion(op_name, channel=_ch, grupa=_grp,
                                      data_obrobki=_get_case_data_obrobki(_doc))
                    for _k in ("messages", "ew_current_case", "ew_reverse_active", "ew_reverse_no_pool",
                               "ew_reverse_doc_id", "ew_reverse_grupa", "current_start_pz", "chat_nrzam"):
                        st.session_state.pop(_k, None)
                    st.session_state.chat_started = False
                    st.session_state._autopilot_loaded = False
                    st.session_state.ew_wsad_ready = ""
                    st.success(f"✅ Wsad odwrotny ({_rt}) zakończony i policzony.")
                    st.rerun()
            else:
                st.caption(f"📨 Tryb odwrotny {_rt}: przycisk „Zakończ” pojawi się, gdy AI wyda decyzję z TAG-KOPERTĄ.")

    if prompt := st.chat_input("Odpowiedz AI..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.rerun()


# ==========================================
# POLE WSADU (gdy chat nie jest uruchomiony)
# ==========================================
if not st.session_state.chat_started:
    if not show_case_main:
        st.subheader(f"📥 Pierwszy wsad ({op_name})")
        if wybrany_tryb_kod != "od_szturchacza":
            st.warning(f"💡 Tryb {st.session_state.tryb_label}: Wklej Tabelkę + Kopertę + Rolkę.")
            st.code(f"ROLKA_START_{wybrany_tryb_kod}")

        wsad_input = st.text_area(
            "Wklej dane tutaj:",
            value="",
            height=350,
        )

        if st.button("🚀 Rozpocznij analizę", type="primary"):
            if wsad_input:
                st.session_state.forum_debug_log = []
                
                _nrzam_clean = None
                import re as _re
                _match = _re.search(r'(\d{5,7})', wsad_input.strip())
                if _match:
                    _nrzam_clean = _match.group(1)
                st.session_state.chat_nrzam = _nrzam_clean
                
                if FORUM_ENABLED and _nrzam_clean:
                    forum_ctx = auto_load_forum_context(db, col, _nrzam_clean)
                    if forum_ctx:
                        wsad_input = wsad_input + "\n\n" + forum_ctx
                        st.toast(f"📖 Forum: kontekst załadowany dla {_nrzam_clean}")
                
                st.session_state.current_start_pz = parse_pz(wsad_input) or "PZ_START"
                st.session_state.messages = [{"role": "user", "content": wsad_input}]
                st.session_state.chat_started = True
                st.session_state.ew_wsad_ready = ""
                st.rerun()
            else:
                st.error("Wsad jest pusty!")

# --- FORUM DEBUG PANEL ---
if st.session_state.get("forum_debug_log"):
    with st.expander("🔍 Forum Debug Log", expanded=False):
        for line in st.session_state.forum_debug_log:
            st.code(line, language=None)
        if st.button("🗑️ Wyczyść debug log"):
            st.session_state.forum_debug_log = []
            st.rerun()
