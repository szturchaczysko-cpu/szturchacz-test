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

# --- BRAMKA LOGOWANIA (TEST) ---
if "operator" not in st.session_state:
    st.set_page_config(page_title="🧪 Szturchacz EW TEST", layout="wide", page_icon="🧪")
    st.title("🧪 Szturchacz EW TEST")
    op = st.selectbox("Operator:", ["Sylwia", "EwelinaG", "Andrzej", "Klaudia"])
    if st.button("Zaloguj"):
        st.session_state.operator = op
        st.rerun()
    st.stop()

# ==========================================
# 🔑 CONFIG I TOŻSAMOŚĆ (identyczne jak prod)
# ==========================================
op_name = st.session_state.operator
cfg_ref = db.collection("operator_configs").document(op_name)  # zawsze z produkcji
cfg = cfg_ref.get().to_dict() or {}

# Fallback jeśli operator nie ma configa w produkcji
if not cfg or not cfg.get("role"):
    cfg = {
        "role": "Operatorzy_DE",
        "prompt_url": "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz-test/refs/heads/main/v4_forum_weryfik.txt",
        "prompt_name": "v4 forum",
        "assigned_key_index": 1,
        "tel": False,
    }

# Zawsze użyj testowego promptu forum
if TEST_MODE:
    cfg["prompt_url"] = "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz-test/refs/heads/main/v4_forum_weryfik.txt"
    cfg["prompt_name"] = "v4 forum"

# --- PROJEKT GCP ---
fixed_key_idx = int(cfg.get("assigned_key_index", 1))
if fixed_key_idx < 1 or fixed_key_idx > len(GCP_PROJECTS):
    fixed_key_idx = 1
    st.warning(f"⚠️ Nieprawidłowy indeks projektu. Domyślnie: 1.")

project_index = fixed_key_idx - 1
current_gcp_project = GCP_PROJECTS[project_index]
st.session_state.vertex_project_index = project_index

# --- URL PROMPTU ---
PROMPT_URL = cfg.get("prompt_url", "")
PROMPT_NAME = cfg.get("prompt_name", "Nieprzypisany")

if not PROMPT_URL:
    st.error("🚨 Brak przypisanego promptu! Poproś admina.")
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
    "Marta": True, "Klaudia": True, "Kasia": True,
    "Iwona": False, "Marlena": False, "Sylwia": False,
    "EwelinaG": False, "Andrzej": False, "Romana": False,
}

OPERATORS_TEL_JEZYKI = {
    "Emilia": ["DE"], "Magda": ["DE"], "Ewelina": ["DE"], "Klaudia": ["DE"],
    "Oliwia": ["EN"],
    "Marta": ["PL"],
    "Kasia": ["FR"],
}

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
        # Skip forum_action cases — czekają na uwolnienie
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
            elif my_tel and not other_tel:
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

def ew_complete_case(case_doc_id, result_tag=None, result_pz=None, prompt_name=None, prompt_url=None):
    upd = {"status": "zakonczony", "completed_at": firestore.SERVER_TIMESTAMP}
    if result_tag: upd["result_tag"] = result_tag
    if result_pz: upd["result_pz"] = result_pz
    if prompt_name: upd["processed_prompt"] = prompt_name
    if prompt_url: upd["processed_prompt_url"] = prompt_url
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

def ew_log_completion(op_name):
    tz_pl = pytz.timezone('Europe/Warsaw')
    today = datetime.now(tz_pl).strftime("%Y-%m-%d")
    time_str = datetime.now(tz_pl).strftime("%H:%M")
    db.collection(col("ew_operator_stats")).document(today).collection("operators").document(op_name).set({
        "cases_completed": firestore.Increment(1),
        "completion_times": firestore.ArrayUnion([time_str]),
    }, merge=True)

def ew_log_taken(op_name):
    tz_pl = pytz.timezone('Europe/Warsaw')
    today = datetime.now(tz_pl).strftime("%Y-%m-%d")
    db.collection(col("ew_operator_stats")).document(today).collection("operators").document(op_name).set({
        "cases_taken": firestore.Increment(1),
    }, merge=True)

def ew_log_skipped(op_name):
    tz_pl = pytz.timezone('Europe/Warsaw')
    today = datetime.now(tz_pl).strftime("%Y-%m-%d")
    db.collection(col("ew_operator_stats")).document(today).collection("operators").document(op_name).set({
        "cases_skipped": firestore.Increment(1),
    }, merge=True)

def detect_tag_in_response(text):
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
            reverse_label = f" 📨 {case.get('_reverse_type', '')}" if is_reverse else ""
            autopilot_label = " 🤖" if case.get("autopilot_status") == "calculated" else ""
            st.info(f"📌 Case: **{case.get('numer_zamowienia', '?')}**{reverse_label}{autopilot_label}\n"
                    f"{case.get('priority_icon', '')} [{case.get('score', 0)}]")
            if case.get("autopilot_status") == "calculated":
                if is_reverse:
                    st.caption(f"🤖 Przeliczone nocą, ale tryb **{case.get('_reverse_type', '')}** → start od zera (nowa instancja kanałowa)")
                elif cfg.get("autopilot_enabled", False):
                    st.caption("🤖 Pierwszy ruch przeliczony — kliknij ▶️ by załadować gotową analizę")
                else:
                    st.caption("🤖 Przeliczone nocą (autopilot OFF — będzie liczone od zera)")

            if not st.session_state.get("chat_started"):
                if is_reverse:
                    st.caption("📨 Edytuj wsad i kliknij **🚀 Rozpocznij analizę** w głównym panelu →")
                elif st.button("▶️ Rozpocznij ten case", type="primary"):
                    wsad = case.get("pelna_linia_szturchacza", "")
                    if wsad:
                        nrzam = str(case.get("numer_zamowienia", ""))
                        st.session_state.chat_nrzam = nrzam
                        
                        # --- NIGHT_TAG: podmień stary tag na nocny ---
                        _night_tag = case.get("night_tag")
                        if _night_tag:
                            _old_tag_match = re.search(r'[Cc]#:[\d.]+;[^\t\n]*?;?[Nn][Ee][Xx][Tt]=\d{2}\.\d{2}', wsad)
                            if _old_tag_match:
                                wsad = wsad[:_old_tag_match.start()] + _night_tag + wsad[_old_tag_match.end():]
                                st.toast(f"🌙 Tag zaktualizowany z nocnego przeliczenia")
                            else:
                                _old_tag_match2 = re.search(r'[Cc]#:[^\t\n]+', wsad)
                                if _old_tag_match2:
                                    wsad = wsad[:_old_tag_match2.start()] + _night_tag + wsad[_old_tag_match2.end():]
                                    st.toast(f"🌙 Tag zaktualizowany z nocnego przeliczenia")
                                else:
                                    wsad = wsad + "\n" + _night_tag
                                    st.toast(f"🌙 Dodano nocny tag")
                        # --- KONIEC NIGHT_TAG ---
                        
                        if FORUM_ENABLED and nrzam:
                            forum_ctx = auto_load_forum_context(db, col, nrzam, wsad_text=wsad)
                            if forum_ctx:
                                wsad = wsad + "\n\n" + forum_ctx
                                st.toast(f"📖 Forum: załadowano kontekst dla {nrzam}")
                        
                        if case.get("_doc_id"):
                            db.collection(col("ew_cases")).document(case["_doc_id"]).update({
                                "status": "w_toku",
                                "started_at": firestore.SERVER_TIMESTAMP,
                            })

                        autopilot_msgs = case.get("autopilot_messages")
                        operator_autopilot_on = cfg.get("autopilot_enabled", False)
                        if (operator_autopilot_on
                                and not is_reverse  
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
                        st.rerun()
                    else:
                        st.error("Case nie ma wsadu!")

            st.markdown("---")
            skip_reason = st.text_area("💬 Powód pominięcia:", key="ew_skip_reason", max_chars=500, height=80, placeholder="np. brak danych, czekam na forum, klient nie odbiera...")
            if st.button("⏭️ Pomiń case"):
                if not skip_reason or not skip_reason.strip():
                    st.error("⚠️ Wpisz powód pominięcia — nie można pominąć bez komentarza!")
                else:
                    if case.get("_doc_id"):
                        db.collection(col("ew_cases")).document(case["_doc_id"]).update({
                            "status": "pominiety",
                            "assigned_to": None,
                            "assigned_at": None,
                            "skip_reason": skip_reason.strip(),
                            "skipped_by": op_name,
                            "skipped_at": firestore.SERVER_TIMESTAMP,
                        })
                        ew_log_skipped(op_name)
                    st.session_state.ew_current_case = None
                    st.session_state.ew_wsad_ready = ""
                    st.session_state.messages = []
                    st.session_state.chat_started = False
                    st.session_state._autopilot_loaded = False
                    st.session_state.current_start_pz = None
                    st.session_state.chat_nrzam = None
                    st.rerun()

            st.markdown("---")
            if st.button("✅ Zakończ → Następny"):
                tag, pz = None, None
                msgs = st.session_state.get("messages", [])
                for m in reversed(msgs):
                    if m.get("role") == "model":
                        tag, pz = detect_tag_in_response(m.get("content", ""))
                        if tag:
                            break
                
                if tag:
                    if case.get("_doc_id"):
                        ew_complete_case(case["_doc_id"], result_tag=tag, result_pz=pz, prompt_name=cfg.get("prompt_name"), prompt_url=cfg.get("prompt_url"))
                    start_pz = st.session_state.get("current_start_pz", None)
                    end_pz = pz  
                    proj_idx = st.session_state.get("current_project_idx", 0)
                    log_stats(op_name, start_pz, end_pz, proj_idx)
                    ew_log_completion(op_name)
                    st.session_state.messages = []
                    st.session_state.chat_started = False
                    st.session_state.current_start_pz = None
                    st.session_state._autopilot_loaded = False
                    st.session_state.chat_nrzam = None
                    new_case = ew_get_next_case(operator_grupa, op_name)
                    st.session_state.ew_current_case = new_case
                    st.session_state.ew_wsad_ready = ""
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
                            st.warning(f"⚠️ Case **{nrzam_input}** jest przydzielony do: **{case_data.get('assigned_to', '?')}**. Wklej wsad w prawym panelu.")
                        elif status == "completed":
                            st.info(f"ℹ️ Case **{nrzam_input}** jest zakończony. Wklej wsad w prawym panelu.")
                        else:
                            st.warning(f"🔍 Nie znaleziono **{nrzam_input}** w bazie casów. Wklej wsad w prawym panelu.")
                    else:
                        st.error("Podaj numer zamówienia!")
            
            else:
                if avail > 0:
                    if st.button("📥 Pobierz następny case", type="primary"):
                        case = ew_get_next_case(operator_grupa, op_name)
                        if case:
                            ew_log_taken(op_name)
                            st.session_state.ew_current_case = case
                            st.rerun()
                        else:
                            st.warning("Brak wolnych casów.")
                else:
                    st.caption("🔍 Brak wolnych casów w Twojej grupie.")

        st.markdown("---")

    admin_msg = cfg.get("admin_message", "")
    if admin_msg and not cfg.get("message_read", False):
        st.error(f"📢 **WIADOMOŚĆ:**\n\n{admin_msg}")
        if st.button("✅ Odczytałem"):
            db.collection(col("admin_config")).document("message_read_" + op_name).set({"read": True})
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

    if st.button("🚀 Nowa sprawa / Reset", type="primary"):
        if st.session_state.ew_current_case:
            case = st.session_state.ew_current_case
            status = db.collection(col("ew_cases")).document(case["_doc_id"]).get().to_dict().get("status")
            if status == "przydzielony":
                ew_release_case(case["_doc_id"])
                st.session_state.ew_current_case = None
        st.session_state.messages = []
        st.session_state.chat_started = False
        st.session_state.current_start_pz = None
        st.session_state.ew_wsad_ready = ""
        st.session_state.chat_nrzam = None
        st.rerun()

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
                forum_ctx = auto_load_forum_context(db, col, str(nrzam), wsad_text=czyste_wsad)
                if forum_ctx:
                    czyste_wsad = czyste_wsad.strip() + "\n\n" + forum_ctx
                    st.toast(f"📖 Forum: kontekst załadowany dla {nrzam}")

            st.session_state.current_start_pz = parse_pz(czyste_wsad) or "PZ_START"
            st.session_state.messages = [{"role": "user", "content": czyste_wsad.strip()}]
            st.session_state.chat_started = True
            st.session_state.ew_wsad_ready = ""
            st.session_state._autopilot_loaded = False
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
                    forum_ctx = auto_load_forum_context(db, col, str(nrzam), wsad_text=wsad_input)
                    if forum_ctx:
                        wsad_input = wsad_input + "\n\n" + forum_ctx
                        st.toast(f"📖 Forum: załadowano kontekst dla {nrzam}")

                if case.get("_doc_id"):
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

    _op_tel = OPERATORS_TEL.get(op_name, False)
    _op_tel_jezyki = OPERATORS_TEL_JEZYKI.get(op_name, [])

    parametry_startowe = f"""
# PARAMETRY STARTOWE
domyslny_operator={op_name}
domyslna_data={now.strftime('%d.%m')}
Grupa_Operatorska={cfg.get('role', 'Operatorzy_DE')}
domyslny_tryb={aktualny_tryb}
notag={p_notag}
analizbior={p_analizbior}
TEL={'TAK' if _op_tel else 'NIE'}
TEL_JEZYKI={','.join(_op_tel_jezyki) if _op_tel_jezyki else 'BRAK'}
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
                                forum_result = execute_forum_actions(ai_text, forum_memory=_fm_e2, user_od=cfg.get('role', 'Operatorzy_DE'), ai_user=op_name)
                                ai_text = forum_result["response"]
                                
                                if forum_result["forum_writes"]:
                                    for fw in forum_result["forum_writes"]:
                                        if fw.get("success"):
                                            st.toast(f"✅ Forum: post {fw.get('FORUM_ID', '?')} wysłany")
                                            if _nrzam_e2 and fw.get("FORUM_ID") and fw.get("cel"):
                                                save_forum_memory(db, col, _nrzam_e2, fw["cel"], fw.get("FORUM_ID"), fw.get("tresc_skrot", ""))
                                        else:
                                            st.toast(f"❌ Forum: {fw.get('error', '?')}")
                                
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
                    forum_ctx = auto_load_forum_context(db, col, _nrzam_clean, wsad_text=wsad_input)
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
        st.caption(f"📄 Prompt: **{PROMPT_NAME}** | URL: {PROMPT_URL.split('/')[-1] if PROMPT_URL else '?'}")
        for line in st.session_state.forum_debug_log:
            st.code(line, language=None)
        if st.button("🗑️ Wyczyść debug log"):
            st.session_state.forum_debug_log = []
            st.rerun()
