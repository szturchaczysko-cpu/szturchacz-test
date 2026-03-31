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
# True = kolekcje z prefixem "test_" i wymuszenie nowych URL promptów
TEST_MODE = True
_COL_PREFIX = "test_" if TEST_MODE else ""
def col(name):
    """Prefixuje nazwę kolekcji w trybie testowym."""
    return f"{_COL_PREFIX}{name}"

# --- 0. KONFIGURACJA ŚRODOWISKA ---
try: locale.setlocale(locale.LC_TIME, "pl_PL.UTF-8")
except: pass

# --- 1. POŁĄCZENIA ---
if not firebase_admin._apps:
    creds_dict = json.loads(st.secrets["FIREBASE_CREDS"])
    cred = credentials.Certificate(creds_dict)
    firebase_admin.initialize_app(cred)
db = firestore.client()

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
    op = st.selectbox("Operator:", ["Sylwia"])
    if st.button("Zaloguj"):
        st.session_state.operator = op
        st.rerun()
    st.stop()

# ==========================================
# 🔑 CONFIG I TOŻSAMOŚĆ
# ==========================================
op_name = st.session_state.operator
cfg_ref = db.collection(col("operator_configs")).document(op_name)
cfg = cfg_ref.get().to_dict() or {}

# --- AUTO-SEED (NAPRAWA BŁĘDU 404 W WIEŻOWCU) ---
# Ten blok wymusza poprawne linki w bazie danych, które czyta Wieżowiec
if TEST_MODE:
    NEW_TEST_URL = "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz-test/refs/heads/main/v4_forum.txt"
    
    # 1. Naprawa konfiguracji Twojego operatora
    cfg = {
        "role": "Operatorzy_FR",
        "prompt_url": NEW_TEST_URL,
        "prompt_name": "v4 forum (EA Edition)",
        "assigned_key_index": 1,
        "tel": False,
    }
    cfg_ref.set(cfg, merge=True)
    
    # 2. Naprawa konfiguracji GLOBALNEJ (Wieżowiec często z niej korzysta)
    global_ref = db.collection(col("admin_config")).document("global_settings")
    global_ref.set({
        "prompt_url": NEW_TEST_URL,
        "prompt_name": "v4 forum (EA Edition)"
    }, merge=True)

# --- PROJEKT GCP ---
fixed_key_idx = int(cfg.get("assigned_key_index", 1))
project_index = fixed_key_idx - 1
current_gcp_project = GCP_PROJECTS[project_index]

# --- URL PROMPTU (Twarda poprawka w kodzie) ---
if TEST_MODE:
    PROMPT_URL = "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz-test/refs/heads/main/v4_forum.txt"
    PROMPT_NAME = "v4 forum (EA Edition)"
else:
    PROMPT_URL = cfg.get("prompt_url", "")
    PROMPT_NAME = cfg.get("prompt_name", "Nieprzypisany")

if not PROMPT_URL:
    st.error("🚨 Brak przypisanego promptu! Poproś admina.")
    st.stop()

# Inicjalizacja Vertex AI
if 'vertex_init_done' not in st.session_state or st.session_state.get('last_project') != current_gcp_project:
    creds_info = json.loads(st.secrets["FIREBASE_CREDS"])
    creds = service_account.Credentials.from_service_account_info(creds_info)
    vertexai.init(project=current_gcp_project, location=st.secrets["GCP_LOCATION"], credentials=creds)
    st.session_state.vertex_init_done = True
    st.session_state.last_project = current_gcp_project

# ==========================================
# 🏢 LOGIKA STARTU ANALIZY (Ujednolicona)
# ==========================================
def start_analysis_logic(wsad_text, nrzam_manual=None):
    """Główny silnik startowy - zapewnia odczyt forum i blokadę numeru zamówienia."""
    st.session_state.forum_debug_log = []
    
    # Próba wyciągnięcia NRZAM
    nrzam = nrzam_manual
    if not nrzam:
        match = re.search(r'(\d{5,7})', wsad_text.strip())
        if match: nrzam = match.group(1)
    
    # Blokada numeru zamówienia dla całej sesji czatu
    st.session_state.chat_nrzam = str(nrzam) if nrzam else None
    
    # Odczyt forum (zawsze, niezależnie od trybu)
    if FORUM_ENABLED and nrzam:
        ctx = auto_load_forum_context(db, col, str(nrzam))
        if ctx:
            wsad_text = wsad_text.strip() + "\n\n" + ctx
            st.toast(f"📖 Forum: Załadowano historię dla {nrzam}")
            
    # Ustawienie PZ startowego
    pz_match = re.search(r'(PZ\d+)', wsad_text, re.I)
    st.session_state.current_start_pz = pz_match.group(1).upper() if pz_match else "PZ0"
    
    st.session_state.messages = [{"role": "user", "content": wsad_text}]
    st.session_state.chat_started = True
    st.rerun()

# --- SIDEBAR UI ---
with st.sidebar:
    st.title(f"👤 {op_name}")
    if st.button("🧹 Reset (czysty start)"):
        st.session_state.chat_nrzam = None
        st.session_state.messages = []
        st.session_state.chat_started = False
        st.rerun()

    st.markdown(f"**📄 Prompt:** `{PROMPT_NAME}`")
    
    current_case = st.session_state.get("ew_current_case")
    if current_case and not st.session_state.get("chat_started"):
        if st.button("▶️ Rozpocznij ten case", type="primary"):
            start_analysis_logic(current_case.get("pelna_linia_szturchacza", ""), current_case.get("numer_zamowienia"))

# ==========================================
# GŁÓWNY INTERFEJS
# ==========================================
st.title("🧪 Szturchacz EW TEST (forum)")

if not st.session_state.get("chat_started"):
    st.subheader("📥 Wklej dane (Wsad Odwrotny / Ręczny)")
    wsad_input = st.text_area("Wklej wiersz lub tabelkę z panelu:", height=300)
    if st.button("🚀 Rozpocznij analizę", type="primary"):
        if wsad_input.strip():
            start_analysis_logic(wsad_input)
        else:
            st.error("Wsad jest pusty!")
else:
    try:
        # Zawsze pobieramy z PROMPT_URL, który w trybie testowym jest teraz wymuszony
        prompt_resp = requests.get(PROMPT_URL)
        prompt_resp.raise_for_status()
        prompt_text = prompt_resp.text
    except Exception as e:
        st.error(f"🚨 Błąd pobierania promptu: {e}\nPróbowałem pobrać z: {PROMPT_URL}")
        st.stop()
        
    FULL_PROMPT = prompt_text + f"\n# PARAMETRY STARTOWE\ndomyslny_operator={op_name}\ndomyslna_data={datetime.now().strftime('%d.%m')}"
    
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]): 
            st.markdown(msg["content"])
        
    if st.session_state.messages[-1]["role"] == "user":
        with st.chat_message("model"):
            with st.spinner("Szturchacz analizuje..."):
                try:
                    model = GenerativeModel("gemini-2.5-pro", system_instruction=FULL_PROMPT)
                    chat = model.start_chat()
                    response = chat.send_message(st.session_state.messages[-1]["content"])
                    ai_text = response.text
                    
                    if FORUM_ENABLED and "[FORUM_" in ai_text:
                        target_nrzam = st.session_state.get("chat_nrzam")
                        fm = load_forum_memory(db, col, target_nrzam) if target_nrzam else {}
                        res = execute_forum_actions(ai_text, forum_memory=fm)
                        ai_text = res["response"]
                        
                        if res["forum_writes"] and target_nrzam:
                            for fw in res["forum_writes"]:
                                if fw.get("success"):
                                    save_forum_memory(db, col, target_nrzam, fw["cel"], fw["FORUM_ID"], fw.get("tresc_skrot", ""))
                        
                        if res["forum_reads"]:
                            st.session_state.messages.append({"role": "model", "content": ai_text})
                            st.session_state.messages.append({"role": "user", "content": "\n\n".join(res["forum_reads"])})
                            st.rerun()

                    st.markdown(ai_text)
                    st.session_state.messages.append({"role": "model", "content": ai_text})
                except Exception as e:
                    st.error(f"Błąd AI: {e}")

    if prompt := st.chat_input("Odpowiedz Szturchaczowi..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.rerun()

# --- FORUM DEBUG PANEL ---
if st.session_state.get("forum_debug_log"):
    with st.expander("🔍 Forum Debug Log", expanded=False):
        for line in st.session_state.forum_debug_log:
            st.code(line, language=None)
