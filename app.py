"""
Plateforme de Suivi — Marché à Terme Marocain (MASI 20 Futures)
Bourse de Casablanca — Instruments Dérivés

Flux :
- Au premier lancement, la base est initialisée avec les fichiers Excel dans /data (historique).
- Ensuite, saisie manuelle quotidienne via formulaires.
- Les données s'ajoutent à la base SQLite (persistante en local).
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
import tempfile
from datetime import datetime, date, timedelta
from pathlib import Path
import io

# ======================== CONFIGURATION ========================
st.set_page_config(
    page_title="MASI 20 Futures — Suivi de Marché",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# DB path — persist locally, use /tmp on Streamlit Cloud (ephemeral fs)
APP_DIR = Path(__file__).parent
_IS_CLOUD = os.environ.get("STREAMLIT_RUNTIME_ENV") == "cloud" or \
            ("HOME" in os.environ and os.environ.get("HOME", "").startswith("/home/appuser"))
if _IS_CLOUD:
    DB_PATH = Path(tempfile.gettempdir()) / "masi20.db"
else:
    DB_PATH = APP_DIR / "masi20.db"

SEED_DIR = APP_DIR / "data"
SEED_MASI = SEED_DIR / "MASI_20.xlsx"
SEED_BULLETIN = SEED_DIR / "Suivi_de_marche.xlsx"

CONTRACT_ORDER = ["FMASI20JUI26", "FMASI20SEP26", "FMASI20DEC26", "FMASI20MAR27"]
CONTRACT_LABELS = {
    "FMASI20JUI26": "Juin 2026",
    "FMASI20SEP26": "Septembre 2026",
    "FMASI20DEC26": "Décembre 2026",
    "FMASI20MAR27": "Mars 2027",
}
CONTRACT_ISIN = {
    "FMASI20JUI26": "MA0009000037", "FMASI20SEP26": "MA0009000045",
    "FMASI20DEC26": "MA0009000052", "FMASI20MAR27": "MA0009000060",
}
CONTRACT_INSTRUMENT = {
    "FMASI20JUI26": "MASI20 FUTURE JUI26", "FMASI20SEP26": "MASI20 FUTURE SEP26",
    "FMASI20DEC26": "MASI20 FUTURE DEC26", "FMASI20MAR27": "MASI20 FUTURE MAR27",
}
CONTRACT_COLORS = {
    "FMASI20JUI26": "#C8A24B", "FMASI20SEP26": "#1F6FEB",
    "FMASI20DEC26": "#D9534F", "FMASI20MAR27": "#2E8B57",
}

# ======================== STYLE ========================
CUSTOM_CSS = """
<style>
    .stApp { background: linear-gradient(180deg, #f7f5f0 0%, #ffffff 40%); }
    .bvc-band {
        background: linear-gradient(90deg, #0b2545 0%, #13315c 50%, #8d5524 100%);
        color: #f7f5f0; padding: 18px 26px; border-radius: 6px;
        margin-bottom: 18px; border-left: 6px solid #C8A24B;
        box-shadow: 0 2px 8px rgba(11,37,69,0.15);
    }
    .bvc-band h1 { margin: 0; font-family: 'Georgia', serif; font-size: 26px; letter-spacing: 0.5px; }
    .bvc-band .sub { font-size: 13px; opacity: 0.85; letter-spacing: 2px; text-transform: uppercase; margin-top: 4px; }
    .metric-card {
        background: #ffffff; border: 1px solid #e5e1d8; border-top: 3px solid #C8A24B;
        border-radius: 4px; padding: 14px 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); height: 100%;
    }
    .metric-card .label { font-size: 11px; text-transform: uppercase; letter-spacing: 1.5px; color: #6b6358; font-weight: 600; }
    .metric-card .value { font-family: 'Georgia', serif; font-size: 22px; color: #0b2545; margin-top: 4px; font-weight: 700; }
    .metric-card .delta-up { color: #2E8B57; font-size: 12px; font-weight: 600; }
    .metric-card .delta-down { color: #D9534F; font-size: 12px; font-weight: 600; }
    .metric-card .delta-flat { color: #6b6358; font-size: 12px; }
    .index-card { background: linear-gradient(135deg, #0b2545 0%, #13315c 100%); color: #f7f5f0; border-left: 4px solid #C8A24B; }
    .index-card .label { color: #C8A24B; }
    .index-card .value { color: #ffffff; }
    .section-h { font-family: 'Georgia', serif; color: #0b2545; border-bottom: 2px solid #C8A24B; padding-bottom: 6px; margin: 18px 0 12px 0; font-size: 18px; }
    [data-testid="stSidebar"] { background: #0b2545; }
    [data-testid="stSidebar"] * { color: #f7f5f0 !important; }
    [data-testid="stSidebar"] .stButton button { background: #C8A24B; color: #0b2545 !important; border: none; font-weight: 700; }
    .stTabs [data-baseweb="tab-list"] { gap: 4px; background: #efe9db; padding: 4px; border-radius: 4px; }
    .stTabs [data-baseweb="tab"] { background: transparent; color: #0b2545; font-weight: 600; padding: 8px 16px; }
    .stTabs [aria-selected="true"] { background: #0b2545 !important; color: #C8A24B !important; }
    footer { visibility: hidden; } #MainMenu { visibility: hidden; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ======================== DATABASE ========================
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    conn = get_conn(); c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS masi20_history (
        price_date DATE PRIMARY KEY, close_price REAL NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS futures_quotes (
        price_date DATE NOT NULL, ticker TEXT NOT NULL, instrument TEXT, code_isin TEXT,
        sous_jacent TEXT, cours_reference REAL, ouverture REAL, plus_bas REAL, plus_haut REAL,
        cloture REAL, cours_compensation REAL, positions_ouvertes REAL, week_number INTEGER,
        PRIMARY KEY (price_date, ticker))""")
    c.execute("""CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, price_date DATE NOT NULL, instrument TEXT,
        ticker TEXT, carnet TEXT, cours_transaction REAL, multiplicateur REAL, volume_mad REAL,
        quantite REAL, nb_transactions INTEGER, week_number INTEGER)""")
    c.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(price_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tx_week ON transactions(week_number)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_fq_week ON futures_quotes(week_number)")
    conn.commit(); conn.close()

def _f(v):
    try:
        if v is None or pd.isna(v): return None
        return float(v)
    except Exception: return None

def _i(v):
    try:
        if v is None or pd.isna(v): return None
        return int(v)
    except Exception: return None

def is_seeded() -> bool:
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT value FROM meta WHERE key='seeded'")
    r = c.fetchone(); conn.close()
    return r is not None and r[0] == "1"

def mark_seeded():
    conn = get_conn()
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('seeded', '1')")
    conn.commit(); conn.close()

def upsert_masi20(df: pd.DataFrame) -> int:
    conn = get_conn(); c = conn.cursor(); n = 0
    for _, row in df.iterrows():
        try:
            d = pd.to_datetime(row["price_date"]).date()
            p = float(row["close_price"])
            c.execute("INSERT OR REPLACE INTO masi20_history (price_date, close_price) VALUES (?, ?)", (d.isoformat(), p))
            n += 1
        except Exception: continue
    conn.commit(); conn.close()
    return n

def upsert_bulletin(df_market: pd.DataFrame, df_tx: pd.DataFrame):
    conn = get_conn(); c = conn.cursor(); n_q, n_t = 0, 0
    for _, r in df_market.iterrows():
        try:
            d = pd.to_datetime(r["Date"]).date()
            ticker = str(r["Ticker"]).strip()
            if not ticker or ticker == "nan": continue
            wk = d.isocalendar().week
            c.execute("""INSERT OR REPLACE INTO futures_quotes
                (price_date, ticker, instrument, code_isin, sous_jacent, cours_reference, ouverture,
                 plus_bas, plus_haut, cloture, cours_compensation, positions_ouvertes, week_number)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (d.isoformat(), ticker, r.get("Instrument"), r.get("Code ISIN"), r.get("Sous jacent"),
                 _f(r.get("Cours de réference")), _f(r.get("Ouverture")), _f(r.get("+ bas")), _f(r.get("+haut")),
                 _f(r.get("Clôture")), _f(r.get("Cours de Compensation")), _f(r.get("Positions Ouvertes")), wk))
            n_q += 1
        except Exception: continue
    dates_in_tx = set()
    for _, r in df_tx.iterrows():
        try: dates_in_tx.add(pd.to_datetime(r["Date"]).date().isoformat())
        except Exception: pass
    for d in dates_in_tx: c.execute("DELETE FROM transactions WHERE price_date = ?", (d,))
    for _, r in df_tx.iterrows():
        try:
            d = pd.to_datetime(r["Date"]).date()
            ticker = r.get("Ticker")
            if pd.isna(ticker) or not str(ticker).strip(): continue
            wk = d.isocalendar().week
            c.execute("""INSERT INTO transactions
                (price_date, instrument, ticker, carnet, cours_transaction, multiplicateur,
                 volume_mad, quantite, nb_transactions, week_number)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (d.isoformat(), r.get("Instrument"), str(ticker).strip(), r.get("Carnet"),
                 _f(r.get("Cours de transaction contrat")), _f(r.get("Multiplicateur de contrat")),
                 _f(r.get("Volume des échanges en MAD")), _f(r.get("Quantité échangée")),
                 _i(r.get("Nombre de transactions")), wk))
            n_t += 1
        except Exception: continue
    conn.commit(); conn.close()
    return n_q, n_t

def seed_from_excel():
    """Charge les fichiers Excel /data dans la base au premier lancement."""
    if is_seeded(): return (0, 0, 0)
    nm, nq, nt = 0, 0, 0
    if SEED_MASI.exists():
        try:
            df_m = pd.read_excel(SEED_MASI, sheet_name=0)
            df_m.columns = [str(c).strip() for c in df_m.columns]
            if "price_date" not in df_m.columns:
                df_m = df_m.rename(columns={df_m.columns[0]: "price_date", df_m.columns[1]: "close_price"})
            nm = upsert_masi20(df_m)
        except Exception as e:
            st.warning(f"Seed MASI: {e}")
    if SEED_BULLETIN.exists():
        try:
            xl = pd.ExcelFile(SEED_BULLETIN)
            sh_m = next((s for s in xl.sheet_names if "MSI" in s.upper() or "MASI" in s.upper() or "MARCH" in s.upper()), xl.sheet_names[0])
            sh_t = next((s for s in xl.sheet_names if "TRANS" in s.upper()), xl.sheet_names[1] if len(xl.sheet_names) > 1 else xl.sheet_names[0])
            dm = pd.read_excel(SEED_BULLETIN, sheet_name=sh_m, header=2)
            dm.columns = [str(c).strip() for c in dm.columns]
            dm = dm.rename(columns={"Date ": "Date", "Clôture (1)": "Clôture"}).dropna(subset=["Date", "Ticker"])
            dt = pd.read_excel(SEED_BULLETIN, sheet_name=sh_t, header=1)
            dt.columns = [str(c).strip() for c in dt.columns]
            dt = dt.dropna(subset=["Date"])
            nq, nt = upsert_bulletin(dm, dt)
        except Exception as e:
            st.warning(f"Seed bulletin: {e}")
    mark_seeded()
    return (nm, nq, nt)

def insert_masi20_single(d: date, close: float):
    conn = get_conn()
    conn.execute("INSERT OR REPLACE INTO masi20_history (price_date, close_price) VALUES (?, ?)", (d.isoformat(), close))
    conn.commit(); conn.close()

def insert_future_quote(d: date, ticker, ref, ouv, bas, haut, clot, comp, pos):
    conn = get_conn(); wk = d.isocalendar().week
    conn.execute("""INSERT OR REPLACE INTO futures_quotes
        (price_date, ticker, instrument, code_isin, sous_jacent, cours_reference, ouverture,
         plus_bas, plus_haut, cloture, cours_compensation, positions_ouvertes, week_number)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (d.isoformat(), ticker, CONTRACT_INSTRUMENT[ticker], CONTRACT_ISIN[ticker], "MASI20",
         _f(ref), _f(ouv), _f(bas), _f(haut), _f(clot), _f(comp), _f(pos), wk))
    conn.commit(); conn.close()

def insert_transaction(d: date, ticker, carnet, cours, mult, volume, qty, nb):
    conn = get_conn(); wk = d.isocalendar().week
    conn.execute("""INSERT INTO transactions
        (price_date, instrument, ticker, carnet, cours_transaction, multiplicateur,
         volume_mad, quantite, nb_transactions, week_number)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (d.isoformat(), CONTRACT_INSTRUMENT[ticker], ticker, carnet,
         _f(cours), _f(mult), _f(volume), _f(qty), _i(nb), wk))
    conn.commit(); conn.close()

def delete_transaction(tx_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM transactions WHERE id=?", (tx_id,))
    conn.commit(); conn.close()

def delete_day(d: date):
    conn = get_conn()
    conn.execute("DELETE FROM masi20_history WHERE price_date=?", (d.isoformat(),))
    conn.execute("DELETE FROM futures_quotes WHERE price_date=?", (d.isoformat(),))
    conn.execute("DELETE FROM transactions WHERE price_date=?", (d.isoformat(),))
    conn.commit(); conn.close()

@st.cache_data(ttl=10, show_spinner=False)
def load_masi20() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT price_date, close_price FROM masi20_history ORDER BY price_date", conn)
    conn.close()
    if not df.empty: df["price_date"] = pd.to_datetime(df["price_date"])
    return df

@st.cache_data(ttl=10, show_spinner=False)
def load_quotes(week=None, d=None) -> pd.DataFrame:
    conn = get_conn()
    q = "SELECT * FROM futures_quotes"; params = []; conds = []
    if week is not None: conds.append("week_number = ?"); params.append(week)
    if d is not None: conds.append("price_date = ?"); params.append(d.isoformat() if hasattr(d, 'isoformat') else d)
    if conds: q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY price_date, ticker"
    df = pd.read_sql_query(q, conn, params=params); conn.close()
    if not df.empty: df["price_date"] = pd.to_datetime(df["price_date"])
    return df

@st.cache_data(ttl=10, show_spinner=False)
def load_transactions(week=None, d=None) -> pd.DataFrame:
    conn = get_conn()
    q = "SELECT * FROM transactions"; params = []; conds = []
    if week is not None: conds.append("week_number = ?"); params.append(week)
    if d is not None: conds.append("price_date = ?"); params.append(d.isoformat() if hasattr(d, 'isoformat') else d)
    if conds: q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY price_date, ticker, cours_transaction"
    df = pd.read_sql_query(q, conn, params=params); conn.close()
    if not df.empty: df["price_date"] = pd.to_datetime(df["price_date"])
    return df

@st.cache_data(ttl=10, show_spinner=False)
def available_weeks() -> list:
    conn = get_conn()
    df = pd.read_sql_query("SELECT DISTINCT week_number FROM futures_quotes WHERE week_number IS NOT NULL ORDER BY week_number", conn)
    conn.close()
    return df["week_number"].tolist()

def clear_all_caches():
    st.cache_data.clear()


# ======================== INIT + AUTO-SEED ========================
init_db()
if not is_seeded() and (SEED_MASI.exists() or SEED_BULLETIN.exists()):
    with st.spinner("Initialisation de la base (historique)..."):
        nm, nq, nt = seed_from_excel()
    st.toast(f"Base initialisée : {nm} clôtures MASI 20, {nq} cotations, {nt} transactions.", icon="✅")
    clear_all_caches()


# ======================== HEADER ========================
st.markdown("""
<div class="bvc-band">
    <div class="sub">Bourse de Casablanca · Marché à Terme</div>
    <h1>📈 MASI 20 Futures — Plateforme de Suivi</h1>
    <div class="sub" style="margin-top:6px;">Bulletin Journalier · Saisie Manuelle · Bilan Hebdomadaire</div>
</div>
""", unsafe_allow_html=True)


# ======================== SIDEBAR ========================
with st.sidebar:
    st.markdown("### 📅 Période d'analyse")
    bilan_date = st.date_input("Date du bilan", value=date.today())
    weeks = available_weeks()
    if weeks:
        selected_week = st.selectbox("Semaine (ISO)", options=weeks, index=len(weeks)-1,
                                      format_func=lambda w: f"Semaine {w}")
    else:
        selected_week = None
        st.info("Base vide. Utilisez l'onglet Saisie.")

    st.divider()
    st.markdown("### 📊 État de la base")
    _m = load_masi20(); _q = load_quotes(); _t = load_transactions()
    st.metric("Clôtures MASI 20", len(_m))
    st.metric("Cotations futures", len(_q))
    st.metric("Transactions", len(_t))

    st.divider()
    with st.expander("⚙️ Maintenance"):
        if st.button("🔄 Rafraîchir cache", use_container_width=True):
            clear_all_caches(); st.rerun()
        if st.button("🗑️ Réinitialiser la base", use_container_width=True):
            if DB_PATH.exists(): DB_PATH.unlink()
            init_db(); clear_all_caches()
            st.success("Base réinitialisée."); st.rerun()


# ======================== MAIN TABS ========================
tab_dashboard, tab_saisie, tab_hebdo, tab_historique = st.tabs([
    "📊 Tableau de bord", "✍️ Saisie journalière", "🗓️ Bilan hebdomadaire", "📚 Historique"
])


# ======================== TAB: DASHBOARD ========================
with tab_dashboard:
    masi_df = load_masi20()
    quotes_day = load_quotes(d=bilan_date)
    tx_day = load_transactions(d=bilan_date)

    st.markdown(f"<div class='section-h'>📊 Bilan du {bilan_date.strftime('%d/%m/%Y')}</div>", unsafe_allow_html=True)

    masi_close, masi_delta = None, None
    if not masi_df.empty:
        row = masi_df[masi_df["price_date"].dt.date == bilan_date]
        if not row.empty:
            masi_close = row.iloc[0]["close_price"]
            prior = masi_df[masi_df["price_date"].dt.date < bilan_date].sort_values("price_date")
            if not prior.empty:
                prev = prior.iloc[-1]["close_price"]
                masi_delta = (masi_close - prev) / prev * 100

    cols = st.columns(5)
    with cols[0]:
        if masi_close is not None:
            d_html = ""
            if masi_delta is not None:
                cls = "delta-up" if masi_delta > 0 else ("delta-down" if masi_delta < 0 else "delta-flat")
                arrow = "▲" if masi_delta > 0 else ("▼" if masi_delta < 0 else "■")
                d_html = f"<div class='{cls}'>{arrow} {masi_delta:+.2f}%</div>"
            st.markdown(f"<div class='metric-card index-card'><div class='label'>Indice MASI 20</div><div class='value'>{masi_close:,.2f}</div>{d_html}</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div class='metric-card index-card'><div class='label'>Indice MASI 20</div><div class='value'>—</div></div>", unsafe_allow_html=True)

       # ======================== CARTES DES CONTRATS À TERME ========================
    for i, ticker in enumerate(CONTRACT_ORDER):
        with cols[i+1]:
            qsub = quotes_day[quotes_day["ticker"] == ticker] if not quotes_day.empty else pd.DataFrame()
            
            if not qsub.empty:
                row = qsub.iloc[0]
                # ✅ PRIORITÉ : Clôture si elle existe et > 0, sinon on prend Cours de Compensation
                cloture_display = row["cloture"]
                if pd.isna(cloture_display) or cloture_display <= 0:
                    cloture_display = row.get("cours_compensation", 0)
                
                ref = row["cours_reference"]
                
                delta_html = ""
                if cloture_display and ref and ref != 0:
                    chg = (cloture_display - ref) / ref * 100
                    cls = "delta-up" if chg > 0 else ("delta-down" if chg < 0 else "delta-flat")
                    arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "■")
                    delta_html = f"<div class='{cls}'>{arrow} {chg:+.2f}%</div>"

                # Petit indicateur quand on utilise la compensation
                note = " (Comp.)" if (pd.isna(row["cloture"]) or row["cloture"] <= 0) else ""

                st.markdown(f"""
                <div class='metric-card'>
                    <div class='label'>{CONTRACT_LABELS[ticker]}{note}</div>
                    <div class='value'>{cloture_display:,.2f}</div>
                    {delta_html}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='metric-card'><div class='label'>{CONTRACT_LABELS[ticker]}</div><div class='value'>—</div></div>", unsafe_allow_html=True)
    st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)
    c2 = st.columns(5)
    if not tx_day.empty:
        total_nb = int(tx_day["nb_transactions"].sum())
        total_vol = float(tx_day["volume_mad"].sum())
        total_qty = float(tx_day["quantite"].sum())
    else:
        total_nb, total_vol, total_qty = 0, 0.0, 0.0
    with c2[0]: st.markdown(f"<div class='metric-card'><div class='label'>Nb. Transactions</div><div class='value'>{total_nb}</div></div>", unsafe_allow_html=True)
    with c2[1]: st.markdown(f"<div class='metric-card'><div class='label'>Volume (MAD)</div><div class='value'>{total_vol:,.0f}</div></div>", unsafe_allow_html=True)
    with c2[2]: st.markdown(f"<div class='metric-card'><div class='label'>Quantité</div><div class='value'>{total_qty:,.0f}</div></div>", unsafe_allow_html=True)
    with c2[3]:
        pos = quotes_day["positions_ouvertes"].sum() if not quotes_day.empty else 0
        st.markdown(f"<div class='metric-card'><div class='label'>Positions Ouvertes</div><div class='value'>{pos:,.0f}</div></div>", unsafe_allow_html=True)
    with c2[4]:
        n_c = quotes_day["ticker"].nunique() if not quotes_day.empty else 0
        st.markdown(f"<div class='metric-card'><div class='label'>Contrats cotés</div><div class='value'>{n_c}</div></div>", unsafe_allow_html=True)

    st.markdown("<div class='section-h'>📈 Évolution des Cours</div>", unsafe_allow_html=True)
    ctab1, ctab2, ctab3 = st.tabs(["📊 MASI 20", "🗓️ Contrats à Terme", "🔀 Comparaison"])
    with ctab1:
        if masi_df.empty: st.info("Aucune donnée MASI 20.")
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=masi_df["price_date"], y=masi_df["close_price"], mode="lines", name="MASI 20",
                                     line=dict(color="#0b2545", width=2.5), fill="tozeroy", fillcolor="rgba(200,162,75,0.12)"))
            fig.update_layout(template="simple_white", height=440, margin=dict(l=10,r=10,t=30,b=10),
                              xaxis_title="Date", yaxis_title="Cours de clôture",
                              title=dict(text="Historique de l'indice MASI 20", font=dict(family="Georgia", size=16, color="#0b2545")))
            st.plotly_chart(fig, use_container_width=True)
    with ctab2:
        allq = load_quotes()
        if allq.empty: st.info("Aucune cotation future.")
        else:
            fig = go.Figure()
            for t in CONTRACT_ORDER:
                sub = allq[allq["ticker"]==t].sort_values("price_date")
                if sub.empty: continue
                fig.add_trace(go.Scatter(x=sub["price_date"], y=sub["cloture"], mode="lines+markers",
                                         name=CONTRACT_LABELS[t], line=dict(color=CONTRACT_COLORS[t], width=2.2), marker=dict(size=7)))
            fig.update_layout(template="simple_white", height=440, margin=dict(l=10,r=10,t=30,b=10),
                              xaxis_title="Date", yaxis_title="Cours de clôture", legend=dict(orientation="h", y=-0.2),
                              title=dict(text="Suivi des contrats à terme MASI 20", font=dict(family="Georgia", size=16, color="#0b2545")))
            st.plotly_chart(fig, use_container_width=True)
    with ctab3:
        allq = load_quotes()
        if masi_df.empty or allq.empty: st.info("Données insuffisantes.")
        else:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Scatter(x=masi_df["price_date"], y=masi_df["close_price"], name="MASI 20",
                                     line=dict(color="#0b2545", width=2.5)), secondary_y=False)
            for t in CONTRACT_ORDER:
                sub = allq[allq["ticker"]==t].sort_values("price_date")
                if sub.empty: continue
                fig.add_trace(go.Scatter(x=sub["price_date"], y=sub["cloture"], name=CONTRACT_LABELS[t],
                                         line=dict(color=CONTRACT_COLORS[t], width=1.8, dash="dot"), mode="lines+markers"), secondary_y=True)
            fig.update_layout(template="simple_white", height=440, margin=dict(l=10,r=10,t=30,b=10),
                              legend=dict(orientation="h", y=-0.2),
                              title=dict(text="MASI 20 vs Futures", font=dict(family="Georgia", size=16, color="#0b2545")))
            fig.update_yaxes(title_text="MASI 20", secondary_y=False)
            fig.update_yaxes(title_text="Futures (pts)", secondary_y=True)
            st.plotly_chart(fig, use_container_width=True)


# ======================== TAB: SAISIE JOURNALIÈRE ========================
with tab_saisie:
    st.markdown("<div class='section-h'>✍️ Saisie du bulletin journalier</div>", unsafe_allow_html=True)
    st.caption("Insérez les données d'une journée. Les valeurs s'ajoutent à la base historique.")

    saisie_date = st.date_input("📅 Date de saisie", value=date.today(), key="saisie_date")

    # FORM 1: MASI 20
    st.markdown("#### 1️⃣ Clôture de l'indice MASI 20")
    existing_m = load_masi20()
    existing_m = existing_m[existing_m["price_date"].dt.date == saisie_date] if not existing_m.empty else existing_m
    current_m = float(existing_m.iloc[0]["close_price"]) if not existing_m.empty else 0.0
    if not existing_m.empty:
        st.info(f"ℹ️ Valeur déjà en base : **{current_m:,.2f}** (sera écrasée si vous enregistrez)")
    with st.form("form_masi"):
        col1, col2 = st.columns([3, 1])
        with col1:
            masi_val = st.number_input("Cours de clôture MASI 20", min_value=0.0, value=current_m, step=0.01, format="%.4f")
        with col2:
            st.write(""); st.write("")
            sub_masi = st.form_submit_button("💾 Enregistrer MASI 20", use_container_width=True)
        if sub_masi:
            if masi_val > 0:
                insert_masi20_single(saisie_date, masi_val); clear_all_caches()
                st.success(f"✅ MASI 20 du {saisie_date.strftime('%d/%m/%Y')} : {masi_val:,.2f}"); st.rerun()
            else: st.error("Valeur invalide.")

    st.divider()

    # FORM 2: FUTURES QUOTES
    st.markdown("#### 2️⃣ Cotations des contrats à terme")
    st.caption("Saisissez pour les 4 échéances. Laissez 0 pour ignorer un contrat.")
    existing_q = load_quotes(d=saisie_date)
    with st.form("form_futures"):
        data_in = {}
        for ticker in CONTRACT_ORDER:
            prev = existing_q[existing_q["ticker"] == ticker] if not existing_q.empty else pd.DataFrame()
            has_prev = not prev.empty
            prefix = "📝 " if has_prev else ""
            st.markdown(f"**{prefix}{CONTRACT_LABELS[ticker]}** — `{ticker}`")
            def gv(col):
                if has_prev and pd.notna(prev.iloc[0][col]): return float(prev.iloc[0][col])
                return 0.0
            c1, c2, c3, c4 = st.columns(4)
            with c1: ref = st.number_input("Cours réf.", value=gv("cours_reference"), step=0.1, key=f"ref_{ticker}", format="%.2f")
            with c2: ouv = st.number_input("Ouverture", value=gv("ouverture"), step=0.1, key=f"ouv_{ticker}", format="%.2f")
            with c3: bas = st.number_input("+ Bas", value=gv("plus_bas"), step=0.1, key=f"bas_{ticker}", format="%.2f")
            with c4: haut = st.number_input("+ Haut", value=gv("plus_haut"), step=0.1, key=f"haut_{ticker}", format="%.2f")
            c5, c6, c7 = st.columns(3)
            with c5: clot = st.number_input("Clôture", value=gv("cloture"), step=0.1, key=f"clot_{ticker}", format="%.2f")
            with c6: comp = st.number_input("Cours compensation", value=gv("cours_compensation"), step=0.1, key=f"comp_{ticker}", format="%.2f")
            with c7: pos = st.number_input("Positions ouvertes", value=gv("positions_ouvertes"), step=1.0, key=f"pos_{ticker}")
            data_in[ticker] = (ref, ouv, bas, haut, clot, comp, pos)
            st.markdown("<hr style='margin:8px 0;border:0;border-top:1px solid #eee;'>", unsafe_allow_html=True)
        sub_fut = st.form_submit_button("💾 Enregistrer toutes les cotations", use_container_width=True)
        if sub_fut:
            n = 0
            for ticker, (ref, ouv, bas, haut, clot, comp, pos) in data_in.items():
                if clot > 0 or comp > 0:
                    insert_future_quote(saisie_date, ticker, ref, ouv, bas, haut, clot, comp, pos); n += 1
            clear_all_caches()
            st.success(f"✅ {n} cotations enregistrées pour le {saisie_date.strftime('%d/%m/%Y')}"); st.rerun()

    st.divider()

    # FORM 3: TRANSACTION
    st.markdown("#### 3️⃣ Ajouter une transaction")
    with st.form("form_tx", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            tx_ticker = st.selectbox("Contrat", options=CONTRACT_ORDER, format_func=lambda x: f"{CONTRACT_LABELS[x]} ({x})")
        with c2:
            tx_carnet = st.selectbox("Carnet", options=["Central", "Blocs", "Autre"])
        with c3:
            tx_mult = st.number_input("Multiplicateur", value=10.0, step=1.0)
        c4, c5, c6, c7 = st.columns(4)
        with c4: tx_cours = st.number_input("Cours de transaction", min_value=0.0, value=0.0, step=0.1, format="%.2f")
        with c5: tx_qty = st.number_input("Quantité", min_value=0.0, value=0.0, step=1.0)
        with c6: tx_vol = st.number_input("Volume (MAD)", min_value=0.0, value=0.0, step=1.0, help="0 = auto (cours × mult × qté)")
        with c7: tx_nb = st.number_input("Nb. transactions", min_value=0, value=1, step=1)
        sub_tx = st.form_submit_button("➕ Ajouter la transaction", use_container_width=True)
        if sub_tx:
            if tx_cours > 0 and tx_qty > 0:
                vol_final = tx_vol if tx_vol > 0 else tx_cours * tx_mult * tx_qty
                insert_transaction(saisie_date, tx_ticker, tx_carnet, tx_cours, tx_mult, vol_final, tx_qty, tx_nb)
                clear_all_caches()
                st.success(f"✅ Transaction : {tx_ticker} @ {tx_cours} × {tx_qty}"); st.rerun()
            else: st.error("Cours et quantité doivent être > 0")

    day_tx = load_transactions(d=saisie_date)
    if not day_tx.empty:
        st.markdown(f"**Transactions saisies pour le {saisie_date.strftime('%d/%m/%Y')} ({len(day_tx)})**")
        disp = day_tx[["id","ticker","carnet","cours_transaction","quantite","volume_mad","nb_transactions"]].rename(
            columns={"id":"ID","ticker":"Ticker","carnet":"Carnet","cours_transaction":"Cours",
                     "quantite":"Qté","volume_mad":"Volume MAD","nb_transactions":"Nb"})
        st.dataframe(disp, use_container_width=True, hide_index=True)
        col_d1, col_d2 = st.columns([2,1])
        with col_d1:
            del_id = st.number_input("ID à supprimer", min_value=0, value=0, step=1, key="del_tx_id")
        with col_d2:
            st.write(""); st.write("")
            if st.button("🗑️ Supprimer", use_container_width=True):
                if del_id > 0:
                    delete_transaction(int(del_id)); clear_all_caches()
                    st.success("Supprimée."); st.rerun()

    with st.expander("⚠️ Supprimer toute la journée"):
        if st.button(f"Tout supprimer le {saisie_date.strftime('%d/%m/%Y')}"):
            delete_day(saisie_date); clear_all_caches()
            st.success("Journée supprimée."); st.rerun()

    st.divider()
    with st.expander("📥 Import massif via Excel (optionnel)"):
        col1, col2 = st.columns(2)
        with col1:
            masi_file = st.file_uploader("Historique MASI 20", type=["xlsx","xls"], key="up_masi")
        with col2:
            bull_file = st.file_uploader("Bulletin CFR", type=["xlsx","xls"], key="up_bull")
        if st.button("Importer les fichiers", use_container_width=True):
            msg = []
            if masi_file:
                try:
                    df = pd.read_excel(masi_file)
                    df.columns = [str(c).strip() for c in df.columns]
                    if "price_date" not in df.columns:
                        df = df.rename(columns={df.columns[0]:"price_date", df.columns[1]:"close_price"})
                    n = upsert_masi20(df); msg.append(f"✅ MASI : {n}")
                except Exception as e: msg.append(f"❌ {e}")
            if bull_file:
                try:
                    xl = pd.ExcelFile(bull_file)
                    sh_m = next((s for s in xl.sheet_names if "MSI" in s.upper() or "MASI" in s.upper()), xl.sheet_names[0])
                    sh_t = next((s for s in xl.sheet_names if "TRANS" in s.upper()), xl.sheet_names[1] if len(xl.sheet_names)>1 else xl.sheet_names[0])
                    dm = pd.read_excel(bull_file, sheet_name=sh_m, header=2)
                    dm.columns = [str(c).strip() for c in dm.columns]
                    dm = dm.rename(columns={"Date ":"Date","Clôture (1)":"Clôture"}).dropna(subset=["Date","Ticker"])
                    dt = pd.read_excel(bull_file, sheet_name=sh_t, header=1)
                    dt.columns = [str(c).strip() for c in dt.columns]
                    dt = dt.dropna(subset=["Date"])
                    nq, nt = upsert_bulletin(dm, dt); msg.append(f"✅ Cot: {nq}, Tx: {nt}")
                except Exception as e: msg.append(f"❌ {e}")
            clear_all_caches()
            for m_ in msg: st.success(m_) if m_.startswith("✅") else st.error(m_)


# ======================== TAB: HEBDO ========================
with tab_hebdo:
    if selected_week is None:
        st.info("Aucune semaine. Saisissez d'abord des cotations.")
    else:
        st.markdown(f"<div class='section-h'>🗓️ Bilan Hebdomadaire — Semaine {selected_week}</div>", unsafe_allow_html=True)
        masi_df = load_masi20()
        if not masi_df.empty:
            mw = masi_df[masi_df["price_date"].dt.isocalendar().week == selected_week].copy()
            if not mw.empty:
                jours = {0:"Lundi",1:"Mardi",2:"Mercredi",3:"Jeudi",4:"Vendredi",5:"Samedi",6:"Dimanche"}
                mw["Jour"] = mw["price_date"].dt.weekday.map(jours)
                mw["Date"] = mw["price_date"].dt.strftime("%d/%m/%Y")
                d_ = mw[["Jour","Date","close_price"]].rename(columns={"close_price":"Clôture MASI 20"})
                d_["Clôture MASI 20"] = d_["Clôture MASI 20"].map(lambda v: f"{v:,.2f}")
                st.markdown("**Clôtures MASI 20 de la semaine**")
                st.dataframe(d_, use_container_width=True, hide_index=True)

        q_week = load_quotes(week=selected_week)
        tx_week = load_transactions(week=selected_week)

        if not q_week.empty:
            colA, colB = st.columns([3,2])
            with colA:
                st.markdown("**Clôture des contrats par jour**")
                pivot = q_week.pivot_table(index="price_date", columns="ticker", values="cloture", aggfunc="mean")
                pivot = pivot.reindex(columns=[c for c in CONTRACT_ORDER if c in pivot.columns])
                pivot.index = pivot.index.strftime("%a %d/%m")
                fig = go.Figure()
                for t in pivot.columns:
                    fig.add_trace(go.Bar(x=pivot.index, y=pivot[t], name=CONTRACT_LABELS[t],
                                         marker_color=CONTRACT_COLORS[t],
                                         text=[f"{v:,.1f}" if pd.notna(v) else "" for v in pivot[t]],
                                         textposition="outside", textfont=dict(size=9)))
                fig.update_layout(template="simple_white", barmode="group", height=400, margin=dict(l=10,r=10,t=30,b=10),
                                  xaxis_title="", yaxis_title="Clôture", legend=dict(orientation="h", y=-0.2),
                                  title=dict(text=f"Clôtures — S{selected_week}", font=dict(family="Georgia", size=14, color="#0b2545")))
                st.plotly_chart(fig, use_container_width=True)
            with colB:
                if not tx_week.empty:
                    st.markdown("**Volume par contrat**")
                    vc = tx_week.groupby("ticker")["volume_mad"].sum().reset_index()
                    vc["label"] = vc["ticker"].map(CONTRACT_LABELS).fillna(vc["ticker"])
                    fig = go.Figure(data=[go.Pie(labels=vc["label"], values=vc["volume_mad"], hole=0.5,
                        marker=dict(colors=[CONTRACT_COLORS.get(t,"#888") for t in vc["ticker"]], line=dict(color="#fff", width=2)),
                        textinfo="label+percent")])
                    fig.update_layout(height=400, margin=dict(l=10,r=10,t=30,b=10), showlegend=False,
                                      title=dict(text=f"Volume MAD — S{selected_week}", font=dict(family="Georgia", size=14, color="#0b2545")),
                                      annotations=[dict(text=f"{vc['volume_mad'].sum()/1e6:.1f}M<br>MAD", x=0.5, y=0.5,
                                                        font=dict(size=14, family="Georgia", color="#0b2545"), showarrow=False)])
                    st.plotly_chart(fig, use_container_width=True)

            if not tx_week.empty:
                st.markdown("**Quantités par contrat**")
                qc = tx_week.groupby("ticker")["quantite"].sum().reset_index()
                qc["label"] = qc["ticker"].map(CONTRACT_LABELS).fillna(qc["ticker"])
                fig = go.Figure(data=[go.Pie(labels=qc["label"], values=qc["quantite"], hole=0.4,
                    marker=dict(colors=[CONTRACT_COLORS.get(t,"#888") for t in qc["ticker"]], line=dict(color="#fff", width=2)),
                    textinfo="label+value+percent")])
                fig.update_layout(height=360, margin=dict(l=10,r=10,t=30,b=10), showlegend=False,
                                  title=dict(text=f"Quantité — S{selected_week}", font=dict(family="Georgia", size=14, color="#0b2545")))
                st.plotly_chart(fig, use_container_width=True)

        if not tx_week.empty:
            st.markdown("**Transactions de la semaine**")
            td = tx_week.copy(); td["Date"] = td["price_date"].dt.strftime("%d/%m/%Y")
            td = td[["Date","instrument","ticker","carnet","cours_transaction","multiplicateur","volume_mad","quantite","nb_transactions"]]
            td.columns = ["Date","Instrument","Ticker","Carnet","Cours","Mult.","Volume MAD","Qté","Nb Trans."]
            st.dataframe(td, use_container_width=True, hide_index=True)

        st.markdown("<div class='section-h'>📥 Export du bilan</div>", unsafe_allow_html=True)
        if st.button("📄 Générer le bilan Excel", use_container_width=True):
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                if not masi_df.empty:
                    mw = masi_df[masi_df["price_date"].dt.isocalendar().week == selected_week].copy()
                    if not mw.empty:
                        mw = mw.rename(columns={"price_date":"Date","close_price":"Clôture MASI 20"})
                        mw.to_excel(writer, sheet_name="MASI 20", index=False)
                if not q_week.empty:
                    qo = q_week.rename(columns={"price_date":"Date","ticker":"Ticker","instrument":"Instrument",
                        "code_isin":"Code ISIN","sous_jacent":"Sous jacent","cours_reference":"Cours de référence",
                        "ouverture":"Ouverture","plus_bas":"+ Bas","plus_haut":"+ Haut","cloture":"Clôture",
                        "cours_compensation":"Cours de compensation","positions_ouvertes":"Positions ouvertes"})[
                        ["Date","Instrument","Ticker","Code ISIN","Sous jacent","Cours de référence","Ouverture",
                         "+ Bas","+ Haut","Clôture","Cours de compensation","Positions ouvertes"]]
                    qo.to_excel(writer, sheet_name="Marché des MASI 20", index=False)
                if not tx_week.empty:
                    to = tx_week.rename(columns={"price_date":"Date","instrument":"Instrument","ticker":"Ticker",
                        "carnet":"Carnet","cours_transaction":"Cours de transaction","multiplicateur":"Multiplicateur",
                        "volume_mad":"Volume (MAD)","quantite":"Quantité","nb_transactions":"Nb. Transactions"})[
                        ["Date","Instrument","Ticker","Carnet","Cours de transaction","Multiplicateur",
                         "Volume (MAD)","Quantité","Nb. Transactions"]]
                    to.to_excel(writer, sheet_name="Transactions", index=False)
                rows = []
                if not q_week.empty:
                    for t in CONTRACT_ORDER:
                        s = q_week[q_week["ticker"]==t]
                        if s.empty: continue
                        rows.append({"Contrat":CONTRACT_LABELS[t],"Ticker":t,
                            "Clôture début":s.sort_values("price_date").iloc[0]["cloture"],
                            "Clôture fin":s.sort_values("price_date").iloc[-1]["cloture"],
                            "Volume total (MAD)":tx_week[tx_week["ticker"]==t]["volume_mad"].sum() if not tx_week.empty else 0,
                            "Quantité totale":tx_week[tx_week["ticker"]==t]["quantite"].sum() if not tx_week.empty else 0,
                            "Nb. transactions":tx_week[tx_week["ticker"]==t]["nb_transactions"].sum() if not tx_week.empty else 0})
                if rows: pd.DataFrame(rows).to_excel(writer, sheet_name="Résumé", index=False)
            buffer.seek(0)
            st.download_button(f"⬇️ Télécharger Bilan_S{selected_week}.xlsx", data=buffer,
                file_name=f"Bilan_MASI20_Futures_S{selected_week}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)


# ======================== TAB: HISTORIQUE ========================
with tab_historique:
    st.markdown("<div class='section-h'>📚 Exploration de l'historique</div>", unsafe_allow_html=True)
    sub1, sub2, sub3 = st.tabs(["MASI 20", "Cotations futures", "Transactions"])
    with sub1:
        df = load_masi20()
        if df.empty: st.info("Vide.")
        else:
            df2 = df.copy(); df2["Date"] = df2["price_date"].dt.strftime("%d/%m/%Y")
            st.dataframe(df2[["Date","close_price"]].rename(columns={"close_price":"Clôture MASI 20"}).sort_values("Date", ascending=False),
                         use_container_width=True, hide_index=True)
    with sub2:
        df = load_quotes()
        if df.empty: st.info("Vide.")
        else:
            df2 = df.copy(); df2["Date"] = df2["price_date"].dt.strftime("%d/%m/%Y")
            cols = ["Date","ticker","cours_reference","ouverture","plus_bas","plus_haut","cloture","cours_compensation","positions_ouvertes","week_number"]
            st.dataframe(df2[cols].rename(columns={"ticker":"Ticker","cours_reference":"Réf.","ouverture":"Ouv.",
                "plus_bas":"+Bas","plus_haut":"+Haut","cloture":"Clôture","cours_compensation":"Compens.",
                "positions_ouvertes":"Pos. Ouv.","week_number":"Sem."}).sort_values(["Date","Ticker"], ascending=[False,True]),
                use_container_width=True, hide_index=True)
    with sub3:
        df = load_transactions()
        if df.empty: st.info("Vide.")
        else:
            df2 = df.copy(); df2["Date"] = df2["price_date"].dt.strftime("%d/%m/%Y")
            cols = ["id","Date","ticker","carnet","cours_transaction","multiplicateur","volume_mad","quantite","nb_transactions","week_number"]
            st.dataframe(df2[cols].rename(columns={"id":"ID","ticker":"Ticker","carnet":"Carnet","cours_transaction":"Cours",
                "multiplicateur":"Mult.","volume_mad":"Volume MAD","quantite":"Qté","nb_transactions":"Nb",
                "week_number":"Sem."}).sort_values(["Date","Ticker"], ascending=[False,True]),
                use_container_width=True, hide_index=True)

st.markdown("---")
st.caption("© Plateforme de Suivi — Marché à Terme MASI 20 · Bourse de Casablanca")
