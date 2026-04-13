"""
Plateforme de Suivi — Marché à Terme Marocain (MASI 20 Futures)
Bourse de Casablanca — Instruments Dérivés
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import sqlite3
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

import tempfile, os
# On Streamlit Cloud the filesystem is ephemeral; /tmp is writable.
# Locally, keep the DB next to app.py so data persists between runs.
_IS_CLOUD = os.environ.get("STREAMLIT_RUNTIME_ENV") == "cloud" or os.environ.get("HOSTNAME", "").startswith("streamlit")
_LOCAL_DB = Path(__file__).parent / "masi20.db"
if _IS_CLOUD or not _LOCAL_DB.parent.exists():
    DB_PATH = Path(tempfile.gettempdir()) / "masi20.db"
else:
    DB_PATH = _LOCAL_DB

CONTRACT_ORDER = ["FMASI20JUI26", "FMASI20SEP26", "FMASI20DEC26", "FMASI20MAR27"]
CONTRACT_LABELS = {
    "FMASI20JUI26": "Juin 2026",
    "FMASI20SEP26": "Septembre 2026",
    "FMASI20DEC26": "Décembre 2026",
    "FMASI20MAR27": "Mars 2027",
}
CONTRACT_COLORS = {
    "FMASI20JUI26": "#C8A24B",  # gold
    "FMASI20SEP26": "#1F6FEB",  # blue
    "FMASI20DEC26": "#D9534F",  # red
    "FMASI20MAR27": "#2E8B57",  # green
}

# ======================== STYLE (Bourse de Casablanca / Rapport) ========================
CUSTOM_CSS = """
<style>
    /* Global */
    .stApp {
        background: linear-gradient(180deg, #f7f5f0 0%, #ffffff 40%);
    }
    /* Headline band */
    .bvc-band {
        background: linear-gradient(90deg, #0b2545 0%, #13315c 50%, #8d5524 100%);
        color: #f7f5f0;
        padding: 18px 26px;
        border-radius: 6px;
        margin-bottom: 18px;
        border-left: 6px solid #C8A24B;
        box-shadow: 0 2px 8px rgba(11,37,69,0.15);
    }
    .bvc-band h1 {
        margin: 0;
        font-family: 'Georgia', serif;
        font-size: 26px;
        letter-spacing: 0.5px;
    }
    .bvc-band .sub {
        font-size: 13px;
        opacity: 0.85;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin-top: 4px;
    }
    /* Metric cards */
    .metric-card {
        background: #ffffff;
        border: 1px solid #e5e1d8;
        border-top: 3px solid #C8A24B;
        border-radius: 4px;
        padding: 14px 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        height: 100%;
    }
    .metric-card .label {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        color: #6b6358;
        font-weight: 600;
    }
    .metric-card .value {
        font-family: 'Georgia', serif;
        font-size: 22px;
        color: #0b2545;
        margin-top: 4px;
        font-weight: 700;
    }
    .metric-card .delta-up { color: #2E8B57; font-size: 12px; font-weight: 600; }
    .metric-card .delta-down { color: #D9534F; font-size: 12px; font-weight: 600; }
    .metric-card .delta-flat { color: #6b6358; font-size: 12px; }

    .index-card {
        background: linear-gradient(135deg, #0b2545 0%, #13315c 100%);
        color: #f7f5f0;
        border-left: 4px solid #C8A24B;
    }
    .index-card .label { color: #C8A24B; }
    .index-card .value { color: #ffffff; }

    /* Section headers */
    .section-h {
        font-family: 'Georgia', serif;
        color: #0b2545;
        border-bottom: 2px solid #C8A24B;
        padding-bottom: 6px;
        margin: 18px 0 12px 0;
        font-size: 18px;
    }
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: #0b2545;
    }
    [data-testid="stSidebar"] * {
        color: #f7f5f0 !important;
    }
    [data-testid="stSidebar"] .stButton button {
        background: #C8A24B;
        color: #0b2545 !important;
        border: none;
        font-weight: 700;
    }
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: #efe9db;
        padding: 4px;
        border-radius: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        color: #0b2545;
        font-weight: 600;
        padding: 8px 16px;
    }
    .stTabs [aria-selected="true"] {
        background: #0b2545 !important;
        color: #C8A24B !important;
    }
    /* Dataframe */
    .stDataFrame { border: 1px solid #e5e1d8; border-radius: 4px; }

    footer { visibility: hidden; }
    #MainMenu { visibility: hidden; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ======================== DATABASE ========================
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS masi20_history (
            price_date DATE PRIMARY KEY,
            close_price REAL NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS futures_quotes (
            price_date DATE NOT NULL,
            ticker TEXT NOT NULL,
            instrument TEXT,
            code_isin TEXT,
            sous_jacent TEXT,
            cours_reference REAL,
            ouverture REAL,
            plus_bas REAL,
            plus_haut REAL,
            cloture REAL,
            cours_compensation REAL,
            positions_ouvertes REAL,
            week_number INTEGER,
            PRIMARY KEY (price_date, ticker)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            price_date DATE NOT NULL,
            instrument TEXT,
            ticker TEXT,
            carnet TEXT,
            cours_transaction REAL,
            multiplicateur REAL,
            volume_mad REAL,
            quantite REAL,
            nb_transactions INTEGER,
            week_number INTEGER
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(price_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tx_week ON transactions(week_number)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_fq_week ON futures_quotes(week_number)")
    conn.commit()
    conn.close()

def upsert_masi20(df: pd.DataFrame) -> int:
    conn = get_conn()
    c = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        try:
            d = pd.to_datetime(row["price_date"]).date()
            p = float(row["close_price"])
            c.execute(
                "INSERT OR REPLACE INTO masi20_history (price_date, close_price) VALUES (?, ?)",
                (d.isoformat(), p),
            )
            count += 1
        except Exception:
            continue
    conn.commit()
    conn.close()
    return count

def upsert_bulletin(df_market: pd.DataFrame, df_tx: pd.DataFrame) -> tuple[int, int]:
    conn = get_conn()
    c = conn.cursor()
    n_q, n_t = 0, 0
    for _, r in df_market.iterrows():
        try:
            d = pd.to_datetime(r["Date"]).date()
            ticker = str(r["Ticker"]).strip()
            if not ticker or ticker == "nan":
                continue
            wk = d.isocalendar().week
            c.execute("""
                INSERT OR REPLACE INTO futures_quotes
                (price_date, ticker, instrument, code_isin, sous_jacent,
                 cours_reference, ouverture, plus_bas, plus_haut, cloture,
                 cours_compensation, positions_ouvertes, week_number)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                d.isoformat(), ticker,
                r.get("Instrument"), r.get("Code ISIN"), r.get("Sous jacent"),
                _f(r.get("Cours de réference")), _f(r.get("Ouverture")),
                _f(r.get("+ bas")), _f(r.get("+haut")), _f(r.get("Clôture")),
                _f(r.get("Cours de Compensation")), _f(r.get("Positions Ouvertes")),
                wk,
            ))
            n_q += 1
        except Exception:
            continue

    # Clear transactions for the dates being imported then reinsert
    dates_in_tx = set()
    for _, r in df_tx.iterrows():
        try:
            d = pd.to_datetime(r["Date"]).date()
            dates_in_tx.add(d.isoformat())
        except Exception:
            pass
    for d in dates_in_tx:
        c.execute("DELETE FROM transactions WHERE price_date = ?", (d,))

    for _, r in df_tx.iterrows():
        try:
            d = pd.to_datetime(r["Date"]).date()
            ticker = r.get("Ticker")
            if pd.isna(ticker) or not str(ticker).strip():
                continue  # skip subtotal rows
            wk = d.isocalendar().week
            c.execute("""
                INSERT INTO transactions
                (price_date, instrument, ticker, carnet, cours_transaction,
                 multiplicateur, volume_mad, quantite, nb_transactions, week_number)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                d.isoformat(), r.get("Instrument"), str(ticker).strip(),
                r.get("Carnet"), _f(r.get("Cours de transaction contrat")),
                _f(r.get("Multiplicateur de contrat")), _f(r.get("Volume des échanges en MAD")),
                _f(r.get("Quantité échangée")), _i(r.get("Nombre de transactions")),
                wk,
            ))
            n_t += 1
        except Exception:
            continue
    conn.commit()
    conn.close()
    return n_q, n_t

def _f(v):
    try:
        if pd.isna(v): return None
        return float(v)
    except Exception:
        return None

def _i(v):
    try:
        if pd.isna(v): return None
        return int(v)
    except Exception:
        return None

@st.cache_data(ttl=60, show_spinner=False)
def load_masi20() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT price_date, close_price FROM masi20_history ORDER BY price_date", conn)
    conn.close()
    if not df.empty:
        df["price_date"] = pd.to_datetime(df["price_date"])
    return df

@st.cache_data(ttl=60, show_spinner=False)
def load_quotes(week: int | None = None, d: date | None = None) -> pd.DataFrame:
    conn = get_conn()
    q = "SELECT * FROM futures_quotes"
    params = []
    conds = []
    if week is not None:
        conds.append("week_number = ?")
        params.append(week)
    if d is not None:
        conds.append("price_date = ?")
        params.append(d.isoformat())
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY price_date, ticker"
    df = pd.read_sql_query(q, conn, params=params)
    conn.close()
    if not df.empty:
        df["price_date"] = pd.to_datetime(df["price_date"])
    return df

@st.cache_data(ttl=60, show_spinner=False)
def load_transactions(week: int | None = None) -> pd.DataFrame:
    conn = get_conn()
    q = "SELECT * FROM transactions"
    params = []
    if week is not None:
        q += " WHERE week_number = ?"
        params.append(week)
    q += " ORDER BY price_date, ticker, cours_transaction"
    df = pd.read_sql_query(q, conn, params=params)
    conn.close()
    if not df.empty:
        df["price_date"] = pd.to_datetime(df["price_date"])
    return df

def available_weeks() -> list[int]:
    conn = get_conn()
    df = pd.read_sql_query("SELECT DISTINCT week_number FROM futures_quotes WHERE week_number IS NOT NULL ORDER BY week_number", conn)
    conn.close()
    return df["week_number"].tolist()


# ======================== HEADER ========================
st.markdown("""
<div class="bvc-band">
    <div class="sub">Bourse de Casablanca · Marché à Terme</div>
    <h1>📈 MASI 20 Futures — Plateforme de Suivi</h1>
    <div class="sub" style="margin-top:6px;">Bulletin Journalier · Bilan Hebdomadaire · Analyse des Contrats</div>
</div>
""", unsafe_allow_html=True)

init_db()

# ======================== SIDEBAR: IMPORT ========================
with st.sidebar:
    st.markdown("### 📥 Import des données")
    st.caption("Ajoutez le bulletin journalier CFR et l'historique MASI 20.")

    masi_file = st.file_uploader("Historique MASI 20 (Excel)", type=["xlsx", "xls"], key="masi")
    bulletin_file = st.file_uploader("Bulletin CFR (Excel)", type=["xlsx", "xls"], key="bull")

    bilan_date = st.date_input("Date du bilan", value=date(2026, 4, 10))

    if st.button("🔄 Charger dans la base", use_container_width=True):
        msg = []
        if masi_file is not None:
            try:
                df_m = pd.read_excel(masi_file, sheet_name=0)
                df_m.columns = [str(c).strip() for c in df_m.columns]
                if "price_date" not in df_m.columns or "close_price" not in df_m.columns:
                    # try to map
                    df_m = df_m.rename(columns={df_m.columns[0]: "price_date", df_m.columns[1]: "close_price"})
                n = upsert_masi20(df_m)
                msg.append(f"✅ MASI 20 : {n} lignes")
            except Exception as e:
                msg.append(f"❌ MASI: {e}")
        if bulletin_file is not None:
            try:
                xl = pd.ExcelFile(bulletin_file)
                # Find sheets
                sh_market = next((s for s in xl.sheet_names if "MSI" in s.upper() or "MASI" in s.upper() or "MARCH" in s.upper()), xl.sheet_names[0])
                sh_tx = next((s for s in xl.sheet_names if "TRANS" in s.upper()), xl.sheet_names[1] if len(xl.sheet_names) > 1 else xl.sheet_names[0])
                df_market = pd.read_excel(bulletin_file, sheet_name=sh_market, header=2)
                df_market.columns = [str(c).strip() for c in df_market.columns]
                df_market = df_market.rename(columns={
                    "Date ": "Date",
                    "Clôture (1)": "Clôture",
                })
                df_market = df_market.dropna(subset=["Date", "Ticker"])
                df_tx = pd.read_excel(bulletin_file, sheet_name=sh_tx, header=1)
                df_tx.columns = [str(c).strip() for c in df_tx.columns]
                df_tx = df_tx.dropna(subset=["Date"])
                nq, nt = upsert_bulletin(df_market, df_tx)
                msg.append(f"✅ Cotations : {nq} · Transactions : {nt}")
            except Exception as e:
                msg.append(f"❌ Bulletin: {e}")
        st.cache_data.clear()
        for m in msg:
            st.success(m) if m.startswith("✅") else st.error(m)
        if not msg:
            st.warning("Aucun fichier fourni.")

    st.divider()
    st.markdown("### 📅 Période d'analyse")
    weeks = available_weeks()
    if weeks:
        selected_week = st.selectbox(
            "Semaine (ISO)",
            options=weeks,
            index=len(weeks) - 1,
            format_func=lambda w: f"Semaine {w}",
        )
    else:
        selected_week = None
        st.info("Importez un bulletin pour commencer.")

    st.divider()
    with st.expander("🗑️ Maintenance"):
        if st.button("Réinitialiser la base", use_container_width=True):
            if DB_PATH.exists():
                DB_PATH.unlink()
            init_db()
            st.success("Base réinitialisée.")
            st.rerun()


# ======================== HEADER METRICS ========================
masi_df = load_masi20()
quotes_day = load_quotes(d=bilan_date)
tx_day = load_transactions()
tx_day_filter = tx_day[tx_day["price_date"].dt.date == bilan_date] if not tx_day.empty else tx_day

st.markdown(f"<div class='section-h'>📊 Bilan du {bilan_date.strftime('%d/%m/%Y')}</div>", unsafe_allow_html=True)

# MASI 20 closing for bilan_date
masi_close = None
masi_delta = None
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
        st.markdown(f"""
        <div class="metric-card index-card">
            <div class="label">Indice MASI 20</div>
            <div class="value">{masi_close:,.2f}</div>
            {d_html}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="metric-card index-card">
            <div class="label">Indice MASI 20</div>
            <div class="value">—</div>
        </div>
        """, unsafe_allow_html=True)

# Futures per contract
for i, ticker in enumerate(CONTRACT_ORDER):
    with cols[i + 1]:
        q = quotes_day[quotes_day["ticker"] == ticker] if not quotes_day.empty else pd.DataFrame()
        if not q.empty:
            cloture = q.iloc[0]["cloture"]
            ref = q.iloc[0]["cours_reference"]
            delta_html = ""
            if cloture and ref and ref != 0:
                chg = (cloture - ref) / ref * 100
                cls = "delta-up" if chg > 0 else ("delta-down" if chg < 0 else "delta-flat")
                arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "■")
                delta_html = f"<div class='{cls}'>{arrow} {chg:+.2f}%</div>"
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">{CONTRACT_LABELS[ticker]}</div>
                <div class="value">{cloture:,.2f}</div>
                {delta_html}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">{CONTRACT_LABELS[ticker]}</div>
                <div class="value">—</div>
            </div>
            """, unsafe_allow_html=True)

# Second row: transactions
st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)
c2 = st.columns(5)
if not tx_day_filter.empty:
    total_nb = int(tx_day_filter["nb_transactions"].sum())
    total_vol = float(tx_day_filter["volume_mad"].sum())
    total_qty = float(tx_day_filter["quantite"].sum())
else:
    total_nb, total_vol, total_qty = 0, 0.0, 0.0

with c2[0]:
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">Nb. Transactions</div>
        <div class="value">{total_nb}</div>
    </div>
    """, unsafe_allow_html=True)
with c2[1]:
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">Volume Échangé (MAD)</div>
        <div class="value">{total_vol:,.0f}</div>
    </div>
    """, unsafe_allow_html=True)
with c2[2]:
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">Quantité Échangée</div>
        <div class="value">{total_qty:,.0f}</div>
    </div>
    """, unsafe_allow_html=True)
with c2[3]:
    if not quotes_day.empty:
        pos = quotes_day["positions_ouvertes"].sum()
        st.markdown(f"""
        <div class="metric-card">
            <div class="label">Positions Ouvertes</div>
            <div class="value">{pos:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("<div class='metric-card'><div class='label'>Positions Ouvertes</div><div class='value'>—</div></div>", unsafe_allow_html=True)
with c2[4]:
    n_contracts = quotes_day["ticker"].nunique() if not quotes_day.empty else 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">Contrats cotés</div>
        <div class="value">{n_contracts}</div>
    </div>
    """, unsafe_allow_html=True)


# ======================== TABS: CHARTS ========================
st.markdown("<div class='section-h'>📈 Évolution des Cours</div>", unsafe_allow_html=True)

tab_masi, tab_fut, tab_compare = st.tabs(["📊 MASI 20", "🗓️ Contrats à Terme", "🔀 Comparaison"])

with tab_masi:
    if masi_df.empty:
        st.info("Importez l'historique MASI 20 pour afficher le graphique.")
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=masi_df["price_date"], y=masi_df["close_price"],
            mode="lines", name="MASI 20",
            line=dict(color="#0b2545", width=2.5),
            fill="tozeroy",
            fillcolor="rgba(200,162,75,0.12)",
        ))
        fig.update_layout(
            template="simple_white",
            height=440,
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_title="Date", yaxis_title="Cours de clôture",
            yaxis=dict(rangemode="tozero", autorange=True),
            title=dict(text="Historique de l'indice MASI 20", font=dict(family="Georgia", size=16, color="#0b2545")),
        )
        fig.update_yaxes(autorange=True)
        st.plotly_chart(fig, use_container_width=True)

with tab_fut:
    all_q = load_quotes()
    if all_q.empty:
        st.info("Importez le bulletin CFR pour afficher les contrats.")
    else:
        fig = go.Figure()
        for t in CONTRACT_ORDER:
            sub = all_q[all_q["ticker"] == t].sort_values("price_date")
            if sub.empty:
                continue
            fig.add_trace(go.Scatter(
                x=sub["price_date"], y=sub["cloture"],
                mode="lines+markers", name=CONTRACT_LABELS[t],
                line=dict(color=CONTRACT_COLORS[t], width=2.2),
                marker=dict(size=7),
            ))
        fig.update_layout(
            template="simple_white",
            height=440,
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_title="Date", yaxis_title="Cours de clôture",
            legend=dict(orientation="h", y=-0.2),
            title=dict(text="Suivi des contrats à terme MASI 20", font=dict(family="Georgia", size=16, color="#0b2545")),
        )
        st.plotly_chart(fig, use_container_width=True)

with tab_compare:
    all_q = load_quotes()
    if masi_df.empty or all_q.empty:
        st.info("Données insuffisantes.")
    else:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(
            x=masi_df["price_date"], y=masi_df["close_price"],
            name="MASI 20", line=dict(color="#0b2545", width=2.5, dash="solid")
        ), secondary_y=False)
        for t in CONTRACT_ORDER:
            sub = all_q[all_q["ticker"] == t].sort_values("price_date")
            if sub.empty: continue
            fig.add_trace(go.Scatter(
                x=sub["price_date"], y=sub["cloture"],
                name=CONTRACT_LABELS[t], line=dict(color=CONTRACT_COLORS[t], width=1.8, dash="dot"),
                mode="lines+markers",
            ), secondary_y=True)
        fig.update_layout(template="simple_white", height=440, margin=dict(l=10,r=10,t=30,b=10),
                          legend=dict(orientation="h", y=-0.2),
                          title=dict(text="MASI 20 vs Futures", font=dict(family="Georgia", size=16, color="#0b2545")))
        fig.update_yaxes(title_text="MASI 20", secondary_y=False)
        fig.update_yaxes(title_text="Futures (pts)", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)


# ======================== WEEKLY VIEW ========================
if selected_week is not None:
    st.markdown(f"<div class='section-h'>🗓️ Bilan Hebdomadaire — Semaine {selected_week}</div>", unsafe_allow_html=True)

    # MASI 20 closures per weekday
    if not masi_df.empty:
        masi_week = masi_df[masi_df["price_date"].dt.isocalendar().week == selected_week].copy()
        if not masi_week.empty:
            jours_fr = {0:"Lundi",1:"Mardi",2:"Mercredi",3:"Jeudi",4:"Vendredi",5:"Samedi",6:"Dimanche"}
            masi_week["Jour"] = masi_week["price_date"].dt.weekday.map(jours_fr)
            masi_week["Date"] = masi_week["price_date"].dt.strftime("%d/%m/%Y")
            masi_week_display = masi_week[["Jour", "Date", "close_price"]].rename(columns={"close_price": "Clôture MASI 20"})
            masi_week_display["Clôture MASI 20"] = masi_week_display["Clôture MASI 20"].map(lambda v: f"{v:,.2f}")

            st.markdown("**Clôtures MASI 20 de la semaine**")
            st.dataframe(masi_week_display, use_container_width=True, hide_index=True)

    # Load week data
    q_week = load_quotes(week=selected_week)
    tx_week = load_transactions(week=selected_week)

    if not q_week.empty:
        colA, colB = st.columns([3, 2])

        with colA:
            st.markdown("**Clôture des contrats par jour**")
            pivot = q_week.pivot_table(index="price_date", columns="ticker", values="cloture", aggfunc="mean")
            pivot = pivot.reindex(columns=[c for c in CONTRACT_ORDER if c in pivot.columns])
            pivot.index = pivot.index.strftime("%a %d/%m")
            fig = go.Figure()
            for t in pivot.columns:
                fig.add_trace(go.Bar(
                    x=pivot.index, y=pivot[t],
                    name=CONTRACT_LABELS[t],
                    marker_color=CONTRACT_COLORS[t],
                    text=[f"{v:,.1f}" if pd.notna(v) else "" for v in pivot[t]],
                    textposition="outside", textfont=dict(size=9),
                ))
            fig.update_layout(
                template="simple_white", barmode="group",
                height=400, margin=dict(l=10, r=10, t=30, b=10),
                xaxis_title="", yaxis_title="Clôture",
                legend=dict(orientation="h", y=-0.2),
                title=dict(text=f"Clôtures hebdomadaires — S{selected_week}", font=dict(family="Georgia", size=14, color="#0b2545")),
            )
            st.plotly_chart(fig, use_container_width=True)

        with colB:
            if not tx_week.empty:
                st.markdown("**Répartition du volume par contrat**")
                vol_contract = tx_week.groupby("ticker")["volume_mad"].sum().reset_index()
                vol_contract["label"] = vol_contract["ticker"].map(CONTRACT_LABELS).fillna(vol_contract["ticker"])
                fig = go.Figure(data=[go.Pie(
                    labels=vol_contract["label"],
                    values=vol_contract["volume_mad"],
                    hole=0.5,
                    marker=dict(colors=[CONTRACT_COLORS.get(t, "#888") for t in vol_contract["ticker"]],
                                line=dict(color="#ffffff", width=2)),
                    textinfo="label+percent",
                    textfont=dict(size=11),
                )])
                fig.update_layout(
                    height=400, margin=dict(l=10, r=10, t=30, b=10),
                    showlegend=False,
                    title=dict(text=f"Volume échangé (MAD) — S{selected_week}", font=dict(family="Georgia", size=14, color="#0b2545")),
                    annotations=[dict(text=f"{vol_contract['volume_mad'].sum()/1e6:.1f}M<br>MAD",
                                      x=0.5, y=0.5, font=dict(size=14, family="Georgia", color="#0b2545"), showarrow=False)],
                )
                st.plotly_chart(fig, use_container_width=True)

        if not tx_week.empty:
            st.markdown("**Quantités échangées par contrat (hebdo)**")
            qty_contract = tx_week.groupby("ticker")["quantite"].sum().reset_index()
            qty_contract["label"] = qty_contract["ticker"].map(CONTRACT_LABELS).fillna(qty_contract["ticker"])
            fig = go.Figure(data=[go.Pie(
                labels=qty_contract["label"], values=qty_contract["quantite"],
                hole=0.4,
                marker=dict(colors=[CONTRACT_COLORS.get(t, "#888") for t in qty_contract["ticker"]],
                            line=dict(color="#ffffff", width=2)),
                textinfo="label+value+percent",
            )])
            fig.update_layout(height=360, margin=dict(l=10,r=10,t=30,b=10), showlegend=False,
                              title=dict(text=f"Quantité échangée — S{selected_week}",
                                         font=dict(family="Georgia", size=14, color="#0b2545")))
            st.plotly_chart(fig, use_container_width=True)

    # Transactions table
    if not tx_week.empty:
        st.markdown("**Tableau des transactions de la semaine**")
        tx_display = tx_week.copy()
        tx_display["Date"] = tx_display["price_date"].dt.strftime("%d/%m/%Y")
        tx_display = tx_display[["Date", "instrument", "ticker", "carnet", "cours_transaction",
                                 "multiplicateur", "volume_mad", "quantite", "nb_transactions"]]
        tx_display.columns = ["Date", "Instrument", "Ticker", "Carnet", "Cours transaction",
                              "Multiplicateur", "Volume (MAD)", "Quantité", "Nb. Trans."]
        st.dataframe(tx_display, use_container_width=True, hide_index=True)

    # ============== DOWNLOAD WEEKLY REPORT ==============
    st.markdown("<div class='section-h'>📥 Téléchargement du bilan</div>", unsafe_allow_html=True)

    if st.button("📄 Générer le bilan Excel de la semaine", use_container_width=True):
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            # Sheet 1: MASI 20 weekly
            if not masi_df.empty:
                mw = masi_df[masi_df["price_date"].dt.isocalendar().week == selected_week].copy()
                if not mw.empty:
                    mw = mw.rename(columns={"price_date": "Date", "close_price": "Clôture MASI 20"})
                    mw.to_excel(writer, sheet_name="MASI 20", index=False)
            # Sheet 2: Cotations futures
            if not q_week.empty:
                q_out = q_week.rename(columns={
                    "price_date":"Date","ticker":"Ticker","instrument":"Instrument",
                    "code_isin":"Code ISIN","sous_jacent":"Sous jacent",
                    "cours_reference":"Cours de référence","ouverture":"Ouverture",
                    "plus_bas":"+ Bas","plus_haut":"+ Haut","cloture":"Clôture",
                    "cours_compensation":"Cours de compensation",
                    "positions_ouvertes":"Positions ouvertes",
                })[["Date","Instrument","Ticker","Code ISIN","Sous jacent","Cours de référence",
                    "Ouverture","+ Bas","+ Haut","Clôture","Cours de compensation","Positions ouvertes"]]
                q_out.to_excel(writer, sheet_name="Marché des MASI 20", index=False)
            # Sheet 3: Transactions
            if not tx_week.empty:
                tx_out = tx_week.rename(columns={
                    "price_date":"Date","instrument":"Instrument","ticker":"Ticker",
                    "carnet":"Carnet","cours_transaction":"Cours de transaction",
                    "multiplicateur":"Multiplicateur","volume_mad":"Volume (MAD)",
                    "quantite":"Quantité","nb_transactions":"Nb. Transactions",
                })[["Date","Instrument","Ticker","Carnet","Cours de transaction",
                    "Multiplicateur","Volume (MAD)","Quantité","Nb. Transactions"]]
                tx_out.to_excel(writer, sheet_name="Transactions", index=False)
            # Sheet 4: Résumé
            summary_rows = []
            if not q_week.empty:
                for t in CONTRACT_ORDER:
                    sub = q_week[q_week["ticker"] == t]
                    if sub.empty: continue
                    summary_rows.append({
                        "Contrat": CONTRACT_LABELS[t],
                        "Ticker": t,
                        "Clôture début sem.": sub.sort_values("price_date").iloc[0]["cloture"],
                        "Clôture fin sem.": sub.sort_values("price_date").iloc[-1]["cloture"],
                        "Volume total (MAD)": tx_week[tx_week["ticker"]==t]["volume_mad"].sum() if not tx_week.empty else 0,
                        "Quantité totale": tx_week[tx_week["ticker"]==t]["quantite"].sum() if not tx_week.empty else 0,
                        "Nb. transactions": tx_week[tx_week["ticker"]==t]["nb_transactions"].sum() if not tx_week.empty else 0,
                    })
            if summary_rows:
                pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Résumé", index=False)

        buffer.seek(0)
        st.download_button(
            label=f"⬇️ Télécharger Bilan_S{selected_week}.xlsx",
            data=buffer,
            file_name=f"Bilan_MASI20_Futures_S{selected_week}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

st.markdown("---")
st.caption("© Plateforme de Suivi — Marché à Terme MASI 20 · Bourse de Casablanca")
