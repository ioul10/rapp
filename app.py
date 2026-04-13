import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import sqlite3
import os
import io
from datetime import datetime, date
import xlsxwriter

# ─── Config ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Marché à Terme MASI 20",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = "masi20.db"
TICKERS = {
    "FMASI20JUI26": "Juin 26",
    "FMASI20SEP26": "Sep 26",
    "FMASI20DEC26": "Déc 26",
    "FMASI20MAR27": "Mars 27",
}
TICKER_COLORS = {
    "FMASI20JUI26": "#1f77b4",
    "FMASI20SEP26": "#ff7f0e",
    "FMASI20DEC26": "#2ca02c",
    "FMASI20MAR27": "#d62728",
}

# ─── DB Init ────────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS masi20 (
            price_date TEXT PRIMARY KEY,
            close_price REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS marche (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, instrument TEXT, ticker TEXT, code_isin TEXT,
            type TEXT, sous_jacent TEXT, cours_reference REAL,
            ouverture REAL, bas REAL, haut REAL,
            cloture REAL, cours_compensation REAL, positions_ouvertes REAL,
            UNIQUE(date, ticker)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, instrument TEXT, ticker TEXT, carnet TEXT,
            cours_transaction REAL, multiplicateur REAL,
            volume_mad REAL, quantite REAL, nb_transactions REAL,
            UNIQUE(date, ticker, cours_transaction, quantite)
        )
    """)
    con.commit()
    con.close()

init_db()

# ─── DB Helpers ────────────────────────────────────────────────────────────────
def get_con(): return sqlite3.connect(DB_PATH)

def load_masi20():
    con = get_con()
    df = pd.read_sql("SELECT price_date, close_price FROM masi20 ORDER BY price_date ASC", con, parse_dates=["price_date"])
    con.close()
    return df

def load_marche():
    con = get_con()
    df = pd.read_sql("SELECT * FROM marche ORDER BY date ASC", con, parse_dates=["date"])
    con.close()
    return df

def load_transactions():
    con = get_con()
    df = pd.read_sql("SELECT * FROM transactions ORDER BY date ASC", con, parse_dates=["date"])
    con.close()
    return df

def upsert_masi20(df):
    con = get_con()
    cur = con.cursor()
    for _, row in df.iterrows():
        cur.execute("INSERT OR REPLACE INTO masi20(price_date, close_price) VALUES(?,?)",
                    (str(row["price_date"])[:10], float(row["close_price"])))
    con.commit(); con.close()

def upsert_marche(df):
    con = get_con()
    cur = con.cursor()
    for _, row in df.iterrows():
        cur.execute("""INSERT OR REPLACE INTO marche
            (date,instrument,ticker,code_isin,type,sous_jacent,cours_reference,
             ouverture,bas,haut,cloture,cours_compensation,positions_ouvertes)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (str(row["date"])[:10], row.get("instrument"), row.get("ticker"),
             row.get("code_isin"), row.get("type"), row.get("sous_jacent"),
             row.get("cours_reference"), row.get("ouverture"), row.get("bas"),
             row.get("haut"), row.get("cloture"), row.get("cours_compensation"),
             row.get("positions_ouvertes")))
    con.commit(); con.close()

def upsert_transactions(df):
    con = get_con()
    cur = con.cursor()
    for _, row in df.iterrows():
        if pd.isna(row.get("ticker")): continue
        cur.execute("""INSERT OR IGNORE INTO transactions
            (date,instrument,ticker,carnet,cours_transaction,multiplicateur,
             volume_mad,quantite,nb_transactions)
            VALUES(?,?,?,?,?,?,?,?,?)""",
            (str(row["date"])[:10], row.get("instrument"), row.get("ticker"),
             row.get("carnet"), row.get("cours_transaction"),
             row.get("multiplicateur"), row.get("volume_mad"),
             row.get("quantite"), row.get("nb_transactions")))
    con.commit(); con.close()

# ─── Parsing ────────────────────────────────────────────────────────────────────
def parse_masi20_file(uploaded):
    df = pd.read_excel(uploaded)
    df.columns = ["price_date", "close_price"]
    df["price_date"] = pd.to_datetime(df["price_date"])
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    return df.dropna()

def parse_bulletin_file(uploaded):
    xl = pd.read_excel(uploaded, sheet_name=None, header=None)
    results = {}

    # ── Sheet Marché
    raw_m = xl.get("Marché des MSI20", pd.DataFrame())
    if not raw_m.empty:
        # Row 1 = headers
        raw_m.columns = raw_m.iloc[1]
        raw_m = raw_m.iloc[2:].reset_index(drop=True)
        raw_m.columns = [
            "date","instrument","ticker","code_isin","type","sous_jacent",
            "cours_reference","ouverture","bas","haut","cloture",
            "cours_compensation","positions_ouvertes"
        ]
        raw_m["date"] = pd.to_datetime(raw_m["date"], errors="coerce")
        for c in ["cours_reference","ouverture","bas","haut","cloture","cours_compensation","positions_ouvertes"]:
            raw_m[c] = pd.to_numeric(raw_m[c], errors="coerce")
        results["marche"] = raw_m.dropna(subset=["date","ticker"])

    # ── Sheet Transactions
    raw_t = xl.get("Transactions", pd.DataFrame())
    if not raw_t.empty:
        raw_t.columns = raw_t.iloc[0]
        raw_t = raw_t.iloc[1:].reset_index(drop=True)
        raw_t.columns = [
            "date","instrument","ticker","carnet","cours_transaction",
            "multiplicateur","volume_mad","quantite","nb_transactions"
        ]
        raw_t["date"] = pd.to_datetime(raw_t["date"], errors="coerce")
        for c in ["cours_transaction","multiplicateur","volume_mad","quantite","nb_transactions"]:
            raw_t[c] = pd.to_numeric(raw_t[c], errors="coerce")
        results["transactions"] = raw_t.dropna(subset=["date"])

    return results

# ─── Sidebar Navigation ─────────────────────────────────────────────────────────
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/2/2c/BVMbvmlogo.png", width=120)
st.sidebar.title("🇲🇦 Marché à Terme")
page = st.sidebar.radio("Navigation", ["📥 Import & Bilan", "📊 Dashboard Journalier", "📅 Vue Hebdomadaire"])

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — IMPORT
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📥 Import & Bilan":
    st.title("📥 Import des données")
    st.markdown("Importez les fichiers quotidiens puis sélectionnez la date du bilan.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Cours de clôture MASI 20")
        masi_file = st.file_uploader("Fichier MASI 20 (.xlsx)", type=["xlsx"], key="masi")
    with col2:
        st.subheader("Bulletin CFR (Suivi de marché)")
        crf_file = st.file_uploader("Bulletin journalier (.xlsx)", type=["xlsx"], key="crf")

    bilan_date = st.date_input("📅 Date du bilan", value=date.today())

    if st.button("💾 Enregistrer et intégrer les données", type="primary"):
        msgs = []
        if masi_file:
            try:
                df_m = parse_masi20_file(masi_file)
                upsert_masi20(df_m)
                msgs.append(f"✅ MASI 20 : {len(df_m)} enregistrements intégrés.")
            except Exception as e:
                msgs.append(f"❌ Erreur MASI 20 : {e}")
        if crf_file:
            try:
                parsed = parse_bulletin_file(crf_file)
                if "marche" in parsed:
                    upsert_marche(parsed["marche"])
                    msgs.append(f"✅ Marché : {len(parsed['marche'])} lignes intégrées.")
                if "transactions" in parsed:
                    upsert_transactions(parsed["transactions"])
                    msgs.append(f"✅ Transactions : {len(parsed['transactions'])} lignes intégrées.")
            except Exception as e:
                msgs.append(f"❌ Erreur bulletin : {e}")
        if msgs:
            for m in msgs:
                st.info(m)
        else:
            st.warning("Aucun fichier chargé.")

    st.divider()
    st.subheader("📋 Aperçu de la base de données")
    tab_m, tab_t, tab_masi = st.tabs(["Marché des contrats", "Transactions", "MASI 20"])
    with tab_m:
        df_marche = load_marche()
        st.dataframe(df_marche, use_container_width=True)
    with tab_t:
        df_trans = load_transactions()
        st.dataframe(df_trans, use_container_width=True)
    with tab_masi:
        df_masi = load_masi20()
        st.dataframe(df_masi, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — DASHBOARD JOURNALIER
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📊 Dashboard Journalier":

    df_marche  = load_marche()
    df_trans   = load_transactions()
    df_masi    = load_masi20()

    st.title("📊 Bilan Journalier — Marché à Terme MASI 20")

    # ── Date selector
    if df_marche.empty:
        st.warning("Aucune donnée disponible. Veuillez importer des fichiers.")
        st.stop()

    available_dates = sorted(df_marche["date"].dt.date.unique(), reverse=True)
    selected_date = st.selectbox("Sélectionner la date du bilan", available_dates)

    day_marche = df_marche[df_marche["date"].dt.date == selected_date]
    day_trans  = df_trans[df_trans["date"].dt.date == selected_date]
    day_masi   = df_masi[df_masi["price_date"].dt.date == selected_date]

    # ── MASI 20 header
    masi_val = day_masi["close_price"].values[0] if not day_masi.empty else None

    st.markdown(f"""
    <div style='background:linear-gradient(135deg,#0a2342,#1a5276);padding:18px 28px;border-radius:12px;margin-bottom:18px'>
        <h2 style='color:#f0f0f0;margin:0;font-size:1.1rem;letter-spacing:2px'>MARCHÉ À TERME — MASI 20</h2>
        <h1 style='color:#27AE60;margin:4px 0 0 0;font-size:2.6rem;font-weight:800'>
            {"📈 " + f"{masi_val:,.2f}" if masi_val else "—"}
        </h1>
        <span style='color:#aaa;font-size:0.9rem'>{selected_date.strftime("%A %d %B %Y")}</span>
    </div>
    """, unsafe_allow_html=True)

    # ── KPI cards per contract
    cols = st.columns(4)
    for i, (ticker, label) in enumerate(TICKERS.items()):
        row = day_marche[day_marche["ticker"] == ticker]
        tx  = day_trans[day_trans["ticker"] == ticker].dropna(subset=["cours_transaction"])
        cloture  = row["cloture"].values[0]  if not row.empty else None
        nb_contrats = int(tx["quantite"].sum()) if not tx.empty else 0
        volume   = tx["volume_mad"].sum() if not tx.empty else 0
        with cols[i]:
            st.markdown(f"""
            <div style='background:#1a1a2e;border-left:4px solid {TICKER_COLORS[ticker]};
                        padding:14px;border-radius:8px;text-align:center'>
                <div style='color:#aaa;font-size:0.75rem;letter-spacing:1px'>{label}</div>
                <div style='color:white;font-size:1.8rem;font-weight:700'>
                    {f"{cloture:,.1f}" if cloture else "—"}
                </div>
                <div style='color:#888;font-size:0.78rem'>Clôture</div>
                <div style='margin-top:6px;display:flex;justify-content:space-around'>
                    <div>
                        <div style='color:#f39c12;font-size:1rem;font-weight:600'>{nb_contrats:,}</div>
                        <div style='color:#777;font-size:0.7rem'>Contrats</div>
                    </div>
                    <div>
                        <div style='color:#3498db;font-size:1rem;font-weight:600'>
                            {f"{volume/1e6:.2f}M" if volume > 0 else "—"}
                        </div>
                        <div style='color:#777;font-size:0.7rem'>Volume (MAD)</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    # ── Global daily totals
    total_contrats = int(day_trans.dropna(subset=["cours_transaction"])["quantite"].sum())
    total_volume   = day_trans.dropna(subset=["cours_transaction"])["volume_mad"].sum()
    st.markdown(f"""
    <div style='background:#111;border-radius:8px;padding:12px 22px;margin-top:14px;
                display:flex;gap:40px;align-items:center'>
        <span style='color:#aaa'>TOTAL JOURNÉE</span>
        <span style='color:#f1c40f;font-weight:700;font-size:1.1rem'>
            {total_contrats:,} contrats échangés
        </span>
        <span style='color:#3498db;font-weight:700;font-size:1.1rem'>
            Volume total : {total_volume:,.0f} MAD
        </span>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # ── Graphiques
    st.subheader("📈 Graphiques")
    chart_tab1, chart_tab2 = st.tabs(["MASI 20 — Historique", "Contrats à Terme — Évolution"])

    with chart_tab1:
        if not df_masi.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_masi["price_date"], y=df_masi["close_price"],
                mode="lines+markers", name="MASI 20",
                line=dict(color="#27AE60", width=2),
                fill="tozeroy", fillcolor="rgba(39,174,96,0.08)"
            ))
            fig.update_layout(
                title="MASI 20 — Cours de clôture historique",
                xaxis_title="Date", yaxis_title="Cours",
                template="plotly_dark", height=420,
                hovermode="x unified"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de données MASI 20 disponibles.")

    with chart_tab2:
        if not df_marche.empty:
            fig2 = go.Figure()
            for ticker, label in TICKERS.items():
                sub = df_marche[df_marche["ticker"] == ticker].sort_values("date")
                if not sub.empty:
                    fig2.add_trace(go.Scatter(
                        x=sub["date"], y=sub["cloture"],
                        mode="lines+markers", name=label,
                        line=dict(color=TICKER_COLORS[ticker], width=2)
                    ))
            fig2.update_layout(
                title="Contrats à Terme MASI 20 — Cours de clôture",
                xaxis_title="Date", yaxis_title="Prix",
                template="plotly_dark", height=420,
                hovermode="x unified",
                legend=dict(orientation="h", y=-0.2)
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Pas de données de marché disponibles.")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — VUE HEBDOMADAIRE
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📅 Vue Hebdomadaire":

    df_marche  = load_marche()
    df_trans   = load_transactions()
    df_masi    = load_masi20()

    st.title("📅 Bilan Hebdomadaire — Marché à Terme MASI 20")

    if df_marche.empty:
        st.warning("Aucune donnée. Importez d'abord les fichiers.")
        st.stop()

    # ── Week selector
    df_marche["week"] = df_marche["date"].dt.isocalendar().week.astype(int)
    df_marche["year"] = df_marche["date"].dt.year
    df_marche["week_label"] = df_marche.apply(
        lambda r: f"Semaine {int(r['week'])} — {int(r['year'])}", axis=1)

    weeks = df_marche[["week","year","week_label"]].drop_duplicates().sort_values(["year","week"])
    week_options = weeks["week_label"].tolist()
    selected_week_label = st.selectbox("Sélectionner la semaine", week_options, index=len(week_options)-1)

    sel_week_row = weeks[weeks["week_label"] == selected_week_label].iloc[0]
    week_marche = df_marche[(df_marche["week"] == sel_week_row["week"]) &
                             (df_marche["year"] == sel_week_row["year"])]
    week_dates  = week_marche["date"].dt.date.unique()
    week_trans  = df_trans[df_trans["date"].dt.date.isin(week_dates)]
    week_masi   = df_masi[df_masi["price_date"].dt.date.isin(week_dates)]

    st.markdown(f"### {selected_week_label}")

    # ── Daily closes MASI 20 table
    st.subheader("📋 MASI 20 — Clôtures journalières")
    if not week_masi.empty:
        day_names = {0:"Lundi",1:"Mardi",2:"Mercredi",3:"Jeudi",4:"Vendredi",5:"Samedi",6:"Dimanche"}
        week_masi["Jour"] = week_masi["price_date"].dt.weekday.map(day_names)
        week_masi_disp = week_masi[["Jour","price_date","close_price"]].copy()
        week_masi_disp.columns = ["Jour","Date","Clôture MASI 20"]
        week_masi_disp["Date"] = week_masi_disp["Date"].dt.strftime("%d/%m/%Y")
        st.dataframe(week_masi_disp.set_index("Jour"), use_container_width=True)
    else:
        st.info("Pas de données MASI 20 pour cette semaine.")

    st.divider()

    # ── Charts section
    st.subheader("📊 Graphiques de la semaine")
    g1, g2, g3 = st.tabs(["Clôtures (histogrammes)", "Volume (barres)", "Quantité (camembert)"])

    with g1:
        # Histogrammes cloture par ticker et date
        if not week_marche.empty:
            sub = week_marche[week_marche["ticker"].isin(TICKERS.keys())].copy()
            sub = sub.dropna(subset=["cloture"])
            sub["date_str"] = sub["date"].dt.strftime("%d/%m")
            sub["label"]    = sub["ticker"].map(TICKERS)
            fig_h = go.Figure()
            for ticker, label in TICKERS.items():
                d = sub[sub["ticker"] == ticker]
                fig_h.add_trace(go.Bar(
                    x=d["date_str"], y=d["cloture"], name=label,
                    marker_color=TICKER_COLORS[ticker],
                    text=d["cloture"].round(1), textposition="outside"
                ))
            fig_h.update_layout(
                barmode="group", template="plotly_dark",
                title="Cours de clôture par contrat et par jour",
                xaxis_title="Date", yaxis_title="Prix",
                height=400, legend=dict(orientation="h", y=-0.2)
            )
            st.plotly_chart(fig_h, use_container_width=True)

    with g2:
        # Volume par ticker
        if not week_trans.empty:
            tx_clean = week_trans.dropna(subset=["cours_transaction"])
            vol_by_ticker = tx_clean.groupby("ticker")["volume_mad"].sum().reset_index()
            vol_by_ticker["label"] = vol_by_ticker["ticker"].map(TICKERS)
            vol_by_ticker["color"] = vol_by_ticker["ticker"].map(TICKER_COLORS)
            fig_v = go.Figure(go.Bar(
                x=vol_by_ticker["label"], y=vol_by_ticker["volume_mad"],
                marker_color=vol_by_ticker["color"].tolist(),
                text=vol_by_ticker["volume_mad"].apply(lambda x: f"{x/1e6:.2f}M"),
                textposition="outside"
            ))
            fig_v.update_layout(
                template="plotly_dark", title="Volume des échanges par contrat (MAD)",
                xaxis_title="Contrat", yaxis_title="Volume (MAD)", height=380
            )
            st.plotly_chart(fig_v, use_container_width=True)

    with g3:
        # Quantité par ticker — camembert
        if not week_trans.empty:
            tx_clean = week_trans.dropna(subset=["cours_transaction"])
            qty_by_ticker = tx_clean.groupby("ticker")["quantite"].sum().reset_index()
            qty_by_ticker["label"] = qty_by_ticker["ticker"].map(TICKERS)
            fig_p = go.Figure(go.Pie(
                labels=qty_by_ticker["label"],
                values=qty_by_ticker["quantite"],
                hole=0.42,
                marker_colors=[TICKER_COLORS[t] for t in qty_by_ticker["ticker"]],
                textinfo="label+percent+value"
            ))
            fig_p.update_layout(
                template="plotly_dark",
                title="Répartition de la quantité échangée par contrat",
                height=400
            )
            st.plotly_chart(fig_p, use_container_width=True)

    st.divider()

    # ── Full transactions table for the week
    st.subheader("📑 Tableau des transactions de la semaine")
    if not week_trans.empty:
        tx_disp = week_trans.dropna(subset=["cours_transaction"]).copy()
        tx_disp["date"] = tx_disp["date"].dt.strftime("%d/%m/%Y")
        tx_disp["label"] = tx_disp["ticker"].map(TICKERS)
        tx_disp = tx_disp.rename(columns={
            "date":"Date","label":"Contrat","ticker":"Ticker",
            "cours_transaction":"Cours","multiplicateur":"Mult.",
            "volume_mad":"Volume (MAD)","quantite":"Qté","nb_transactions":"Nb Tx"
        })
        cols_show = ["Date","Contrat","Ticker","Cours","Volume (MAD)","Qté","Nb Tx"]
        st.dataframe(tx_disp[cols_show], use_container_width=True)
    else:
        st.info("Pas de transactions pour cette semaine.")

    st.divider()

    # ── Download weekly report
    st.subheader("⬇️ Télécharger le bilan de la semaine")

    @st.cache_data
    def build_weekly_excel(week_label, _week_masi, _week_marche, _week_trans):
        output = io.BytesIO()
        with xlsxwriter.Workbook(output, {"in_memory": True}) as wb:
            bold    = wb.add_format({"bold": True, "bg_color": "#0a2342", "font_color": "white", "border": 1})
            header  = wb.add_format({"bold": True, "bg_color": "#1a5276", "font_color": "white", "border": 1, "align": "center"})
            num_fmt = wb.add_format({"num_format": "#,##0.00", "border": 1})
            int_fmt = wb.add_format({"num_format": "#,##0", "border": 1})
            date_fmt= wb.add_format({"num_format": "dd/mm/yyyy", "border": 1})
            normal  = wb.add_format({"border": 1})

            # ── Sheet 1: MASI 20
            ws1 = wb.add_worksheet("MASI 20")
            ws1.set_column("A:B", 20)
            ws1.write(0, 0, week_label, bold)
            for j, col in enumerate(["Date","Clôture MASI 20"]):
                ws1.write(1, j, col, header)
            for i, (_, row) in enumerate(_week_masi.iterrows()):
                ws1.write(2+i, 0, str(row["price_date"])[:10], normal)
                ws1.write(2+i, 1, row["close_price"], num_fmt)

            # ── Sheet 2: Marché des contrats
            ws2 = wb.add_worksheet("Marché des MSI20")
            ws2.set_column("A:M", 18)
            cols_m = ["date","instrument","ticker","code_isin","type","sous_jacent",
                      "cours_reference","ouverture","bas","haut","cloture",
                      "cours_compensation","positions_ouvertes"]
            labels_m = ["Date","Instrument","Ticker","Code ISIN","Type","Sous-Jacent",
                        "Cours Réf.","Ouverture","Bas","Haut","Clôture",
                        "Cours Compensation","Positions Ouvertes"]
            for j, lbl in enumerate(labels_m):
                ws2.write(0, j, lbl, header)
            if not _week_marche.empty:
                sub = _week_marche[_week_marche["ticker"].isin(TICKERS.keys())]
                for i, (_, row) in enumerate(sub.iterrows()):
                    ws2.write(1+i, 0, str(row["date"])[:10], normal)
                    for j, c in enumerate(cols_m[1:], 1):
                        val = row.get(c)
                        if pd.isna(val): ws2.write(1+i, j, "", normal)
                        elif isinstance(val, float): ws2.write(1+i, j, val, num_fmt)
                        else: ws2.write(1+i, j, str(val), normal)

            # ── Sheet 3: Transactions
            ws3 = wb.add_worksheet("Transactions")
            ws3.set_column("A:I", 20)
            cols_t = ["date","instrument","ticker","carnet","cours_transaction",
                      "multiplicateur","volume_mad","quantite","nb_transactions"]
            labels_t = ["Date","Instrument","Ticker","Carnet","Cours Tx",
                        "Multiplicateur","Volume (MAD)","Quantité","Nb Tx"]
            for j, lbl in enumerate(labels_t):
                ws3.write(0, j, lbl, header)
            if not _week_trans.empty:
                tx_sub = _week_trans.dropna(subset=["cours_transaction"])
                for i, (_, row) in enumerate(tx_sub.iterrows()):
                    ws3.write(1+i, 0, str(row["date"])[:10], normal)
                    for j, c in enumerate(cols_t[1:], 1):
                        val = row.get(c)
                        if pd.isna(val): ws3.write(1+i, j, "", normal)
                        elif isinstance(val, float): ws3.write(1+i, j, val, num_fmt)
                        else: ws3.write(1+i, j, str(val), normal)

        output.seek(0)
        return output.read()

    excel_bytes = build_weekly_excel(
        selected_week_label,
        week_masi, week_marche, week_trans
    )
    st.download_button(
        label=f"📥 Télécharger le bilan — {selected_week_label}",
        data=excel_bytes,
        file_name=f"Bilan_{selected_week_label.replace(' ','_').replace('—','')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary"
    )
