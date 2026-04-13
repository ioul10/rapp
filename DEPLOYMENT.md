# Déploiement sur Streamlit Cloud (GitHub)

## Pourquoi c'était lent

Sans `requirements.txt`, Streamlit Cloud essaie de deviner les dépendances → résolution lente de pip, versions qui rentrent en conflit, redémarrages. Avec versions épinglées + cache, le premier build prend ~2 min et les redémarrages suivants sont quasi instantanés.

## Fichiers ajoutés (à pousser sur GitHub)

```
masi20_platform/
├── app.py                  # application (avec @st.cache_data)
├── requirements.txt        # versions EXACTES
├── runtime.txt             # Python 3.11
├── .streamlit/config.toml  # thème + mode headless
├── .gitignore
├── masi20.db               # base pré-remplie (optionnel)
└── README.md
```

## Étapes

### 1. Pousser sur GitHub

```bash
cd masi20_platform
git init
git add .
git commit -m "Initial commit - MASI 20 Futures platform"
git branch -M main
git remote add origin https://github.com/VOTRE_USER/masi20-futures.git
git push -u origin main
```

### 2. Déployer sur Streamlit Cloud

1. Aller sur https://share.streamlit.io
2. **New app** → connecter GitHub
3. Sélectionner le repo, branche `main`, fichier principal `app.py`
4. **Advanced settings** → Python version : `3.11`
5. **Deploy**

Premier build : 1-3 minutes (installation des packages). Après, le redémarrage prend ~10 secondes.

## Si c'est toujours lent après déploiement

**Cause 1 — L'app se rendort (plan gratuit)**
Les apps gratuites Streamlit Cloud s'endorment après 7 jours sans trafic, ou quelques heures d'inactivité. Le premier clic les réveille → 30-60s. Solution : passer en plan payant, ou juste visiter l'app régulièrement.

**Cause 2 — La base SQLite est trop grosse**
Si `masi20.db` dépasse ~50 Mo, le clone Git devient lent. Solution : ne pas committer la base, la recréer au démarrage via les uploads utilisateur.

**Cause 3 — Trop de recalcul**
Déjà résolu avec `@st.cache_data(ttl=60)` sur toutes les lectures DB.

**Cause 4 — Packages lourds**
`pandas` + `plotly` + `openpyxl` = ~80 Mo au total. C'est le minimum pour ce type d'app. Évitez d'ajouter `scipy`, `scikit-learn`, `matplotlib` si pas nécessaires.

## Test local d'abord

Avant de pousser, testez localement avec les mêmes versions :

```bash
python -m venv .venv
source .venv/bin/activate   # ou .venv\Scripts\activate sur Windows
pip install -r requirements.txt
streamlit run app.py
```

Si ça marche en local avec ces versions exactes, ça marchera sur Streamlit Cloud.
