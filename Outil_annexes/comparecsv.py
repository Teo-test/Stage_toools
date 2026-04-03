#!/usr/bin/env python3
"""
CSV Comparator — Comparateur interactif de fichiers CSV
Usage: python csv_comparator.py [fichier1.csv fichier2.csv ...]
"""

import sys
import os
import csv
import argparse
from pathlib import Path

# Vérification des dépendances
try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import numpy as np
except ImportError as e:
    print(f"[ERREUR] Dépendance manquante : {e}")
    print("Installe les dépendances : pip install pandas matplotlib numpy")
    sys.exit(1)

try:
    import openpyxl
    EXCEL_OK = True
except ImportError:
    EXCEL_OK = False

# ─── Couleurs terminal ──────────────────────────────────────────────────────
class C:
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    CYAN   = "\033[96m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    RED    = "\033[91m"
    BLUE   = "\033[94m"
    MAGENTA= "\033[95m"
    RESET  = "\033[0m"

def titre(texte):
    largeur = 60
    print(f"\n{C.CYAN}{C.BOLD}{'─' * largeur}{C.RESET}")
    print(f"{C.CYAN}{C.BOLD}  {texte}{C.RESET}")
    print(f"{C.CYAN}{C.BOLD}{'─' * largeur}{C.RESET}")

def info(texte):    print(f"  {C.BLUE}ℹ {C.RESET}{texte}")
def ok(texte):      print(f"  {C.GREEN}✔ {C.RESET}{texte}")
def warn(texte):    print(f"  {C.YELLOW}⚠ {C.RESET}{texte}")
def erreur(texte):  print(f"  {C.RED}✘ {C.RESET}{texte}")

# ─── Séparateurs ────────────────────────────────────────────────────────────────
SEPARATEURS = {
    "1": ("Auto-détection",     None),
    "2": ("Virgule      ( , )", ","),
    "3": ("Point-virgule ( ; )", ";"),
    "4": ("Tabulation   ( \\t )", "\t"),
    "5": ("Pipe         ( | )", "|"),
    "6": ("Espace       (   )", " "),
}

SEP_EXPORT = {
    "1": ("Virgule      ( , )", ","),
    "2": ("Point-virgule ( ; )", ";"),
    "3": ("Tabulation   ( \\t )", "\t"),
    "4": ("Pipe         ( | )", "|"),
}

def choisir_separateur(titre_q="Séparateur :", avec_auto=True):
    """Sélectionne un séparateur via menu numéroté."""
    table = SEPARATEURS if avec_auto else SEP_EXPORT
    print(f"\n{C.BOLD}  {titre_q}{C.RESET}")
    for k, (label, _) in table.items():
        print(f"    {C.CYAN}{k}{C.RESET}. {label}")
    print()
    while True:
        entree = input(f"  {C.BOLD}Choix{C.RESET} : ").strip()
        if entree in table:
            return table[entree][1]
        erreur("Entrée invalide, réessaie.")

def menu_numerote(titre_menu, options, allow_multiple=False):
    """Affiche un menu numéroté et retourne le(s) choix."""
    print(f"\n{C.BOLD}  {titre_menu}{C.RESET}")
    for i, opt in enumerate(options, 1):
        print(f"    {C.CYAN}{i:>2}{C.RESET}. {opt}")
    print()

    while True:
        if allow_multiple:
            entree = input(f"  {C.BOLD}Choix (ex: 1 3 5 ou 'all'){C.RESET} : ").strip()
            if entree.lower() == "all":
                return list(range(len(options)))
            try:
                choix = [int(x) - 1 for x in entree.split()]
                if all(0 <= c < len(options) for c in choix) and choix:
                    return choix
            except ValueError:
                pass
        else:
            entree = input(f"  {C.BOLD}Choix{C.RESET} : ").strip()
            try:
                c = int(entree) - 1
                if 0 <= c < len(options):
                    return c
            except ValueError:
                pass
        erreur("Entrée invalide, réessaie.")

def input_prompt(texte, defaut=None):
    """Input avec valeur par défaut."""
    if defaut:
        reponse = input(f"  {C.BOLD}{texte}{C.RESET} [{C.DIM}{defaut}{C.RESET}] : ").strip()
        return reponse if reponse else defaut
    return input(f"  {C.BOLD}{texte}{C.RESET} : ").strip()

# ─── Chargement des CSV ─────────────────────────────────────────────────────
def charger_csv(chemin, sep=None):
    """Charge un CSV avec détection automatique du séparateur."""
    try:
        if sep is None:
            with open(chemin, 'r', encoding='utf-8-sig') as f:
                echantillon = f.read(4096)
                dialect = csv.Sniffer().sniff(echantillon, delimiters=',;|\t')
                sep = dialect.delimiter
        df = pd.read_csv(chemin, sep=sep, encoding='utf-8-sig')
        return df, sep
    except Exception as e:
        erreur(f"Impossible de charger {chemin} : {e}")
        return None, None

def charger_fichiers(chemins, sep=None):
    """Charge plusieurs fichiers CSV."""
    datasets = {}
    for chemin in chemins:
        p = Path(chemin)
        if not p.exists():
            warn(f"Fichier introuvable : {chemin}")
            continue
        df, sep_utilise = charger_csv(chemin, sep)
        if df is not None:
            datasets[p.stem] = {"df": df, "chemin": chemin, "sep": sep_utilise}
            ok(f"Chargé : {C.BOLD}{p.name}{C.RESET}  ({len(df)} lignes, {len(df.columns)} colonnes, sep='{sep_utilise}')")
    return datasets

def ajouter_fichier(datasets):
    """Ajoute interactivement un fichier."""
    chemin = input_prompt("Chemin du fichier CSV").strip('"').strip("'")
    p = Path(chemin)
    if not p.exists():
        erreur(f"Fichier introuvable : {chemin}")
        return
    sep = choisir_separateur("Séparateur du fichier :", avec_auto=True)
    df, sep_utilise = charger_csv(chemin, sep)
    if df is not None:
        datasets[p.stem] = {"df": df, "chemin": chemin, "sep": sep_utilise}
        ok(f"Ajouté : {C.BOLD}{p.name}{C.RESET}  (sep='{sep_utilise}')")

# ─── Aperçu des données ─────────────────────────────────────────────────────
def afficher_apercu(datasets):
    """Affiche un résumé de chaque dataset."""
    titre("APERÇU DES DONNÉES")
    for nom, d in datasets.items():
        df = d["df"]
        print(f"\n  {C.BOLD}{C.MAGENTA}{nom}{C.RESET}  →  {d['chemin']}")
        print(f"  {C.DIM}Lignes: {len(df)}  |  Colonnes: {len(df.columns)}{C.RESET}")
        
        cols_num = df.select_dtypes(include='number').columns.tolist()
        cols_cat = df.select_dtypes(exclude='number').columns.tolist()
        
        if cols_num:
            print(f"  {C.GREEN}Numériques:{C.RESET} {', '.join(cols_num)}")
        if cols_cat:
            print(f"  {C.YELLOW}Texte/Date:{C.RESET} {', '.join(cols_cat)}")
        
        print(f"\n{df.head(3).to_string(index=False)}\n")

def afficher_stats(datasets):
    """Affiche les statistiques descriptives."""
    titre("STATISTIQUES DESCRIPTIVES")
    noms = list(datasets.keys())
    idx = menu_numerote("Quel dataset ?", noms + ["Tous"])
    
    selection = noms if idx == len(noms) else [noms[idx]]
    for nom in selection:
        df = datasets[nom]["df"]
        print(f"\n  {C.BOLD}{C.MAGENTA}── {nom} ──{C.RESET}")
        print(df.describe().to_string())

# ─── Sélection des colonnes ─────────────────────────────────────────────────

def _mini_barre(valeurs, largeur=12):
    """
    Génère une mini barre ASCII proportionnelle à la valeur.
    Pour colonnes numériques : barre de progression.
    Pour colonnes texte : affiche les valeurs uniques.
    """
    try:
        nums = pd.to_numeric(valeurs, errors='coerce').dropna()
        if len(nums) == 0:
            raise ValueError
        vmin, vmax = nums.min(), nums.max()
        etendue = vmax - vmin if vmax != vmin else 1
        # Résumé : min / moy / max
        moy = nums.mean()
        remplissage = int((moy - vmin) / etendue * largeur)
        barre = "█" * remplissage + "░" * (largeur - remplissage)
        return (f"{C.DIM}[{C.CYAN}{barre}{C.DIM}]{C.RESET} "
                f"{C.DIM}min={vmin:.3g} moy={moy:.3g} max={vmax:.3g} "
                f"n={len(nums)}{C.RESET}")
    except Exception:
        pass

    # Colonne texte : valeurs uniques
    uniques = valeurs.dropna().astype(str).unique()
    n = len(uniques)
    apercu = ", ".join(uniques[:4])
    if n > 4:
        apercu += f", … ({n} uniques)"
    return f"{C.DIM}[{apercu}]{C.RESET}"

def choisir_colonne(df, message, filtre_num=False):
    """
    Fait choisir une colonne avec mini-visu des valeurs pour chaque option.
    """
    if filtre_num:
        cols = df.select_dtypes(include='number').columns.tolist()
        if not cols:
            erreur("Aucune colonne numérique disponible.")
            return None
    else:
        cols = df.columns.tolist()

    print(f"\n{C.BOLD}  {message}{C.RESET}")
    for i, col in enumerate(cols, 1):
        visu = _mini_barre(df[col])
        print(f"    {C.CYAN}{i:>2}{C.RESET}. {C.BOLD}{col:<20}{C.RESET}  {visu}")
    print()

    while True:
        entree = input(f"  {C.BOLD}Choix{C.RESET} : ").strip()
        try:
            c = int(entree) - 1
            if 0 <= c < len(cols):
                return cols[c]
        except ValueError:
            pass
        erreur("Entrée invalide, réessaie.")

def choisir_colonne_multi(df, message, filtre_num=False):
    """
    Fait choisir plusieurs colonnes avec mini-visu des valeurs.
    """
    if filtre_num:
        cols = df.select_dtypes(include='number').columns.tolist()
        if not cols:
            erreur("Aucune colonne numérique disponible.")
            return []
    else:
        cols = df.columns.tolist()

    print(f"\n{C.BOLD}  {message}{C.RESET}")
    for i, col in enumerate(cols, 1):
        visu = _mini_barre(df[col])
        print(f"    {C.CYAN}{i:>2}{C.RESET}. {C.BOLD}{col:<20}{C.RESET}  {visu}")
    print()

    while True:
        entree = input(f"  {C.BOLD}Choix (ex: 1 3 ou 'all'){C.RESET} : ").strip()
        if entree.lower() == "all":
            return cols
        try:
            idxs = [int(x) - 1 for x in entree.split()]
            if idxs and all(0 <= i < len(cols) for i in idxs):
                return [cols[i] for i in idxs]
        except ValueError:
            pass
        erreur("Entrée invalide, réessaie.")

def choisir_datasets(datasets, allow_multiple=True):
    """Fait choisir un ou plusieurs datasets."""
    noms = list(datasets.keys())
    if allow_multiple and len(noms) > 1:
        idxs = menu_numerote("Quels fichiers ? (ex: 1 2 ou 'all')", noms, allow_multiple=True)
        return [noms[i] for i in idxs]
    elif len(noms) == 1:
        return noms
    else:
        idx = menu_numerote("Quel fichier ?", noms)
        return [noms[idx]]

def demander_titres(titre_defaut="", xlabel_defaut="", ylabel_defaut=""):
    """Demande interactivement le titre du graphe et les labels des axes."""
    t = input_prompt("Titre du graphe", titre_defaut)
    x = input_prompt("Label axe X",    xlabel_defaut)
    y = input_prompt("Label axe Y",    ylabel_defaut)
    return (t or titre_defaut), (x or xlabel_defaut), (y or ylabel_defaut)

# ─── Types de graphes ───────────────────────────────────────────────────────
TYPES_GRAPHE = {
    "Ligne (X vs Y)":           "ligne",
    "Nuage de points (scatter)": "scatter",
    "Barres":                   "barres",
    "Histogramme":              "histo",
    "Boîte à moustaches":       "boxplot",
    "Aire empilée":             "aire",
    "Corrélation (heatmap)":    "heatmap",
}

def appliquer_style(ax, titre_graphe, xlabel="", ylabel=""):
    """Style commun pour tous les graphes."""
    ax.set_title(titre_graphe, fontsize=13, fontweight='bold', pad=12)
    if xlabel: ax.set_xlabel(xlabel, fontsize=10)
    if ylabel: ax.set_ylabel(ylabel, fontsize=10)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

def couleurs_datasets(n):
    """Palette de couleurs distinctes."""
    return cm.tab10(np.linspace(0, 0.9, max(n, 1)))

# ── Graphe ligne ─────────────────────────────────────────────────────────────
def graphe_ligne(datasets, selection):
    print()
    # Construire la liste de toutes les séries : (nom_dataset, col_y)
    series = []
    for nom in selection:
        df = datasets[nom]["df"]
        cols_y = choisir_colonne_multi(
            df,
            f"[{C.MAGENTA}{nom}{C.RESET}] Colonnes Y",
            filtre_num=True
        )
        for col_y in cols_y:
            series.append((nom, col_y))

    if not series:
        return

    # Choisir X pour chaque série
    titre("CHOIX DE L'AXE X PAR SÉRIE")
    series_configs = []
    for nom, col_y in series:
        df = datasets[nom]["df"]
        info(f"Série  {C.BOLD}{C.MAGENTA}{nom}{C.RESET} — {C.BOLD}{col_y}{C.RESET}")
        col_x = choisir_colonne(df, f"Colonne X pour '{col_y}'")
        if col_x:
            series_configs.append((nom, col_x, col_y))

    if not series_configs:
        return

    x_def = ", ".join(dict.fromkeys(c[1] for c in series_configs))
    titre_g, xlabel, ylabel = demander_titres("Graphe Ligne", x_def, "Valeur")

    fig, ax = plt.subplots(figsize=(10, 5))
    clrs = couleurs_datasets(len(series_configs))
    for i, (nom, col_x, col_y) in enumerate(series_configs):
        df = datasets[nom]["df"]
        label = f"{nom} — {col_y}" if len(selection) > 1 else col_y
        ax.plot(df[col_x], df[col_y], label=label, color=clrs[i],
                linewidth=1.8, marker='o', markersize=3)

    appliquer_style(ax, titre_g, xlabel, ylabel)
    ax.legend(fontsize=8)
    plt.tight_layout()
    plt.show()

# ── Scatter ──────────────────────────────────────────────────────────────────
def graphe_scatter(datasets, selection):
    print()
    series = []
    for nom in selection:
        df = datasets[nom]["df"]
        cols_y = choisir_colonne_multi(
            df,
            f"[{C.MAGENTA}{nom}{C.RESET}] Colonnes Y (numériques)",
            filtre_num=True
        )
        for col_y in cols_y:
            series.append((nom, col_y))

    if not series:
        return

    titre("CHOIX DE L'AXE X PAR SÉRIE")
    series_configs = []
    for nom, col_y in series:
        df = datasets[nom]["df"]
        info(f"Série  {C.BOLD}{C.MAGENTA}{nom}{C.RESET} — {C.BOLD}{col_y}{C.RESET}")
        col_x = choisir_colonne(df, f"Colonne X pour '{col_y}'", filtre_num=True)
        if col_x:
            series_configs.append((nom, col_x, col_y))

    if not series_configs:
        return

    x_def = ", ".join(dict.fromkeys(c[1] for c in series_configs))
    titre_g, xlabel, ylabel = demander_titres("Nuage de points", x_def, "Y")

    fig, ax = plt.subplots(figsize=(8, 6))
    clrs = couleurs_datasets(len(series_configs))
    for i, (nom, col_x, col_y) in enumerate(series_configs):
        df = datasets[nom]["df"]
        label = f"{nom} — {col_y}" if len(selection) > 1 else col_y
        ax.scatter(df[col_x], df[col_y], label=label, color=clrs[i],
                   alpha=0.65, s=30, edgecolors='none')

    appliquer_style(ax, titre_g, xlabel, ylabel)
    if len(series_configs) > 1:
        ax.legend(fontsize=8)
    plt.tight_layout()
    plt.show()

# ── Barres ───────────────────────────────────────────────────────────────────
def graphe_barres(datasets, selection):
    print()
    series = []
    for nom in selection:
        df = datasets[nom]["df"]
        cols_y = choisir_colonne_multi(
            df,
            f"[{C.MAGENTA}{nom}{C.RESET}] Colonnes Y (valeurs)",
            filtre_num=True
        )
        for col_y in cols_y:
            series.append((nom, col_y))

    if not series:
        return

    titre("CHOIX DE L'AXE X PAR SÉRIE")
    series_configs = []
    for nom, col_y in series:
        df = datasets[nom]["df"]
        info(f"Série  {C.BOLD}{C.MAGENTA}{nom}{C.RESET} — {C.BOLD}{col_y}{C.RESET}")
        col_x = choisir_colonne(df, f"Colonne catégories X pour '{col_y}'")
        if col_x:
            series_configs.append((nom, col_x, col_y))

    if not series_configs:
        return

    x_def = ", ".join(dict.fromkeys(c[1] for c in series_configs))
    titre_g, xlabel, ylabel = demander_titres("Graphe Barres", x_def, "Valeur")

    fig, ax = plt.subplots(figsize=(10, 5))
    n    = len(series_configs)
    clrs = couleurs_datasets(n)
    width = 0.8 / max(n, 1)

    # Référence des catégories X de la première série pour positionner les ticks
    nom0, col_x0, col_y0 = series_configs[0]
    grouped0 = datasets[nom0]["df"].groupby(col_x0)[col_y0].mean().reset_index()
    x_ref = np.arange(len(grouped0))

    for i, (nom, col_x, col_y) in enumerate(series_configs):
        df = datasets[nom]["df"]
        grouped = df.groupby(col_x)[col_y].mean().reset_index()
        x = np.arange(len(grouped))
        offset = (i - n / 2 + 0.5) * width
        label = f"{nom} — {col_y}" if len(selection) > 1 else col_y
        ax.bar(x + offset, grouped[col_y], width=width * 0.9,
               label=label, color=clrs[i], alpha=0.85)
        if i == 0:
            ax.set_xticks(x)
            ax.set_xticklabels(grouped[col_x], rotation=45, ha='right', fontsize=8)

    appliquer_style(ax, titre_g, xlabel, ylabel)
    if n > 1: ax.legend(fontsize=8)
    plt.tight_layout()
    plt.show()

# ── Histogramme ──────────────────────────────────────────────────────────────
def graphe_histo(datasets, selection):
    print()
    df_ref = datasets[selection[0]]["df"]
    col = choisir_colonne(df_ref, "Colonne à distribuer", filtre_num=True)
    if col is None: return
    bins_str = input_prompt("Nombre de bins", "20")
    try: bins = int(bins_str)
    except: bins = 20

    titre_g, xlabel, ylabel = demander_titres(f"Distribution : {col}", col, "Fréquence")

    fig, ax = plt.subplots(figsize=(9, 5))
    couleurs = couleurs_datasets(len(selection))
    for i, nom in enumerate(selection):
        df = datasets[nom]["df"]
        if col not in df.columns: continue
        ax.hist(df[col].dropna(), bins=bins, label=nom, color=couleurs[i], alpha=0.6, edgecolor='white', linewidth=0.5)

    appliquer_style(ax, titre_g, xlabel, ylabel)
    if len(selection) > 1: ax.legend(fontsize=8)
    plt.tight_layout()
    plt.show()

# ── Boxplot ───────────────────────────────────────────────────────────────────
def graphe_boxplot(datasets, selection):
    print()
    df_ref = datasets[selection[0]]["df"]
    cols = choisir_colonne_multi(df_ref, "Colonnes à comparer", filtre_num=True)
    if not cols:
        return

    titre_g, _, ylabel = demander_titres("Boîtes à moustaches", "", "Valeur")

    fig, axes = plt.subplots(1, len(cols), figsize=(4 * len(cols), 5), squeeze=False)
    couleurs = couleurs_datasets(len(selection))

    for j, col in enumerate(cols):
        ax = axes[0][j]
        data = []
        labels = []
        for i, nom in enumerate(selection):
            df = datasets[nom]["df"]
            if col in df.columns:
                data.append(df[col].dropna().values)
                labels.append(nom)
        bp = ax.boxplot(data, labels=labels, patch_artist=True, medianprops=dict(color='black', linewidth=2))
        for patch, couleur in zip(bp['boxes'], couleurs):
            patch.set_facecolor(couleur)
            patch.set_alpha(0.7)
        ax.set_title(col, fontweight='bold')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis='x', rotation=20)
        if ylabel:
            ax.set_ylabel(ylabel, fontsize=10)

    fig.suptitle(titre_g, fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.show()

# ── Aire empilée ──────────────────────────────────────────────────────────────
def graphe_aire(datasets, selection):
    print()
    nom = selection[0]
    if len(selection) > 1:
        info("Aire empilée fonctionne sur un seul dataset — utilisation du premier.")
    df = datasets[nom]["df"]

    col_x  = choisir_colonne(df, "Colonne X (abscisse)")
    cols_y = choisir_colonne_multi(df, "Colonnes Y à empiler", filtre_num=True)
    if not cols_y:
        return

    titre_g, xlabel, ylabel = demander_titres(f"Aire empilée — {nom}", col_x or "", "Valeur cumulée")

    fig, ax = plt.subplots(figsize=(10, 5))
    clrs = couleurs_datasets(len(cols_y))
    ax.stackplot(df[col_x], [df[c] for c in cols_y if c in df.columns],
                 labels=cols_y, colors=clrs, alpha=0.8)
    appliquer_style(ax, titre_g, xlabel, ylabel)
    ax.legend(fontsize=8, loc='upper left')
    plt.tight_layout()
    plt.show()

# ── Heatmap corrélation ───────────────────────────────────────────────────────
def graphe_heatmap(datasets, selection):
    nom = selection[0]
    if len(selection) > 1:
        info("Heatmap sur un seul dataset — utilisation du premier.")
    df = datasets[nom]["df"]
    cols_num = df.select_dtypes(include='number').columns.tolist()
    if len(cols_num) < 2:
        erreur("Pas assez de colonnes numériques pour une heatmap.")
        return

    titre_g, _, _ = demander_titres(f"Matrice de corrélation — {nom}")

    corr = df[cols_num].corr()
    fig, ax = plt.subplots(figsize=(max(6, len(cols_num)), max(5, len(cols_num) - 1)))
    im = ax.imshow(corr, cmap='RdBu_r', vmin=-1, vmax=1, aspect='auto')
    plt.colorbar(im, ax=ax, label="Corrélation")
    ax.set_xticks(range(len(cols_num)))
    ax.set_yticks(range(len(cols_num)))
    ax.set_xticklabels(cols_num, rotation=45, ha='right', fontsize=8)
    ax.set_yticklabels(cols_num, fontsize=8)
    for i in range(len(cols_num)):
        for j in range(len(cols_num)):
            val = corr.iloc[i, j]
            ax.text(j, i, f"{val:.2f}", ha='center', va='center', fontsize=7,
                    color='white' if abs(val) > 0.5 else 'black')
    ax.set_title(titre_g, fontsize=12, fontweight='bold', pad=12)
    plt.tight_layout()
    plt.show()

# ─── Routeur graphes ────────────────────────────────────────────────────────
def menu_graphe(datasets):
    """Menu principal pour tracer un graphe."""
    if not datasets:
        erreur("Aucun fichier chargé !")
        return

    titre("TRACER UN GRAPHE")
    selection = choisir_datasets(datasets)
    type_labels = list(TYPES_GRAPHE.keys())
    idx_type = menu_numerote("Type de graphe", type_labels)
    type_graphe = TYPES_GRAPHE[type_labels[idx_type]]

    dispatch = {
        "ligne":   graphe_ligne,
        "scatter": graphe_scatter,
        "barres":  graphe_barres,
        "histo":   graphe_histo,
        "boxplot": graphe_boxplot,
        "aire":    graphe_aire,
        "heatmap": graphe_heatmap,
    }
    dispatch[type_graphe](datasets, selection)

# ─── Exportation ──────────────────────
def _formats_export():
    fmts = ["CSV", "TSV (tabulation)", "TXT (espace)", "JSON", "Parquet"]
    if EXCEL_OK:
        fmts.insert(4, "Excel (.xlsx)")
    return fmts

def exporter_merge(datasets):
    """Fusionne et exporte les datasets sélectionnés."""
    titre("EXPORTER / FUSIONNER")
    selection = choisir_datasets(datasets)

    dfs = [datasets[n]["df"].assign(_source=n) for n in selection]
    merged = pd.concat(dfs, ignore_index=True)

    # ── Sélection des colonnes à exporter ─────────────────────────────────────
    toutes_cols = merged.columns.tolist()
    idxs_cols = menu_numerote(
        f"Colonnes à exporter (ex: 1 3 5 ou 'all') — {len(toutes_cols)} colonnes :",
        toutes_cols,
        allow_multiple=True,
    )
    cols_export = [toutes_cols[i] for i in idxs_cols]
    if not cols_export:
        warn("Aucune colonne sélectionnée, export annulé.")
        return
    merged = merged[cols_export]

    # ── Format ────────────────────────────────────────────────────────────────
    fmts = _formats_export()
    idx_fmt = menu_numerote("Format d'export :", fmts)
    fmt = fmts[idx_fmt]

    ext_defaut = {
        "CSV":              ".csv",
        "TSV (tabulation)": ".tsv",
        "TXT (espace)":     ".txt",
        "JSON":             ".json",
        "Excel (.xlsx)":    ".xlsx",
        "Parquet":          ".parquet",
    }
    chemin_out = input_prompt("Nom du fichier de sortie", f"export_merge{ext_defaut.get(fmt, '.csv')}")

    try:
        if fmt == "CSV":
            sep = choisir_separateur("Séparateur CSV :", avec_auto=False)
            merged.to_csv(chemin_out, index=False, sep=sep, encoding="utf-8-sig")

        elif fmt == "TSV (tabulation)":
            merged.to_csv(chemin_out, index=False, sep="\t", encoding="utf-8-sig")

        elif fmt == "TXT (espace)":
            merged.to_csv(chemin_out, index=False, sep=" ", encoding="utf-8-sig")

        elif fmt == "JSON":
            orients = [
                "records  — liste d'objets  [ {col: val, …}, … ]",
                "table    — avec schéma de types",
                "index    — dictionnaire indexé",
            ]
            orient_map = {
                "records  — liste d'objets  [ {col: val, …}, … ]": "records",
                "table    — avec schéma de types":                  "table",
                "index    — dictionnaire indexé":                   "index",
            }
            idx_orient = menu_numerote("Format JSON :", orients)
            orient = orient_map[orients[idx_orient]]
            merged.to_json(chemin_out, orient=orient, force_ascii=False, indent=2)

        elif fmt == "Excel (.xlsx)":
            sheet = input_prompt("Nom de la feuille", "Données")
            merged.to_excel(chemin_out, index=False, sheet_name=sheet)

        elif fmt == "Parquet":
            merged.to_parquet(chemin_out, index=False)

        ok(f"Exporté : {chemin_out}  ({len(merged)} lignes, {len(merged.columns)} colonnes)")

    except Exception as e:
        erreur(f"Erreur lors de l'export : {e}")

# ─── Division par groupes de colonnes ──────────────────────────────────────────
def diviser_par_groupes(datasets):
    """
    Détecte les groupes de lignes partageant le même ensemble de colonnes
    non-nulles et les enregistre comme datasets distincts.
    """
    titre("DIVISER PAR GROUPES DE COLONNES")
    noms = list(datasets.keys())
    idx = menu_numerote("Quel dataset à diviser ?", noms)
    nom = noms[idx]
    df = datasets[nom]["df"]

    signatures = df.apply(
        lambda row: frozenset(df.columns[row.notna()].tolist()), axis=1
    )
    groupes = signatures.unique()

    if len(groupes) == 1:
        warn("Toutes les lignes ont le même ensemble de colonnes — rien à diviser.")
        return

    info(f"{len(groupes)} groupe(s) détecté(s) :")
    groupes_tries = sorted(groupes, key=lambda g: -signatures.value_counts()[g])
    for i, g in enumerate(groupes_tries):
        n_lignes = (signatures == g).sum()
        cols = ", ".join(sorted(g))
        print(f"    {C.CYAN}{i+1}{C.RESET}. ({n_lignes} lignes)  {C.DIM}{cols}{C.RESET}")
    print()

    confirmer = input_prompt(f"Créer {len(groupes_tries)} sous-datasets ? (o/n)", "o")
    if confirmer.lower() not in ("o", "oui", "y", "yes"):
        return

    for i, sig in enumerate(groupes_tries):
        masque = signatures == sig
        sous_df = df[masque][sorted(sig)].reset_index(drop=True)
        cols_courtes = "_".join(c[:6] for c in sorted(sig))
        sous_nom_defaut = f"{nom}_g{i+1}_{cols_courtes}"[:40]
        sous_nom = input_prompt(f"Nom du sous-dataset {i+1}/{len(groupes_tries)}", sous_nom_defaut)
        if not sous_nom:
            sous_nom = sous_nom_defaut
        datasets[sous_nom] = {
            "df": sous_df,
            "chemin": f"(issu de {datasets[nom]['chemin']})",
            "sep": datasets[nom]["sep"],
        }
        ok(f"Créé : {C.BOLD}{sous_nom}{C.RESET}  ({len(sous_df)} lignes)")

# ─── Menu principal ─────────────────────────────────────────────────────────
MENU_PRINCIPAL = [
    "Tracer un graphe",
    "Aperçu des données",
    "Statistiques descriptives",
    "Ajouter un fichier CSV",
    "Diviser par groupes de colonnes",
    "Exporter / Fusionner",
    "Quitter",
]

def main():
    parser = argparse.ArgumentParser(description="Comparateur interactif de fichiers CSV")
    parser.add_argument("fichiers", nargs="*", help="Fichiers CSV à charger au démarrage")
    parser.add_argument(
        "--sep",
        help="Séparateur forcé au chargement (ex: , ; \\t |)",
        default=None,
    )
    args = parser.parse_args()

    print(f"\n{C.CYAN}{C.BOLD}")
    print("  ╔══════════════════════════════════════════╗")
    print("  ║        CSV COMPARATOR  v3.0              ║")
    print("  ║   Comparateur interactif de fichiers     ║")
    print("  ╚══════════════════════════════════════════╝")
    print(C.RESET)

    datasets = {}

    if args.fichiers:
        titre("CHARGEMENT DES FICHIERS")
        sep = args.sep.replace("\\t", "\t") if args.sep else None
        datasets = charger_fichiers(args.fichiers, sep=sep)
    else:
        info("Aucun fichier spécifié. Utilise le menu pour en ajouter.")
        info("Astuce : python comparecsv.py fichier1.csv fichier2.csv [--sep ,]")

    while True:
        titre("MENU PRINCIPAL")
        if datasets:
            info(f"Fichiers chargés : {C.BOLD}{', '.join(datasets.keys())}{C.RESET}")
        else:
            warn("Aucun fichier chargé")

        choix = menu_numerote("Que veux-tu faire ?", MENU_PRINCIPAL)

        if choix == 0:
            menu_graphe(datasets)
        elif choix == 1:
            if datasets: afficher_apercu(datasets)
            else: warn("Aucun fichier chargé.")
        elif choix == 2:
            if datasets: afficher_stats(datasets)
            else: warn("Aucun fichier chargé.")
        elif choix == 3:
            ajouter_fichier(datasets)
        elif choix == 4:
            if datasets: diviser_par_groupes(datasets)
            else: warn("Aucun fichier chargé.")
        elif choix == 5:
            if datasets: exporter_merge(datasets)
            else: warn("Aucun fichier chargé.")
        elif choix == 6:
            print(f"\n  {C.GREEN}Au revoir !{C.RESET}\n")
            break

if __name__ == "__main__":
    main()