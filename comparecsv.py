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

def charger_fichiers(chemins):
    """Charge plusieurs fichiers CSV."""
    datasets = {}
    for chemin in chemins:
        p = Path(chemin)
        if not p.exists():
            warn(f"Fichier introuvable : {chemin}")
            continue
        df, sep = charger_csv(chemin)
        if df is not None:
            datasets[p.stem] = {"df": df, "chemin": chemin, "sep": sep}
            ok(f"Chargé : {C.BOLD}{p.name}{C.RESET}  ({len(df)} lignes, {len(df.columns)} colonnes, sep='{sep}')")
    return datasets

def ajouter_fichier(datasets):
    """Ajoute interactivement un fichier."""
    chemin = input_prompt("Chemin du fichier CSV").strip('"').strip("'")
    p = Path(chemin)
    if not p.exists():
        erreur(f"Fichier introuvable : {chemin}")
        return
    df, sep = charger_csv(chemin)
    if df is not None:
        datasets[p.stem] = {"df": df, "chemin": chemin, "sep": sep}
        ok(f"Ajouté : {C.BOLD}{p.name}{C.RESET}")

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
def choisir_colonne(df, message, filtre_num=False):
    """Fait choisir une colonne parmi celles du dataframe."""
    if filtre_num:
        cols = df.select_dtypes(include='number').columns.tolist()
        if not cols:
            erreur("Aucune colonne numérique disponible.")
            return None
    else:
        cols = df.columns.tolist()
    
    idx = menu_numerote(message, cols)
    return cols[idx]

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
    # Référence sur premier dataset pour colonnes X
    df_ref = datasets[selection[0]]["df"]
    col_x = choisir_colonne(df_ref, "Colonne X (abscisse)")
    if col_x is None: return

    # Colonnes Y : peut en choisir plusieurs
    cols_y_dispo = df_ref.select_dtypes(include='number').columns.tolist()
    if not cols_y_dispo:
        erreur("Aucune colonne numérique pour Y.")
        return
    idxs_y = menu_numerote("Colonnes Y (peut en choisir plusieurs, ex: 1 3)", cols_y_dispo, allow_multiple=True)
    cols_y = [cols_y_dispo[i] for i in idxs_y]

    fig, ax = plt.subplots(figsize=(10, 5))
    couleurs = couleurs_datasets(len(selection) * len(cols_y))
    c_idx = 0
    for nom in selection:
        df = datasets[nom]["df"]
        for col_y in cols_y:
            if col_y not in df.columns: continue
            label = f"{nom} — {col_y}" if len(selection) > 1 else col_y
            ax.plot(df[col_x], df[col_y], label=label, color=couleurs[c_idx], linewidth=1.8, marker='o', markersize=3)
            c_idx += 1

    appliquer_style(ax, "Graphe Ligne", col_x, "Valeur")
    ax.legend(fontsize=8)
    plt.tight_layout()
    plt.show()

# ── Scatter ──────────────────────────────────────────────────────────────────
def graphe_scatter(datasets, selection):
    print()
    df_ref = datasets[selection[0]]["df"]
    col_x = choisir_colonne(df_ref, "Colonne X", filtre_num=True)
    col_y = choisir_colonne(df_ref, "Colonne Y", filtre_num=True)
    if col_x is None or col_y is None: return

    fig, ax = plt.subplots(figsize=(8, 6))
    couleurs = couleurs_datasets(len(selection))
    for i, nom in enumerate(selection):
        df = datasets[nom]["df"]
        if col_x not in df.columns or col_y not in df.columns:
            warn(f"Colonnes manquantes dans {nom}")
            continue
        ax.scatter(df[col_x], df[col_y], label=nom, color=couleurs[i], alpha=0.65, s=30, edgecolors='none')

    appliquer_style(ax, f"Scatter : {col_x} vs {col_y}", col_x, col_y)
    if len(selection) > 1: ax.legend(fontsize=8)
    plt.tight_layout()
    plt.show()

# ── Barres ───────────────────────────────────────────────────────────────────
def graphe_barres(datasets, selection):
    print()
    df_ref = datasets[selection[0]]["df"]
    col_x = choisir_colonne(df_ref, "Colonne catégories (X)")
    col_y = choisir_colonne(df_ref, "Colonne valeurs (Y)", filtre_num=True)
    if col_x is None or col_y is None: return

    fig, ax = plt.subplots(figsize=(10, 5))
    n = len(selection)
    couleurs = couleurs_datasets(n)
    width = 0.8 / max(n, 1)

    for i, nom in enumerate(selection):
        df = datasets[nom]["df"]
        if col_x not in df.columns or col_y not in df.columns: continue
        grouped = df.groupby(col_x)[col_y].mean().reset_index()
        x = np.arange(len(grouped))
        offset = (i - n/2 + 0.5) * width
        ax.bar(x + offset, grouped[col_y], width=width * 0.9, label=nom, color=couleurs[i], alpha=0.85)
        ax.set_xticks(x)
        ax.set_xticklabels(grouped[col_x], rotation=45, ha='right', fontsize=8)

    appliquer_style(ax, f"Barres : {col_y} par {col_x}", col_x, col_y)
    if len(selection) > 1: ax.legend(fontsize=8)
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

    fig, ax = plt.subplots(figsize=(9, 5))
    couleurs = couleurs_datasets(len(selection))
    for i, nom in enumerate(selection):
        df = datasets[nom]["df"]
        if col not in df.columns: continue
        ax.hist(df[col].dropna(), bins=bins, label=nom, color=couleurs[i], alpha=0.6, edgecolor='white', linewidth=0.5)

    appliquer_style(ax, f"Distribution : {col}", col, "Fréquence")
    if len(selection) > 1: ax.legend(fontsize=8)
    plt.tight_layout()
    plt.show()

# ── Boxplot ───────────────────────────────────────────────────────────────────
def graphe_boxplot(datasets, selection):
    print()
    df_ref = datasets[selection[0]]["df"]
    cols_num = df_ref.select_dtypes(include='number').columns.tolist()
    idxs = menu_numerote("Colonnes à comparer (ex: 1 2 3 ou 'all')", cols_num, allow_multiple=True)
    cols = [cols_num[i] for i in idxs]

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

    fig.suptitle("Boîtes à moustaches", fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.show()

# ── Aire empilée ──────────────────────────────────────────────────────────────
def graphe_aire(datasets, selection):
    print()
    df_ref = datasets[selection[0]]["df"]
    col_x = choisir_colonne(df_ref, "Colonne X (abscisse)")
    cols_num = df_ref.select_dtypes(include='number').columns.tolist()
    idxs_y = menu_numerote("Colonnes Y à empiler (ex: 1 2 ou 'all')", cols_num, allow_multiple=True)
    cols_y = [cols_num[i] for i in idxs_y]

    nom = selection[0]
    if len(selection) > 1:
        info("Aire empilée fonctionne sur un seul dataset — utilisation du premier.")
    df = datasets[nom]["df"]

    fig, ax = plt.subplots(figsize=(10, 5))
    couleurs = couleurs_datasets(len(cols_y))
    ax.stackplot(df[col_x], [df[c] for c in cols_y if c in df.columns], labels=cols_y, colors=couleurs, alpha=0.8)
    appliquer_style(ax, f"Aire empilée — {nom}", col_x, "Valeur cumulée")
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
    ax.set_title(f"Matrice de corrélation — {nom}", fontsize=12, fontweight='bold', pad=12)
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

# ─── Exportation ────────────────────────────────────────────────────────────
def exporter_merge(datasets):
    """Fusionne et exporte les datasets sélectionnés."""
    titre("EXPORTER / FUSIONNER")
    selection = choisir_datasets(datasets)
    dfs = [datasets[n]["df"].assign(_source=n) for n in selection]
    merged = pd.concat(dfs, ignore_index=True)
    chemin_out = input_prompt("Nom du fichier de sortie", "export_merge.csv")
    merged.to_csv(chemin_out, index=False)
    ok(f"Exporté : {chemin_out}  ({len(merged)} lignes)")

# ─── Menu principal ─────────────────────────────────────────────────────────
MENU_PRINCIPAL = [
    "Tracer un graphe",
    "Aperçu des données",
    "Statistiques descriptives",
    "Ajouter un fichier CSV",
    "Exporter / Fusionner",
    "Quitter",
]

def main():
    parser = argparse.ArgumentParser(description="Comparateur interactif de fichiers CSV")
    parser.add_argument("fichiers", nargs="*", help="Fichiers CSV à charger au démarrage")
    args = parser.parse_args()

    print(f"\n{C.CYAN}{C.BOLD}")
    print("  ╔══════════════════════════════════════════╗")
    print("  ║        CSV COMPARATOR  v1.0              ║")
    print("  ║   Comparateur interactif de fichiers     ║")
    print("  ╚══════════════════════════════════════════╝")
    print(C.RESET)

    datasets = {}

    if args.fichiers:
        titre("CHARGEMENT DES FICHIERS")
        datasets = charger_fichiers(args.fichiers)
    else:
        info("Aucun fichier spécifié. Utilise le menu pour en ajouter.")
        info("Astuce : python csv_comparator.py fichier1.csv fichier2.csv")

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
            if datasets: exporter_merge(datasets)
            else: warn("Aucun fichier chargé.")
        elif choix == 5:
            print(f"\n  {C.GREEN}Au revoir !{C.RESET}\n")
            break

if __name__ == "__main__":
    main()
