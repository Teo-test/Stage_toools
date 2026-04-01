#!/usr/bin/env python3
"""
PPTX Chart Extractor — Extraction de graphes PowerPoint via XML brut
Utilise uniquement zipfile + xml.etree.ElementTree (stdlib) + pandas/matplotlib.

Usage: python pptx_chart_extractor.py [fichier.pptx] [-o dossier_sortie]
"""

import sys
import os
import re
import argparse
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import numpy as np
except ImportError as e:
    print(f"[ERREUR] Dépendance manquante : {e}")
    print("Installe : pip install pandas matplotlib numpy")
    sys.exit(1)

# ─── Namespaces XML Office Open ──────────────────────────────────────────────
NS = {
    'c':  'http://schemas.openxmlformats.org/drawingml/2006/chart',
    'a':  'http://schemas.openxmlformats.org/drawingml/2006/main',
    'p':  'http://schemas.openxmlformats.org/presentationml/2006/main',
    'r':  'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
    'rel':'http://schemas.openxmlformats.org/package/2006/relationships',
}

# Balises de graphes reconnus → famille lisible
CHART_TAGS = {
    'lineChart':     'ligne',
    'line3DChart':   'ligne',
    'barChart':      'barres',
    'bar3DChart':    'barres',
    'scatterChart':  'scatter',
    'bubbleChart':   'scatter',
    'pieChart':      'pie',
    'pie3DChart':    'pie',
    'doughnutChart': 'pie',
    'areaChart':     'aire',
    'area3DChart':   'aire',
    'radarChart':    'radar',
    'stockChart':    'ligne',
    'surfaceChart':  'surface',
    'surface3DChart':'surface',
}

# ─── Couleurs terminal ────────────────────────────────────────────────────────
class C:
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    CYAN    = "\033[96m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    RED     = "\033[91m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    RESET   = "\033[0m"

def titre(texte):
    largeur = 60
    print(f"\n{C.CYAN}{C.BOLD}{'─' * largeur}{C.RESET}")
    print(f"{C.CYAN}{C.BOLD}  {texte}{C.RESET}")
    print(f"{C.CYAN}{C.BOLD}{'─' * largeur}{C.RESET}")

def info(texte):   print(f"  {C.BLUE}ℹ {C.RESET}{texte}")
def ok(texte):     print(f"  {C.GREEN}✔ {C.RESET}{texte}")
def warn(texte):   print(f"  {C.YELLOW}⚠ {C.RESET}{texte}")
def erreur(texte): print(f"  {C.RED}✘ {C.RESET}{texte}")

def menu_numerote(titre_menu, options, allow_multiple=False):
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
    if defaut:
        rep = input(f"  {C.BOLD}{texte}{C.RESET} [{C.DIM}{defaut}{C.RESET}] : ").strip()
        return rep if rep else defaut
    return input(f"  {C.BOLD}{texte}{C.RESET} : ").strip()

def slugify(texte):
    texte = re.sub(r'[^\w\s-]', '', str(texte).lower())
    return re.sub(r'[\s-]+', '_', texte).strip('_') or "graphe"

def couleurs(n):
    return cm.tab10(np.linspace(0, 0.9, max(n, 1)))

# ─── Lecture XML bas niveau ───────────────────────────────────────────────────

def lire_pts(ref_el):
    """
    Lit les points d'une référence XML (strRef ou numRef).
    Retourne un dict {idx: valeur} — gère les séries creuses (sparse).
    """
    if ref_el is None:
        return {}
    pts = {}
    for pt in ref_el.findall('.//c:pt', NS):
        idx  = pt.get('idx')
        v_el = pt.find('c:v', NS)
        if idx is not None and v_el is not None and v_el.text is not None:
            pts[int(idx)] = v_el.text
    return pts

def lire_ptCount(ref_el):
    """Lit c:ptCount pour connaître la taille totale déclarée."""
    pc = ref_el.find('.//c:ptCount', NS) if ref_el is not None else None
    return int(pc.get('val')) if pc is not None else None

def pts_vers_liste(pts_dict, ptCount=None):
    """
    Convertit {idx: val} en liste ordonnée.
    Remplit les trous avec None pour conserver l'alignement avec les autres colonnes.
    """
    if not pts_dict:
        return []
    n = ptCount if ptCount is not None else max(pts_dict.keys()) + 1
    return [pts_dict.get(i) for i in range(n)]

def lire_ref(parent_el):
    """
    Lit strRef, numRef ou valeurs inline dans un élément
    (c:cat, c:val, c:xVal, c:yVal…).
    Retourne (liste_valeurs, type_ref).
    """
    if parent_el is None:
        return [], None

    for ref_tag, type_ref in [('c:strRef', 'str'), ('c:numRef', 'num'), ('c:multiLvlStrRef', 'str')]:
        ref = parent_el.find(ref_tag, NS)
        if ref is not None:
            pts      = lire_pts(ref)
            ptCount  = lire_ptCount(ref)
            return pts_vers_liste(pts, ptCount), type_ref

    # Fallback : valeurs littérales directement dans l'élément
    vals = [v.text for v in parent_el.findall('.//c:v', NS)]
    return vals, 'inline' if vals else None

def lire_nom_serie(ser_el):
    """Extrait le nom d'une série depuis c:tx."""
    tx = ser_el.find('c:tx', NS)
    if tx is None:
        return None
    v = tx.find('.//c:v', NS)
    if v is not None and v.text:
        return v.text.strip()
    t = tx.find('.//a:t', NS)
    if t is not None and t.text:
        return t.text.strip()
    return None

def lire_titre_chart(chart_tree):
    """Extrait le titre du graphe depuis c:title."""
    title_el = chart_tree.find('.//c:title', NS)
    if title_el is None:
        return ""
    v = title_el.find('.//c:v', NS)
    if v is not None and v.text:
        return v.text.strip()
    parties = [t.text for t in title_el.findall('.//a:t', NS) if t.text]
    return "".join(parties).strip()

def lire_titre_slide(slide_tree):
    """Extrait le titre d'une slide (placeholder type title ou ctrTitle)."""
    for sp in slide_tree.findall('.//p:sp', NS):
        ph = sp.find('.//p:ph', NS)
        if ph is not None and ph.get('type') in ('title', 'ctrTitle'):
            parties = [t.text for t in sp.findall('.//a:t', NS) if t.text]
            return "".join(parties).strip()
    return ""

def detecter_famille(chart_tree):
    """Détecte le type de graphe depuis les balises enfants de c:plotArea."""
    pa = chart_tree.find('.//c:plotArea', NS)
    if pa is None:
        return 'inconnu', 'inconnu'
    for child in pa:
        local = child.tag.split('}')[-1]
        if local in CHART_TAGS:
            return CHART_TAGS[local], local
    return 'inconnu', 'inconnu'

def to_float(val):
    """Convertit en float si possible, sinon retourne la valeur brute."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return val

# ─── Extraction par type de graphe ───────────────────────────────────────────

def extraire_serie_classique(ser_el):
    """
    Extrait (nom, catégories, valeurs) d'une série standard.
    Catégories : strRef > numRef > inline > indices numériques.
    """
    nom  = lire_nom_serie(ser_el) or "Série"
    cats, _ = lire_ref(ser_el.find('c:cat', NS))
    vals, _ = lire_ref(ser_el.find('c:val', NS))
    vals = [to_float(v) for v in vals]
    return nom, cats, vals

def extraire_serie_scatter(ser_el):
    """Extrait (nom, x_vals, y_vals) d'une série scatter/bubble."""
    nom   = lire_nom_serie(ser_el) or "Série"
    xs, _ = lire_ref(ser_el.find('c:xVal', NS))
    ys, _ = lire_ref(ser_el.find('c:yVal', NS))
    xs = [to_float(v) for v in xs]
    ys = [to_float(v) for v in ys]
    return nom, xs, ys

def construire_df_classique(series_data):
    """
    Construit un DataFrame avec une paire de colonnes (X, Y) par série.
    Format : NomSérie_X | NomSérie_Y | NomSérie2_X | NomSérie2_Y | …

    Chaque série garde ses propres catégories en X — pas d'union forcée.
    Les séries de longueurs différentes sont complétées par None.
    """
    if not series_data:
        return pd.DataFrame()

    df_dict = {}
    max_len = 0

    for nom, cats, vals in series_data:
        # Catégories X : strRef/numRef si disponibles, sinon indices
        if cats:
            xs = [str(c) if c is not None else "" for c in cats]
        else:
            xs = [str(i) for i in range(len(vals))]

        max_len = max(max_len, len(xs), len(vals))
        df_dict[f"{nom}_X"] = xs
        df_dict[f"{nom}_Y"] = vals

    # Homogénéiser les longueurs
    for col in df_dict:
        diff = max_len - len(df_dict[col])
        if diff > 0:
            df_dict[col] = df_dict[col] + [None] * diff

    return pd.DataFrame(df_dict)

def construire_df_scatter(series_data):
    """
    DataFrame scatter avec paires X/Y par série.
    Format : NomSérie_X | NomSérie_Y | …
    """
    if not series_data:
        return pd.DataFrame()

    df_dict = {}
    max_len = 0

    for nom, xs, ys in series_data:
        max_len = max(max_len, len(xs), len(ys))
        df_dict[f"{nom}_X"] = list(xs)
        df_dict[f"{nom}_Y"] = list(ys)

    for col in df_dict:
        diff = max_len - len(df_dict[col])
        if diff > 0:
            df_dict[col] = df_dict[col] + [None] * diff

    return pd.DataFrame(df_dict)

def extraire_chart(chart_tree):
    """
    Extrait toutes les données d'un arbre XML de graphe.
    Retourne (DataFrame, famille, tag_xml).
    """
    famille, tag = detecter_famille(chart_tree)
    pa = chart_tree.find('.//c:plotArea', NS)
    if pa is None:
        return pd.DataFrame(), famille, tag

    if famille == 'scatter':
        series_data = [extraire_serie_scatter(s) for s in pa.findall('.//c:ser', NS)]
        return construire_df_scatter(series_data), famille, tag

    series_data = [extraire_serie_classique(s) for s in pa.findall('.//c:ser', NS)]
    return construire_df_classique(series_data), famille, tag

# ─── Analyse du fichier PPTX ─────────────────────────────────────────────────

def charger_rels(zf, rels_path):
    """Retourne {rId: target} depuis un fichier .rels."""
    if rels_path not in zf.namelist():
        return {}
    tree = ET.fromstring(zf.read(rels_path))
    return {rel.get('Id'): rel.get('Target') for rel in tree}

def resoudre_chart_path(slide_path, target):
    """
    Résout le chemin d'un chart référencé depuis une slide.
    Exemple : 'ppt/slides/slide1.xml' + '../charts/chart1.xml'
              → 'ppt/charts/chart1.xml'
    """
    base = Path(slide_path).parent
    resolved = (base / target).resolve()
    # Path.resolve() donne un chemin absolu — on le rend relatif à la racine ZIP
    parts = resolved.parts
    try:
        # Retrouver 'ppt' dans le chemin et reconstruire depuis là
        idx = parts.index('ppt')
        return '/'.join(parts[idx:])
    except ValueError:
        return str(resolved).lstrip('/')

def analyser_pptx(chemin_pptx):
    """
    Parcourt toutes les slides, détecte et extrait chaque graphe.
    Retourne une liste de dicts.
    """
    graphes    = []
    graphe_idx = 0

    with zipfile.ZipFile(chemin_pptx, 'r') as zf:
        noms = set(zf.namelist())

        slides = sorted(
            [n for n in noms if re.match(r'ppt/slides/slide\d+\.xml$', n)],
            key=lambda x: int(re.search(r'\d+', x).group())
        )

        for slide_path in slides:
            slide_num  = int(re.search(r'\d+', slide_path).group())
            slide_tree = ET.fromstring(zf.read(slide_path))
            titre_slide = lire_titre_slide(slide_tree)

            rels_path = re.sub(
                r'slides/(slide\d+\.xml)$',
                r'slides/_rels/\1.rels',
                slide_path
            )
            rels = charger_rels(zf, rels_path)

            for rId, target in rels.items():
                if 'chart' not in target.lower():
                    continue

                chart_path = resoudre_chart_path(slide_path, target)
                if chart_path not in noms:
                    # Fallback : juste le nom du fichier dans ppt/charts/
                    chart_path = 'ppt/charts/' + Path(target).name
                if chart_path not in noms:
                    warn(f"Chart introuvable : {target}")
                    continue

                chart_tree  = ET.fromstring(zf.read(chart_path))
                titre_chart = lire_titre_chart(chart_tree)
                famille, tag = detecter_famille(chart_tree)
                df, _, _    = extraire_chart(chart_tree)

                graphe_idx += 1
                graphes.append({
                    "idx":        graphe_idx,
                    "slide":      slide_num,
                    "titre":      titre_chart or titre_slide or f"Graphe {graphe_idx}",
                    "famille":    famille,
                    "tag_xml":    tag,
                    "df":         df,
                    "chart_path": chart_path,
                })

    return graphes

# ─── Affichage ────────────────────────────────────────────────────────────────

def afficher_resume(graphes):
    titre("GRAPHES TROUVÉS")
    if not graphes:
        warn("Aucun graphe détecté dans ce fichier.")
        return
    print(f"  {'N°':>3}  {'Slide':>5}  {'Type':<10}  {'Lignes':>6}  {'Cols':>4}  Titre")
    print(f"  {'─'*3}  {'─'*5}  {'─'*10}  {'─'*6}  {'─'*4}  {'─'*32}")
    for g in graphes:
        vide  = f" {C.YELLOW}[vide]{C.RESET}" if g["df"].empty else ""
        ncols = len(g["df"].columns) if not g["df"].empty else 0
        print(f"  {C.CYAN}{g['idx']:>3}{C.RESET}  "
              f"  {g['slide']:>3}  "
              f"  {C.BOLD}{g['famille']:<10}{C.RESET}  "
              f"  {len(g['df']):>5}  "
              f"  {ncols:>3}  "
              f"  {g['titre']}{vide}")

def afficher_detail(graphes):
    titre("DÉTAIL D'UN GRAPHE")
    idx = menu_numerote("Quel graphe ?",
                        [f"[Slide {g['slide']}] {g['famille']:<8} — {g['titre']}" for g in graphes])
    g = graphes[idx]
    print(f"\n  {C.BOLD}{C.MAGENTA}{g['titre']}{C.RESET}")
    print(f"  Slide      : {g['slide']}")
    print(f"  Balise XML : {g['tag_xml']}")
    print(f"  Famille    : {g['famille']}")
    if not g["df"].empty:
        df = g["df"]
        cols_x = [c for c in df.columns if str(c).endswith('_X')]
        series_noms = [c[:-2] for c in cols_x]
        print(f"  Lignes     : {len(df)}")
        print(f"  Séries     : {', '.join(series_noms)}")
        print(f"  Colonnes   : {', '.join(str(c) for c in df.columns)}")
        print(f"\n{df.head(10).to_string(index=False)}\n")
    else:
        warn("  Aucune donnée extractible.")

# ─── Export CSV ───────────────────────────────────────────────────────────────

def exporter_csv(graphes, selection, dossier_out):
    titre("EXPORT CSV")
    dossier = Path(dossier_out)
    dossier.mkdir(parents=True, exist_ok=True)
    for idx in selection:
        g = graphes[idx]
        if g["df"].empty:
            warn(f"Graphe {g['idx']} ({g['titre']}) : données vides, ignoré.")
            continue
        nom    = f"graphe_{g['idx']:02d}_{slugify(g['titre'])}.csv"
        chemin = dossier / nom
        g["df"].to_csv(chemin, index=False, encoding="utf-8-sig")
        ok(f"Exporté : {C.BOLD}{chemin}{C.RESET}  "
           f"({len(g['df'])} lignes, {len(g['df'].columns)} colonnes)")

# ─── Plots ────────────────────────────────────────────────────────────────────

def style_ax(ax, titre_graphe, xlabel="", ylabel=""):
    ax.set_title(titre_graphe, fontsize=12, fontweight='bold', pad=10)
    if xlabel: ax.set_xlabel(xlabel, fontsize=9)
    if ylabel: ax.set_ylabel(ylabel, fontsize=9)
    ax.grid(True, alpha=0.25, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

def plot_graphe(g, sauvegarder=False, dossier_out="."):
    df      = g["df"]
    famille = g["famille"]
    titre_g = g["titre"]
    fig, ax = plt.subplots(figsize=(9, 5))

    if df.empty:
        ax.text(0.5, 0.5, "Données non disponibles", ha='center', va='center',
                transform=ax.transAxes, fontsize=14, color='gray')
        ax.set_title(titre_g, fontsize=12, fontweight='bold')
        plt.tight_layout()
        _finaliser(fig, g, sauvegarder, dossier_out)
        return

    # Extraire les paires (NomSérie_X, NomSérie_Y) depuis les colonnes
    cols_x = [c for c in df.columns if str(c).endswith('_X')]
    series_noms = [c[:-2] for c in cols_x]  # noms sans le suffixe _X / _Y
    clrs = couleurs(max(len(series_noms), 1))

    # ── Scatter ──────────────────────────────────────────────────────────────
    if famille == 'scatter':
        for i, nom in enumerate(series_noms):
            xs = pd.to_numeric(df[f"{nom}_X"], errors='coerce')
            ys = pd.to_numeric(df[f"{nom}_Y"], errors='coerce')
            ax.scatter(xs, ys, label=nom, color=clrs[i],
                       alpha=0.75, s=45, edgecolors='none')
        style_ax(ax, titre_g, "X", "Y")
        if len(series_noms) > 1:
            ax.legend(fontsize=8)

    # ── Pie / Donut ───────────────────────────────────────────────────────────
    elif famille == 'pie':
        # Pour le pie : X = labels, Y = valeurs — on prend la première série
        if series_noms:
            nom      = series_noms[0]
            labs     = df[f"{nom}_X"].dropna()
            vals_num = pd.to_numeric(df[f"{nom}_Y"], errors='coerce').dropna()
            idx      = vals_num.index
            ax.pie(vals_num, labels=labs.iloc[idx] if len(labs) > max(idx) else labs,
                   autopct='%1.1f%%', startangle=90,
                   colors=couleurs(len(vals_num)), pctdistance=0.82,
                   wedgeprops=dict(linewidth=0.6, edgecolor='white'))
            ax.set_title(titre_g, fontsize=12, fontweight='bold', pad=10)

    # ── Barres ────────────────────────────────────────────────────────────────
    elif famille == 'barres':
        # Référence des labels X depuis la première série
        x_labels = df[f"{series_noms[0]}_X"].fillna("").tolist() if series_noms else []
        x        = np.arange(len(df))
        n        = len(series_noms)
        w        = 0.8 / max(n, 1)
        for i, nom in enumerate(series_noms):
            offset = (i - n / 2 + 0.5) * w
            vals   = pd.to_numeric(df[f"{nom}_Y"], errors='coerce')
            ax.bar(x + offset, vals, width=w * 0.92,
                   label=nom, color=clrs[i], alpha=0.85)
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, rotation=30, ha='right', fontsize=8)
        style_ax(ax, titre_g, "", "Valeur")
        if n > 1: ax.legend(fontsize=8)

    # ── Ligne / Aire / Radar / Surface ────────────────────────────────────────
    else:
        # Labels X depuis la première série (partagés si identiques)
        x_labels = df[f"{series_noms[0]}_X"].fillna("").tolist() if series_noms else []
        x        = np.arange(len(df))
        for i, nom in enumerate(series_noms):
            vals = pd.to_numeric(df[f"{nom}_Y"], errors='coerce')
            if famille == 'aire':
                ax.fill_between(x, vals, alpha=0.35, color=clrs[i])
            ax.plot(x, vals, label=nom, color=clrs[i],
                    linewidth=1.8, marker='o', markersize=3)
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, rotation=30, ha='right', fontsize=8)
        style_ax(ax, titre_g, "", "Valeur")
        if len(series_noms) > 1: ax.legend(fontsize=8)

    plt.tight_layout()
    _finaliser(fig, g, sauvegarder, dossier_out)

def _finaliser(fig, g, sauvegarder, dossier_out):
    if sauvegarder:
        dossier = Path(dossier_out)
        dossier.mkdir(parents=True, exist_ok=True)
        nom    = f"graphe_{g['idx']:02d}_{slugify(g['titre'])}.png"
        chemin = dossier / nom
        fig.savefig(chemin, dpi=150, bbox_inches='tight')
        plt.close(fig)
        ok(f"Image sauvegardée : {C.BOLD}{chemin}{C.RESET}")
    else:
        plt.show()

# ─── Menus ────────────────────────────────────────────────────────────────────

def choisir_graphes(graphes, message="Quels graphes ?"):
    labels = [f"[Slide {g['slide']}] {g['famille']:<8} — {g['titre']}" for g in graphes]
    idxs   = menu_numerote(message, labels + ["Tous"], allow_multiple=True)
    if len(graphes) in idxs:
        return list(range(len(graphes)))
    return idxs

def menu_plot(graphes, dossier_out):
    selection = choisir_graphes(graphes, "Afficher quels graphes ?")
    mode_idx  = menu_numerote("Mode d'affichage",
                              ["Afficher à l'écran", "Sauvegarder en PNG", "Les deux"])
    for idx in selection:
        g = graphes[idx]
        info(f"Graphe {g['idx']} : {g['titre']}  ({g['tag_xml']})")
        if g["df"].empty:
            warn("  Données vides — plot ignoré.")
            continue
        if mode_idx == 0:
            plot_graphe(g, sauvegarder=False)
        elif mode_idx == 1:
            plot_graphe(g, sauvegarder=True, dossier_out=dossier_out)
        else:
            plot_graphe(g, sauvegarder=True, dossier_out=dossier_out)
            plot_graphe(g, sauvegarder=False)

def menu_exporter(graphes, dossier_out):
    selection = choisir_graphes(graphes, "Exporter quels graphes en CSV ?")
    exporter_csv(graphes, selection, dossier_out)

def menu_tout_exporter(graphes, dossier_out):
    titre("EXPORT COMPLET (CSV + PNG)")
    non_vides = [i for i, g in enumerate(graphes) if not g["df"].empty]
    if not non_vides:
        warn("Aucun graphe avec des données à exporter.")
        return
    info(f"{len(non_vides)} graphe(s) → {C.BOLD}{dossier_out}/{C.RESET}")
    exporter_csv(graphes, non_vides, dossier_out)
    for idx in non_vides:
        plot_graphe(graphes[idx], sauvegarder=True, dossier_out=dossier_out)
    ok(f"Export terminé dans {C.BOLD}{dossier_out}/{C.RESET}")

# ─── Menu principal ───────────────────────────────────────────────────────────

MENU_PRINCIPAL = [
    "Afficher / sauvegarder des graphes",
    "Exporter des graphes en CSV",
    "Export complet (tous CSV + PNG)",
    "Voir le détail d'un graphe",
    "Résumé des graphes",
    "Charger un autre fichier PPTX",
    "Quitter",
]

def charger_pptx(chemin):
    p = Path(chemin)
    if not p.exists():
        erreur(f"Fichier introuvable : {chemin}")
        return None
    if not zipfile.is_zipfile(chemin):
        erreur(f"Fichier invalide (pas un PPTX/ZIP) : {chemin}")
        return None
    info(f"Analyse de {C.BOLD}{p.name}{C.RESET} …")
    graphes = analyser_pptx(chemin)
    ok(f"{len(graphes)} graphe(s) trouvé(s)")
    return graphes

def main():
    parser = argparse.ArgumentParser(
        description="Extracteur de graphes PPTX → CSV + plots  (stdlib XML uniquement)")
    parser.add_argument("fichier", nargs="?", help="Fichier .pptx à analyser")
    parser.add_argument("-o", "--output", default="pptx_export",
                        help="Dossier de sortie (défaut: pptx_export)")
    args = parser.parse_args()

    print(f"\n{C.CYAN}{C.BOLD}")
    print("  ╔══════════════════════════════════════════╗")
    print("  ║      PPTX CHART EXTRACTOR  v2.0          ║")
    print("  ║   zipfile + ElementTree — stdlib only    ║")
    print("  ╚══════════════════════════════════════════╝")
    print(C.RESET)

    graphes      = []
    chemin_actif = None
    dossier_out  = args.output

    if args.fichier:
        graphes = charger_pptx(args.fichier)
        if graphes is None:
            sys.exit(1)
        chemin_actif = args.fichier
        afficher_resume(graphes)
    else:
        info("Aucun fichier spécifié.")
        info("Astuce : python pptx_chart_extractor.py presentation.pptx")

    while True:
        titre("MENU PRINCIPAL")
        if chemin_actif:
            info(f"Fichier : {C.BOLD}{Path(chemin_actif).name}{C.RESET}  "
                 f"({len(graphes)} graphe(s))  →  sortie : {C.BOLD}{dossier_out}/{C.RESET}")
        else:
            warn("Aucun fichier chargé")

        choix = menu_numerote("Que veux-tu faire ?", MENU_PRINCIPAL)

        if not graphes and choix not in (5, 6):
            warn("Charge d'abord un fichier PPTX (option 6).")
            continue

        if   choix == 0: menu_plot(graphes, dossier_out)
        elif choix == 1: menu_exporter(graphes, dossier_out)
        elif choix == 2: menu_tout_exporter(graphes, dossier_out)
        elif choix == 3: afficher_detail(graphes)
        elif choix == 4: afficher_resume(graphes)
        elif choix == 5:
            chemin = input_prompt("Chemin du fichier PPTX").strip('"').strip("'")
            g = charger_pptx(chemin)
            if g is not None:
                graphes, chemin_actif = g, chemin
                afficher_resume(graphes)
        elif choix == 6:
            print(f"\n  {C.GREEN}Au revoir !{C.RESET}\n")
            break

if __name__ == "__main__":
    main()
