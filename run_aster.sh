#!/bin/bash
#===============================================================================
#  run_aster.sh — Soumission de calculs Code_Aster via Slurm
#===============================================================================
#
#  Usage :  bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]
#
#  ARCHITECTURE EN DEUX PHASES DANS UN SEUL FICHIER
#  ─────────────────────────────────────────────────
#    Phase 1 — noeud login (appel direct par l'utilisateur)
#      · Detecte les fichiers d'entree (.comm, .med, .mail, base, rmed)
#      · Analyse le .comm pour detecter automatiquement les UNITE requises
#      · Valide la coherence .comm / fichiers / .export avant soumission
#      · Cree un dossier scratch, copie les entrees, genere le .export
#      · Soumet CE MEME script via sbatch avec __RUN_PHASE=EXEC
#
#    Phase 2 — noeud de calcul (appel par sbatch)
#      · Lance le calcul, analyse le .mess, rapatrie les resultats
#
#  NOTE MPI : run_aster gere MPI en interne — ne PAS encapsuler dans srun.
#
#  Auteur  : Teo LEROY
#  Version : 12.0
#===============================================================================

# ══════════════════════════════════════════
#  CONFIGURATION GLOBALE
# ══════════════════════════════════════════

ASTER_ROOT="${ASTER_ROOT:-/opt/code_aster}"
ASTER_MODULE="${ASTER_MODULE:-}"
SCRATCH_BASE="${SCRATCH_BASE:-/scratch}"

# Ressources Slurm par defaut
DEFAULT_PARTITION="court"
DEFAULT_NODES=1
DEFAULT_NTASKS=1
DEFAULT_CPUS=1
DEFAULT_MEM="5G"
DEFAULT_TIME="05:00:00"

# Presets
declare -A PRESET_PARTITION=([court]="court"  [moyen]="normal" [long]="long")
declare -A PRESET_NTASKS=(   [court]=1        [moyen]=1        [long]=1)
declare -A PRESET_MEM=(      [court]="2G"     [moyen]="20G"    [long]="50G")
declare -A PRESET_TIME=(     [court]="05:00:00" [moyen]="03-00:00:00" [long]="30-00:00:00")

# ══════════════════════════════════════════
#  AFFICHAGE
# ══════════════════════════════════════════

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()      { echo -e "${GREEN}[ OK ]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()     { echo -e "${RED}[ ERR]${NC}  $*" >&2; }
log()     { echo "[$(date +%H:%M:%S)] $*"; }
section() { echo -e "\n${BOLD}${CYAN}> $*${NC}"; echo -e "${CYAN}$(printf -- '-%.0s' {1..60})${NC}"; }
header()  { echo ""; echo "========================================================"; echo "  $*"; echo "========================================================"; }

# ══════════════════════════════════════════
#  NAVIGATION CLAVIER — menus interactifs
# ══════════════════════════════════════════

_MENU_IDX=0; _MENU_ITEMS=(); _SAISIE=""; _TOUCHE=""

_lire_touche() {
    local k1 k2 k3
    IFS= read -r -s -n1 k1 </dev/tty
    if [[ "$k1" == $'\x1b' ]]; then
        IFS= read -r -s -n1 -t 0.05 k2 </dev/tty || k2=""
        IFS= read -r -s -n1 -t 0.05 k3 </dev/tty || k3=""
        _TOUCHE="${k1}${k2}${k3}"
    else
        _TOUCHE="$k1"
    fi
}

_dessiner_menu() {
    local sel="$1"; shift; local opts=("$@")
    for ((i=0; i<${#opts[@]}; i++)); do
        if ((i == sel)); then
            printf "  ${CYAN}${BOLD}❯ %-55s${NC}\n" "${opts[$i]}" >/dev/tty
        else
            printf "    %-55s\n" "${opts[$i]}" >/dev/tty
        fi
    done
}

_COCHES=()
_dessiner_cases() {
    local sel="$1"; shift; local opts=("$@"); local i marq
    for ((i=0; i<${#opts[@]}; i++)); do
        [ "$i" -eq "$sel" ] && marq="${CYAN}${BOLD}❯${NC}" || marq=" "
        if [ "${_COCHES[$i]}" = "1" ]; then
            printf "  %b [${GREEN}✔${NC}] %-51s\n" "$marq" "${opts[$i]}" >/dev/tty
        else
            printf "  %b [ ] %-51s\n"               "$marq" "${opts[$i]}" >/dev/tty
        fi
    done
}

menu_fleches() {
    local msg="$1"; shift; local opts=("$@"); local n=${#opts[@]} sel=0
    printf "\n${BOLD}  %s${NC}\n" "$msg" >/dev/tty
    tput civis >/dev/tty 2>/dev/null || true
    _dessiner_menu "$sel" "${opts[@]}"
    while true; do
        _lire_touche
        case "$_TOUCHE" in
            $'\x1b[A') sel=$(( (sel - 1 + n) % n )) ;;
            $'\x1b[B') sel=$(( (sel + 1) % n ))     ;;
            $'\x0d'|$'\x0a'|'') break ;;
            $'\x03') tput cnorm >/dev/tty 2>/dev/null || true; printf "\n" >/dev/tty; _MENU_IDX=-1; return ;;
        esac
        printf "\033[%dA" "$n" >/dev/tty
        _dessiner_menu "$sel" "${opts[@]}"
    done
    printf "\033[%dA" "$n" >/dev/tty
    for ((i=0; i<n; i++)); do
        if ((i == sel)); then
            printf "  ${GREEN}✔ ${BOLD}%-55s${NC}\n" "${opts[$i]}" >/dev/tty
        else
            printf "\033[2K\r\033[1B" >/dev/tty
        fi
    done
    tput cnorm >/dev/tty 2>/dev/null || true
    _MENU_IDX="$sel"
}

menu_cases() {
    local msg="$1"; shift; local opts=("$@"); local n=${#opts[@]} sel=0 i
    _COCHES=(); for ((i=0; i<n; i++)); do _COCHES[$i]=0; done
    printf "\n${BOLD}  %s${NC}\n" "$msg" >/dev/tty
    printf "  ${DIM}(espace : cocher  —  a : tout  —  i : inverser  —  entrée : valider)${NC}\n" >/dev/tty
    tput civis >/dev/tty 2>/dev/null || true
    _dessiner_cases "$sel" "${opts[@]}"
    while true; do
        _lire_touche
        local j
        case "$_TOUCHE" in
            $'\x1b[A') sel=$(( (sel - 1 + n) % n )) ;;
            $'\x1b[B') sel=$(( (sel + 1) % n ))     ;;
            ' ')        _COCHES[$sel]=$(( _COCHES[sel] ^ 1 )) ;;
            'a')        for ((j=0; j<n; j++)); do _COCHES[$j]=1; done ;;
            'i')        for ((j=0; j<n; j++)); do _COCHES[$j]=$(( _COCHES[j] ^ 1 )); done ;;
            $'\x0d'|$'\x0a'|'') break ;;
            $'\x03') tput cnorm >/dev/tty 2>/dev/null || true; printf "\n" >/dev/tty; _MENU_ITEMS=(); return ;;
        esac
        printf "\033[%dA" "$n" >/dev/tty
        _dessiner_cases "$sel" "${opts[@]}"
    done
    tput cnorm >/dev/tty 2>/dev/null || true
    _MENU_ITEMS=()
    for ((i=0; i<n; i++)); do [ "${_COCHES[$i]}" = "1" ] && _MENU_ITEMS+=("$i"); done
}

saisir() {
    local msg="$1" defaut="${2:-}"
    if [ -n "$defaut" ]; then
        printf "  ${BOLD}%s${NC} [${DIM}%s${NC}] : " "$msg" "$defaut" >/dev/tty
    else
        printf "  ${BOLD}%s${NC} : " "$msg" >/dev/tty
    fi
    IFS= read -r _SAISIE </dev/tty
    [ -z "$_SAISIE" ] && _SAISIE="$defaut"
}

# ══════════════════════════════════════════
#  UTILITAIRES
# ══════════════════════════════════════════

# Cherche les fichiers d'un type dans un dossier, retourne le premier
# Usage: _find_first STUDY_DIR "*.comm" → imprime le chemin ou rien
_find_first() {
    local dir="$1" pattern="$2"
    local -a arr=()
    shopt -s nullglob; arr=("${dir}"/${pattern}); shopt -u nullglob
    [ ${#arr[@]} -ge 1 ] && echo "${arr[0]}"
    [ ${#arr[@]} -gt 1 ] && warn "Plusieurs ${pattern} trouves, utilisation du premier" >&2
}

# Compte les fichiers d'un type
_count_files() {
    local dir="$1" pattern="$2"
    local -a arr=()
    shopt -s nullglob; arr=("${dir}"/${pattern}); shopt -u nullglob
    echo "${#arr[@]}"
}

# ══════════════════════════════════════════
#  ANALYSE DU .comm — detection des UNITE et validation
#
#  Deux fonctions :
#    _parse_comm_outputs  : detecte les IMPR_RESU/IMPR_TABLE/DEFI_FICHIER
#                           et leurs UNITE → remplit $_COMM_OUTPUTS
#    _validate_comm       : verifie la coherence globale du .comm :
#                           - UNITE de maillage vs fichiers presents
#                           - UNITE de sortie non declarees dans le .export
#                           - Commandes INCLUDE avec fichiers absents
#                           - POURSUITE sans base disponible
# ══════════════════════════════════════════

_COMM_OUTPUTS=()

# Aplatit les blocs multi-lignes Python du .comm en lignes uniques
_flatten_comm() {
    awk '
    BEGIN { buf=""; depth=0 }
    /^[[:space:]]*#/ { next }
    {
        line = $0; gsub(/#.*$/, "", line)
        buf = buf " " $0
        for (i=1; i<=length(line); i++) {
            c = substr(line, i, 1)
            if (c == "(") depth++
            else if (c == ")") depth--
        }
        if (depth <= 0 && buf ~ /[^[:space:]]/) {
            print buf; buf = ""; depth = 0
        }
    }
    END { if (buf ~ /[^[:space:]]/) print buf }
    ' "$1" 2>/dev/null
}

_parse_comm_outputs() {
    local comm_file="$1"
    _COMM_OUTPUTS=()
    local flat
    flat=$(_flatten_comm "$comm_file")

    while IFS= read -r block; do
        [ -z "$block" ] && continue
        local unite
        unite=$(echo "$block" | sed -n 's/.*UNITE[[:space:]]*=[[:space:]]*\([0-9]\+\).*/\1/p' | head -1)
        [ -z "$unite" ] && continue
        case "$unite" in 1|6|8|20|80) continue ;; esac

        local type label
        if echo "$block" | grep -q "IMPR_RESU"; then
            if echo "$block" | grep -qE "FORMAT[[:space:]]*=[[:space:]]*['\"]MED['\"]"; then
                type="rmed"; label="IMPR_RESU FORMAT=MED    →  unite $unite  (.rmed)"
            else
                type="resu"; label="IMPR_RESU              →  unite $unite  (.resu)"
            fi
        elif echo "$block" | grep -q "IMPR_TABLE"; then
            type="table"; label="IMPR_TABLE             →  unite $unite  (.table)"
        elif echo "$block" | grep -q "DEFI_FICHIER"; then
            local ext
            ext=$(echo "$block" | sed -n "s/.*FICHIER[[:space:]]*=[[:space:]]*['\"][^'\"]*\.\([a-zA-Z0-9]*\)['\"].*/\1/p" | head -1)
            type="${ext:-dat}"; label="DEFI_FICHIER           →  unite $unite${ext:+  (.$ext)}"
        else
            continue
        fi
        _COMM_OUTPUTS+=("${label}|${type}|${unite}")
    done <<< "$flat"
}

# ──────────────────────────────────────────────────────────────────────
#  _validate_comm : validation de coherence .comm / fichiers / options
#
#  Detecte AVANT soumission les erreurs classiques qui font planter
#  le calcul sur le noeud de calcul (perte de temps + quota scratch).
#
#  Controles effectues :
#    1. LIRE_MAILLAGE : verifie qu'un fichier .med ou .mail est present
#       si le .comm lit un maillage (UNITE=20 par defaut)
#    2. IMPR_RESU / IMPR_TABLE / DEFI_FICHIER avec UNITE non standard :
#       verifie que -R ou l'auto-detection les a declares
#    3. INCLUDE : verifie que les fichiers inclus existent dans STUDY_DIR
#    4. POURSUITE : avertit si aucune base n'est trouvee
#    5. Fichiers .py importes : verifie leur presence
#
#  Retourne 0 si tout est OK, 1 si erreur bloquante.
#  Les warnings ne bloquent pas la soumission.
# ──────────────────────────────────────────────────────────────────────

_validate_comm() {
    local comm_file="$1"
    local study_dir="$2"
    local has_med="$3"    # non-vide si .med present
    local has_mail="$4"   # non-vide si .mail present
    local results="$5"    # chaine RESULTS courante (type:unite,...)
    local errors=0

    local flat
    flat=$(_flatten_comm "$comm_file")

    section "Validation du .comm"

    # ── 1. Maillage requis ? ──────────────────────────────────────
    if echo "$flat" | grep -q "LIRE_MAILLAGE"; then
        if [ -z "$has_med" ] && [ -z "$has_mail" ]; then
            err "Le .comm contient LIRE_MAILLAGE mais aucun fichier .med ou .mail n'est present"
            errors=$((errors + 1))
        else
            ok "LIRE_MAILLAGE : maillage detecte"
        fi
    fi

    # ── 2. POURSUITE sans base ────────────────────────────────────
    if echo "$flat" | grep -q "POURSUITE"; then
        local has_base=0
        shopt -s nullglob
        local -a bases=("${study_dir}"/glob.* "${study_dir}"/pick.*)
        shopt -u nullglob
        [ ${#bases[@]} -gt 0 ] && has_base=1
        if [ "$has_base" -eq 0 ]; then
            warn "Le .comm contient POURSUITE mais aucune base (glob.*/pick.*) trouvee dans $study_dir"
            warn "  → Le calcul va probablement echouer. Utilisez -B pour specifier le dossier base."
        else
            ok "POURSUITE : base trouvee (${#bases[@]} fichiers)"
        fi
    fi

    # ── 3. UNITE non standard non declarees ───────────────────────
    # Extraire toutes les UNITE du .comm (hors standard 1,6,8,20,80)
    local -a all_unites=()
    while IFS= read -r block; do
        [ -z "$block" ] && continue
        local u
        u=$(echo "$block" | sed -n 's/.*UNITE[[:space:]]*=[[:space:]]*\([0-9]\+\).*/\1/p' | head -1)
        [ -z "$u" ] && continue
        case "$u" in 1|6|8|20|80) continue ;; esac
        # Verifier si c'est une sortie (IMPR_RESU, IMPR_TABLE, DEFI_FICHIER)
        if echo "$block" | grep -qE "IMPR_RESU|IMPR_TABLE|DEFI_FICHIER"; then
            all_unites+=("$u")
        fi
    done <<< "$flat"

    # Verifier que chaque UNITE est declaree dans RESULTS
    local declared_unites=""
    if [ -n "$results" ]; then
        declared_unites=$(echo "$results" | tr ',' '\n' | sed 's/.*://')
    fi

    local missing_unites=()
    for u in "${all_unites[@]}"; do
        if ! echo "$declared_unites" | grep -qw "$u"; then
            missing_unites+=("$u")
        fi
    done

    if [ ${#missing_unites[@]} -gt 0 ]; then
        warn "UNITE de sortie dans le .comm non declarees dans le .export :"
        for u in "${missing_unites[@]}"; do
            warn "  → UNITE=$u  (pas de ligne F ... R $u dans le .export)"
        done
        warn "Ces fichiers de sortie seront perdus apres le calcul !"
        warn "  Correction : ajoutez -R \"type:unite\" (ex: -R \"rmed:$u\")"
        warn "  Ou utilisez le mode interactif pour les selectionner."
    elif [ ${#all_unites[@]} -gt 0 ]; then
        ok "Toutes les UNITE de sortie (${all_unites[*]}) sont declarees"
    fi

    # ── 4. INCLUDE : fichiers presents ? ──────────────────────────
    local includes
    includes=$(echo "$flat" | grep -oP "INCLUDE\s*\(.*?DONNEE\s*=\s*['\"]([^'\"]+)['\"]" | \
               sed -n "s/.*['\"]\\([^'\"]*\\)['\"].*/\\1/p" 2>/dev/null || true)
    while IFS= read -r inc; do
        [ -z "$inc" ] && continue
        if [ ! -f "${study_dir}/${inc}" ]; then
            warn "INCLUDE reference '${inc}' — fichier absent de ${study_dir}"
        else
            ok "INCLUDE : ${inc} present"
        fi
    done <<< "$includes"

    # ── 5. Imports Python (.py) ───────────────────────────────────
    local py_imports
    py_imports=$(grep -oP "^\s*(?:from|import)\s+(\w+)" "$comm_file" 2>/dev/null | \
                 awk '{print $NF}' | sort -u || true)
    while IFS= read -r mod; do
        [ -z "$mod" ] && continue
        # Ignorer les modules standard Python et Code_Aster
        case "$mod" in
            os|sys|math|numpy|np|json|re|time|datetime|glob|shutil|pathlib) continue ;;
            code_aster|Cata|Macro|Comportement|Utilitai*) continue ;;
        esac
        if [ -f "${study_dir}/${mod}.py" ]; then
            ok "Import Python : ${mod}.py present"
        fi
        # On ne warn pas pour les imports inconnus (trop de faux positifs)
    done <<< "$py_imports"

    # ── 6. Verif rapide : DEBUT ou POURSUITE present ──────────────
    if ! echo "$flat" | grep -qE "^\s*(DEBUT|POURSUITE)\s*\("; then
        warn "Ni DEBUT() ni POURSUITE() trouve dans le .comm — fichier incomplet ?"
    fi

    if ! echo "$flat" | grep -qE "^\s*FIN\s*\(\s*\)"; then
        warn "FIN() absent du .comm — le calcul risque de ne pas terminer proprement"
    fi

    [ "$errors" -gt 0 ] && return 1
    return 0
}

# ──────────────────────────────────────────────────────────────────────
#  _auto_detect_results : analyse le .comm et ajoute automatiquement
#  les UNITE de sortie manquantes dans RESULTS.
#
#  Appele en mode CLI (non-interactif) pour eviter les oublis de -R.
# ──────────────────────────────────────────────────────────────────────

_auto_detect_results() {
    local comm_file="$1"
    _parse_comm_outputs "$comm_file"

    [ ${#_COMM_OUTPUTS[@]} -eq 0 ] && return

    local auto_results=""
    for item in "${_COMM_OUTPUTS[@]}"; do
        local _type="${item#*|}"; _type="${_type%%|*}"
        local _unite="${item##*|}"
        # Verifier si deja dans RESULTS
        if [ -n "$RESULTS" ] && echo "$RESULTS" | grep -q ":${_unite}"; then
            continue
        fi
        [ -n "$auto_results" ] && auto_results+=","
        auto_results+="${_type}:${_unite}"
    done

    if [ -n "$auto_results" ]; then
        if [ -n "$RESULTS" ]; then
            RESULTS="${RESULTS},${auto_results}"
        else
            RESULTS="$auto_results"
        fi
        info "Auto-detection des sorties du .comm : $auto_results"
    fi
}

# ══════════════════════════════════════════
#  MODE INTERACTIF
# ══════════════════════════════════════════

_verif_chemin() {
    local nom="$1" chemin="$2"
    [ -d "$chemin" ] && { _SAISIE="$chemin"; return; }
    section "Chemin $nom introuvable"
    warn "$nom = $chemin  (dossier absent)"
    saisir "Nouveau chemin $nom" "$chemin"
    [ -d "$_SAISIE" ] && ok "$nom : $_SAISIE" \
                      || warn "Dossier toujours absent — le calcul peut echouer"
}

mode_interactif() {
    printf "\n${CYAN}${BOLD}" >/dev/tty
    printf "  ╔══════════════════════════════════════════╗\n" >/dev/tty
    printf "  ║       RUN ASTER — Mode interactif        ║\n" >/dev/tty
    printf "  ║   Navigation  ↑↓  •  espace  •  entrée   ║\n" >/dev/tty
    printf "  ╚══════════════════════════════════════════╝\n" >/dev/tty
    printf "${NC}\n" >/dev/tty

    _verif_chemin "ASTER_ROOT"   "$ASTER_ROOT";   ASTER_ROOT="$_SAISIE"
    _verif_chemin "SCRATCH_BASE" "$SCRATCH_BASE"; SCRATCH_BASE="$_SAISIE"

    # ── Dossier d'etude ───────────────────────────────────────────
    section "Dossier d'étude"
    local -a dossiers=(); local d
    while IFS= read -r d; do dossiers+=("$d"); done < <(
        find . -maxdepth 2 -name "*.comm" -printf '%h\n' 2>/dev/null | sort -u | sed 's|^\./\?||;/^$/d'
    )
    [ ${#dossiers[@]} -eq 0 ] && dossiers=(".")

    if [ ${#dossiers[@]} -gt 1 ]; then
        menu_fleches "Dossier d'étude (contient un .comm) :" "${dossiers[@]}"
        [ "$_MENU_IDX" -eq -1 ] && { warn "Annulé."; exit 0; }
        STUDY_DIR="${dossiers[$_MENU_IDX]}"
    else
        saisir "Dossier d'étude" "${dossiers[0]}"
        STUDY_DIR="$_SAISIE"
    fi
    ok "Dossier : $STUDY_DIR"

    # ── Sorties du calcul ─────────────────────────────────────────
    section "Sorties du calcul"
    local _comm_found
    _comm_found=$(_find_first "$STUDY_DIR" "*.comm")

    if [ -n "$_comm_found" ]; then
        info "Lecture : $(basename "$_comm_found")"
        _parse_comm_outputs "$_comm_found"
    fi

    if [ ${#_COMM_OUTPUTS[@]} -gt 0 ]; then
        local -a _labels=()
        for _item in "${_COMM_OUTPUTS[@]}"; do _labels+=("${_item%%|*}"); done
        menu_cases "Sorties supplémentaires à inclure :" "${_labels[@]}"

        local _sel_results="" _idx
        for _idx in "${_MENU_ITEMS[@]}"; do
            _item="${_COMM_OUTPUTS[$_idx]}"
            local _type="${_item#*|}"; _type="${_type%%|*}"
            local _unite="${_item##*|}"
            [ -n "$_sel_results" ] && _sel_results+=","
            _sel_results+="${_type}:${_unite}"
        done
        [ -n "$_sel_results" ] && RESULTS="$_sel_results"
    else
        [ -n "$_comm_found" ] && info "Aucune sortie supplémentaire détectée dans le .comm"
        info "Les sorties par défaut seront générées (.mess, .resu, .rmed unite 80)"
    fi

    # ── Preset de ressources ──────────────────────────────────────
    section "Ressources Slurm"
    menu_fleches "Preset de ressources :" \
        "court   — ${PRESET_PARTITION[court]}  ${PRESET_MEM[court]}  ${PRESET_TIME[court]}" \
        "moyen   — ${PRESET_PARTITION[moyen]}  ${PRESET_MEM[moyen]}  ${PRESET_TIME[moyen]}" \
        "long    — ${PRESET_PARTITION[long]}   ${PRESET_MEM[long]}   ${PRESET_TIME[long]}" \
        "Manuel  — saisir les valeurs"
    [ "$_MENU_IDX" -eq -1 ] && { warn "Annulé."; exit 0; }

    local _presets=(court moyen long)
    if [ "$_MENU_IDX" -lt 3 ]; then
        local _p="${_presets[$_MENU_IDX]}"
        PARTITION="${PRESET_PARTITION[$_p]}"; NTASKS="${PRESET_NTASKS[$_p]}"
        MEM="${PRESET_MEM[$_p]}"; TIME_LIMIT="${PRESET_TIME[$_p]}"
    else
        saisir "Partition"       "$DEFAULT_PARTITION"; PARTITION="$_SAISIE"
        saisir "Nb nœuds"        "$DEFAULT_NODES";     NODES="$_SAISIE"
        saisir "Nb tâches MPI"   "$DEFAULT_NTASKS";    NTASKS="$_SAISIE"
        saisir "CPUs par tâche"  "$DEFAULT_CPUS";      CPUS="$_SAISIE"
        saisir "Mémoire (ex: 8G)" "$DEFAULT_MEM";      MEM="$_SAISIE"
        saisir "Durée max"       "$DEFAULT_TIME";       TIME_LIMIT="$_SAISIE"
    fi
    PRESET=""

    # ── Options ───────────────────────────────────────────────────
    section "Options"
    menu_cases "Options :" \
        "Suivre le job en temps réel (--follow)" \
        "Conserver le scratch après le calcul (--keep-scratch)" \
        "Dry-run — afficher sans soumettre (--dry-run)"

    for idx in "${_MENU_ITEMS[@]}"; do
        case "$idx" in 0) FOLLOW=1 ;; 1) KEEP_SCRATCH=1 ;; 2) DRY_RUN=1 ;; esac
    done

    # ── Récapitulatif ─────────────────────────────────────────────
    section "Récapitulatif"
    info "Dossier   : $STUDY_DIR"
    info "Partition : ${PARTITION:-$DEFAULT_PARTITION}  Tâches : ${NTASKS:-$DEFAULT_NTASKS}"
    info "Mémoire   : ${MEM:-$DEFAULT_MEM}  |  Durée : ${TIME_LIMIT:-$DEFAULT_TIME}"
    [ "$FOLLOW"       = "1" ] && info "Option    : --follow"
    [ "$KEEP_SCRATCH" = "1" ] && info "Option    : --keep-scratch"
    [ "$DRY_RUN"      = "1" ] && info "Option    : --dry-run"

    menu_fleches "Confirmer ?" "Soumettre le calcul" "Annuler"
    [ "$_MENU_IDX" -ne 0 ] && { warn "Annulé."; exit 0; }
}

# ══════════════════════════════════════════
#  AIDE
# ══════════════════════════════════════════

usage() {
    cat <<'EOF'
USAGE
  bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]

FICHIERS
  -C, --comm FILE       Fichier .comm (auto-detecte si absent)
  -M, --med  FILE       Fichier .med  (auto-detecte si absent)
  -A, --mail FILE       Fichier .mail (auto-detecte si absent)

RESULTATS SUPPLEMENTAIRES
  -R, --results LIST    Format "type:unite,..." (ex: "rmed:81,csv:38")
                        Si omis, les UNITE sont auto-detectees du .comm

RESSOURCES SLURM
  -P, --preset  NOM     court, moyen ou long
  -p, --partition NOM   Partition Slurm
  -n, --nodes N         Nombre de noeuds
  -t, --ntasks N        Taches MPI
  -c, --cpus N          CPUs par tache
  -m, --mem MEM         Memoire (ex: 8G)
  -T, --time DUREE      Duree max (J-HH:MM:SS)

OPTIONS
  -q, --quiet           Sortie minimale (juste le JOB ID)
  -f, --follow          Suivre le job apres soumission
      --keep-scratch    Ne pas supprimer le scratch
      --dry-run         Afficher sans lancer
      --no-validate     Desactiver la validation du .comm
      --debug           Mode verbose (set -x)
  -h, --help            Afficher cette aide
EOF
    exit 0
}

# ##########################################################################
#
#   PHASE 2 — NOEUD DE CALCUL
#
# ##########################################################################

if [ "${__RUN_PHASE:-}" = "EXEC" ]; then

    set -uo pipefail
    [ "${__DEBUG:-0}" = "1" ] && set -x
    ALREADY_COLLECTED=0

    _cp() { if command -v rsync &>/dev/null; then rsync -a "$@"; else cp -a "$@"; fi; }
    _log_dir() { ls -la "$1" 2>/dev/null | while IFS= read -r l; do log "  $l"; done; }

    collect_results() {
        [ "$ALREADY_COLLECTED" -eq 1 ] && return
        ALREADY_COLLECTED=1
        header "RAPATRIEMENT"

        local dest="${__STUDY_DIR}/run_${SLURM_JOB_ID}"
        mkdir -p "$dest" || { log "!! Impossible de creer $dest"; return; }
        local count=0

        log "Fichiers dans le scratch :"
        _log_dir "${__SCRATCH}/"

        _copy_if_exists() {
            local f="$1"
            [ -f "$f" ] && [ -s "$f" ] || return 0
            _cp "$f" "$dest/" && log "  -> $(basename "$f")" || log "  !! ECHEC : $(basename "$f")"
            count=$((count + 1))
        }

        shopt -s nullglob
        for ext in mess resu med csv table dat pos rmed txt vtu vtk py base; do
            for f in "${__SCRATCH}"/*."${ext}"; do _copy_if_exists "$f"; done
        done
        for f in "${__SCRATCH}"/glob.* "${__SCRATCH}"/pick.* "${__SCRATCH}"/vola.*; do
            _copy_if_exists "$f"
        done
        shopt -u nullglob

        if [ -d "${__SCRATCH}/REPE_OUT" ]; then
            _cp "${__SCRATCH}/REPE_OUT" "$dest/" && log "  -> REPE_OUT/"
            count=$((count + 1))
        fi

        rm -f "${__STUDY_DIR}/latest" 2>/dev/null
        ln -s "run_${SLURM_JOB_ID}" "${__STUDY_DIR}/latest" 2>/dev/null

        log "$count fichier(s) rapatrie(s) -> $dest"
        _log_dir "$dest/"

        if [ "${__KEEP_SCRATCH:-0}" != "1" ]; then
            rm -rf "$__SCRATCH" 2>/dev/null && log "Scratch supprime"
        else
            log "Scratch conserve : $__SCRATCH"
        fi
    }

    trap collect_results EXIT
    trap 'collect_results; exit 143' SIGTERM

    # ── En-tete ───────────────────────────────────────────────────
    header "CODE_ASTER — $(date)"
    log "Job       : $SLURM_JOB_ID"
    log "Noeud     : $SLURM_NODELIST"
    log "Scratch   : $__SCRATCH"

    # ── Chargement module ─────────────────────────────────────────
    if [ -n "${__MODULE:-}" ]; then
        if ! command -v module &>/dev/null; then
            for _mfile in /etc/profile.d/modules.sh /etc/profile.d/lmod.sh; do
                [ -f "$_mfile" ] && . "$_mfile" && break
            done
        fi
        if command -v module &>/dev/null; then
            module load "$__MODULE" 2>&1 && log "Module '$__MODULE' charge" || warn "Module '$__MODULE' echec"
        else
            warn "Commande module introuvable"
        fi
    fi

    # ── Executable Code_Aster ─────────────────────────────────────
    EXE=""
    for c in "${__ASTER_ROOT}/bin/run_aster" \
             "${__ASTER_ROOT}/bin/as_run" \
             "$(command -v run_aster 2>/dev/null || true)" \
             "$(command -v as_run 2>/dev/null || true)"; do
        [ -n "$c" ] && [ -x "$c" ] && { EXE="$c"; break; }
    done
    [ -z "$EXE" ] && { err "Code_Aster introuvable"; exit 1; }
    log "Executable : $EXE"
    "$EXE" --version 2>&1 | head -1 | while read -r l; do log "Version : $l"; done

    # ── Verification ──────────────────────────────────────────────
    header "VERIFICATION"
    log "Contenu scratch :"; _log_dir "$__SCRATCH/"
    log "Contenu .export :"
    cat "$__EXPORT" 2>/dev/null | while IFS= read -r l; do log "  $l"; done

    # ── Calcul ────────────────────────────────────────────────────
    header "CALCUL"
    log "Lancement : $(date)"
    RC=0; set +e; "$EXE" "$__EXPORT"; RC=$?; set -e
    log "Termine : $(date) — code retour $RC"

    # ── Diagnostic .mess ──────────────────────────────────────────
    _diagnose_mess() {
        local mess_file="$1"
        [ -f "$mess_file" ] || { log "!! Pas de .mess"; return 1; }
        local na nf ns
        na=$(grep -c "<A>" "$mess_file" 2>/dev/null || true)
        nf=$(grep -c "<F>" "$mess_file" 2>/dev/null || true)
        ns=$(grep -c "<S>" "$mess_file" 2>/dev/null || true)
        log "Alarmes <A>:$na  Fatales <F>:$nf  Exceptions <S>:$ns"
        [ "$nf" -gt 0 ] && grep -B2 -A5 "<F>" "$mess_file" | head -20
        [ "$ns" -gt 0 ] && [ "$nf" -eq 0 ] && grep -B2 -A5 "<S>" "$mess_file" | head -20
        [ "$nf" -gt 0 ] && return 1 || return 0
    }

    header "DIAGNOSTIC"
    _diagnose_mess "${__SCRATCH}/${__STUDY_NAME}.mess" || _log_dir "$__SCRATCH/"

    log "Contenu scratch apres calcul :"
    _log_dir "$__SCRATCH/"

    collect_results
    header "FIN"
    [ "$RC" -eq 0 ] && log "SUCCES" || log "ECHEC (code $RC)"
    log "Resultats : ${__STUDY_DIR}/run_${SLURM_JOB_ID}"
    exit $RC
fi

# ##########################################################################
#
#   PHASE 1 — NOEUD LOGIN
#
# ##########################################################################

STUDY_DIR="."; COMM=""; MED=""; MAIL=""
PRESET=""; PARTITION=""; NODES=""; NTASKS=""; CPUS=""; MEM=""; TIME_LIMIT=""
QUIET=false; RESULTS=""; KEEP_SCRATCH=0; DRY_RUN=0; DEBUG=0; FOLLOW=0
NO_VALIDATE=0
_NARGS=$#

# ── Parsing des arguments ─────────────────────────────────────
while [ $# -gt 0 ]; do
    case "$1" in
        -C|--comm)      COMM="$2";        shift 2 ;;
        -M|--med)       MED="$2";         shift 2 ;;
        -A|--mail)      MAIL="$2";        shift 2 ;;
        -R|--results)   RESULTS="$2";     shift 2 ;;
        -P|--preset)    PRESET="$2";      shift 2 ;;
        -p|--partition) PARTITION="$2";   shift 2 ;;
        -n|--nodes)     NODES="$2";       shift 2 ;;
        -t|--ntasks)    NTASKS="$2";      shift 2 ;;
        -c|--cpus)      CPUS="$2";        shift 2 ;;
        -m|--mem)       MEM="$2";         shift 2 ;;
        -T|--time)      TIME_LIMIT="$2";  shift 2 ;;
        -q|--quiet)     QUIET=true;       shift ;;
        -f|--follow)    FOLLOW=1;         shift ;;
        --keep-scratch) KEEP_SCRATCH=1;   shift ;;
        --dry-run)      DRY_RUN=1;        shift ;;
        --no-validate)  NO_VALIDATE=1;    shift ;;
        --debug)        DEBUG=1;          shift ;;
        -h|--help)      usage ;;
        -*)             err "Option inconnue : $1"; usage ;;
        *)              STUDY_DIR="$1";   shift ;;
    esac
done

# ── Mode interactif (avant set -euo pipefail) ────────────────
[ "$_NARGS" -eq 0 ] && mode_interactif

set -euo pipefail

# ── Presets ───────────────────────────────────────────────────
if [ -n "$PRESET" ]; then
    local_preset="${PRESET,,}"
    case "$local_preset" in
        court|short)   local_preset="court" ;;
        normal|medium) local_preset="moyen" ;;
        long)          local_preset="long"  ;;
        *) err "Preset inconnu : $PRESET"; exit 1 ;;
    esac
    : "${PARTITION:=${PRESET_PARTITION[$local_preset]}}"
    : "${NTASKS:=${PRESET_NTASKS[$local_preset]}}"
    : "${MEM:=${PRESET_MEM[$local_preset]}}"
    : "${TIME_LIMIT:=${PRESET_TIME[$local_preset]}}"
    $QUIET || info "Preset : $PRESET"
fi

: "${PARTITION:=$DEFAULT_PARTITION}"; : "${NODES:=$DEFAULT_NODES}"
: "${NTASKS:=$DEFAULT_NTASKS}"; : "${CPUS:=$DEFAULT_CPUS}"
: "${MEM:=$DEFAULT_MEM}"; : "${TIME_LIMIT:=$DEFAULT_TIME}"

# ── Detection des fichiers ────────────────────────────────────
$QUIET || section "Detection des fichiers"

STUDY_DIR="$(realpath "$STUDY_DIR")"
STUDY_NAME="$(basename "$STUDY_DIR")"
[ -d "$STUDY_DIR" ] || { err "Dossier introuvable : $STUDY_DIR"; exit 1; }

# .comm (obligatoire)
if [ -z "$COMM" ]; then
    COMM=$(_find_first "$STUDY_DIR" "*.comm")
    [ -z "$COMM" ] && { err "Aucun .comm dans $STUDY_DIR"; exit 1; }
fi
COMM="$(realpath "$COMM")"
$QUIET || ok "Comm : $COMM"

# .med (optionnel)
if [ -z "$MED" ]; then
    MED=$(_find_first "$STUDY_DIR" "*.med" 2>/dev/null || true)
fi
[ -n "$MED" ] && { MED="$(realpath "$MED")"; $QUIET || ok "Med  : $MED"; }

# .mail (optionnel)
if [ -z "$MAIL" ]; then
    MAIL=$(_find_first "$STUDY_DIR" "*.mail" 2>/dev/null || true)
fi
[ -n "$MAIL" ] && { MAIL="$(realpath "$MAIL")"; $QUIET || ok "Mail : $MAIL"; }

# ── Auto-detection des UNITE de sortie du .comm ──────────────
if [ -z "$RESULTS" ]; then
    _auto_detect_results "$COMM"
fi

# ── Validation du .comm ──────────────────────────────────────
if [ "$NO_VALIDATE" -eq 0 ] && ! $QUIET; then
    if ! _validate_comm "$COMM" "$STUDY_DIR" "$MED" "$MAIL" "$RESULTS"; then
        err "Validation du .comm echouee — corrigez les erreurs ci-dessus"
        err "  Utilisez --no-validate pour forcer la soumission"
        exit 1
    fi
fi

# ── Preparation du scratch ────────────────────────────────────
$QUIET || section "Preparation du scratch"
SCRATCH="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_$(date +%s)_$$"
mkdir -p "$SCRATCH"
$QUIET || ok "Scratch : $SCRATCH"

cp "$COMM" "$SCRATCH/"
[ -n "$MED" ]  && cp "$MED" "$SCRATCH/"
[ -n "$MAIL" ] && cp "$MAIL" "$SCRATCH/"

shopt -s nullglob
for f in "$STUDY_DIR"/*.py "$STUDY_DIR"/*.dat "$STUDY_DIR"/*.para \
         "$STUDY_DIR"/*.include "$STUDY_DIR"/*.mfront; do
    cp "$f" "$SCRATCH/"
done
shopt -u nullglob

# ── Conversion memoire / duree ────────────────────────────────
MEM_MB=$(echo "$MEM" | awk '
    tolower($0) ~ /g$/ { gsub(/[gGiI]/,""); print int($0*1024); next }
    tolower($0) ~ /m$/ { gsub(/[mMiI]/,""); print int($0);      next }
    /^[0-9]+$/          { print int($0); next }
    { print -1 }')
[ "$MEM_MB" -le 0 ] 2>/dev/null && { err "Memoire invalide : $MEM"; exit 1; }
ASTER_MEM=$(( MEM_MB - 512 ))
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512

TIME_SEC=$(echo "$TIME_LIMIT" | awk -F'[-:]' '
    NF==4 {print $1*86400+$2*3600+$3*60+$4; next}
    NF==3 {print $1*3600+$2*60+$3;          next}
    NF==2 {print $1*60+$2;                  next}
    {print $1*60}')

# ── Generation du .export ─────────────────────────────────────
$QUIET || section "Generation du .export"
EXPORT="${SCRATCH}/${STUDY_NAME}.export"
{
    echo "P time_limit $TIME_SEC"
    echo "P memory_limit $ASTER_MEM"
    echo "P ncpus $NTASKS"

    echo "F comm ${SCRATCH}/$(basename "$COMM") D 1"
    [ -n "$MED" ]  && echo "F mmed ${SCRATCH}/$(basename "$MED") D 20"
    [ -n "$MAIL" ] && echo "F mail ${SCRATCH}/$(basename "$MAIL") D 20"

    echo "F mess ${SCRATCH}/${STUDY_NAME}.mess R 6"
    echo "F resu ${SCRATCH}/${STUDY_NAME}.resu R 8"
    echo "F rmed ${SCRATCH}/${STUDY_NAME}_resu.rmed R 80"

    if [ -n "$RESULTS" ]; then
        IFS=',' read -ra ITEMS <<< "${RESULTS// /}"
        for item in "${ITEMS[@]}"; do
            TYPE="${item%%:*}"; UNIT="${item##*:}"
            echo "F ${TYPE} ${SCRATCH}/${STUDY_NAME}_u${UNIT}.${TYPE} R ${UNIT}"
        done
    fi
} > "$EXPORT"

if ! $QUIET; then
    ok "Export : $EXPORT"
    while IFS= read -r line; do info "  $line"; done < "$EXPORT"
fi

# ── Recapitulatif ─────────────────────────────────────────────
if ! $QUIET; then
    section "Ressources Slurm"
    info "Partition : $PARTITION | Noeuds : $NODES | Taches : $NTASKS | CPUs : $CPUS"
    info "Memoire   : $MEM (${ASTER_MEM}MB pour Aster) | Duree : $TIME_LIMIT"
    [ "$KEEP_SCRATCH" = "1" ] && info "Scratch   : conserve"
fi

# ── Suivi du job ──────────────────────────────────────────────
_follow_job() {
    local job="$1" logfile="$2"
    local state="" spinner_idx=0
    local -a SP=('|' '/' '-' '\')

    echo ""
    while true; do
        state=$(squeue -j "$job" -h -o "%T" 2>/dev/null || true)
        [ -z "$state" ] && break
        if [ "$state" = "RUNNING" ]; then
            printf "\r  %-70s\n" "Etat : RUNNING"; break
        fi
        printf "\r  %s  %-12s  %s" "${SP[$spinner_idx]}" "$state" "(Ctrl+C pour detacher)"
        spinner_idx=$(( (spinner_idx+1) % 4 ))
        sleep 3
    done

    if [ "$state" = "RUNNING" ]; then
        info "Logs en temps reel — Ctrl+C pour detacher :"
        echo ""
        local t=0
        while ! [ -f "$logfile" ] && [ "$t" -lt 30 ]; do sleep 1; t=$((t + 1)); done
        [ -f "$logfile" ] || warn "Fichier log introuvable : $logfile"

        tail -f "$logfile" &
        local TAIL_PID=$!
        # shellcheck disable=SC2064
        trap "kill $TAIL_PID 2>/dev/null; echo ''; info 'Detache — job $job toujours en cours'; exit 0" INT
        while squeue -j "$job" -h &>/dev/null; do sleep 5; done
        sleep 2; kill $TAIL_PID 2>/dev/null; wait $TAIL_PID 2>/dev/null; trap - INT
    fi

    # Bilan
    local dest="${STUDY_DIR}/run_${job}"
    echo ""; section "BILAN JOB $job"
    if [ -d "$dest" ]; then
        local mess na nf ns
        mess=$(ls "${dest}"/*.mess 2>/dev/null | head -1 || true)
        if [ -n "$mess" ]; then
            na=$(grep -c "<A>" "$mess" 2>/dev/null || true)
            nf=$(grep -c "<F>" "$mess" 2>/dev/null || true)
            ns=$(grep -c "<S>" "$mess" 2>/dev/null || true)
            if [ "$nf" -eq 0 ] && [ "$ns" -eq 0 ]; then
                ok "Calcul terminé — $na alarme(s)"
            else
                err "Calcul en échec — <F>:$nf  <S>:$ns  <A>:$na"
                [ "$nf" -gt 0 ] && grep -B2 -A5 "<F>" "$mess" | head -20
            fi
        fi
        ok "Resultats : $dest"
        ls "$dest/" 2>/dev/null | while read -r f; do info "  $f"; done
    else
        warn "Dossier de resultats absent : $dest"
    fi
}

# ── Soumission sbatch ─────────────────────────────────────────
$QUIET || section "Soumission Slurm"

SELF="$(realpath "$0")"
VARS="ALL,__RUN_PHASE=EXEC,__STUDY_DIR=${STUDY_DIR},__STUDY_NAME=${STUDY_NAME}"
VARS+=",__SCRATCH=${SCRATCH},__EXPORT=${EXPORT},__ASTER_ROOT=${ASTER_ROOT}"
VARS+=",__MODULE=${ASTER_MODULE},__KEEP_SCRATCH=${KEEP_SCRATCH},__DEBUG=${DEBUG}"

CMD=(sbatch --parsable
    --job-name="aster_${STUDY_NAME}"
    --partition="$PARTITION"
    --nodes="$NODES"
    --ntasks="$NTASKS"
    --cpus-per-task="$CPUS"
    --mem="$MEM"
    --time="$TIME_LIMIT"
    --output="${STUDY_DIR}/aster_run_%j.out"
    --error="${STUDY_DIR}/aster_run_%j.err"
    --export="$VARS"
    "$SELF"
)

if [ "$DRY_RUN" = "1" ]; then
    section "DRY RUN — commande sbatch (non lancee)"
    echo "  ${CMD[*]}"
    echo ""
    info "Contenu du .export genere :"
    while IFS= read -r line; do info "  $line"; done < "$EXPORT"
    exit 0
fi

JOB=$("${CMD[@]}") || { err "sbatch a echoue"; exit 1; }
[ -z "$JOB" ] && { err "Job ID vide"; exit 1; }

if $QUIET; then
    echo "$JOB"
else
    ok "Job $JOB soumis"
    echo ""
    echo "  squeue -j $JOB              # Etat du job"
    echo "  tail -f ${STUDY_DIR}/aster_run_${JOB}.out"
    echo "  scancel $JOB                # Annuler"
    echo "  ls ${STUDY_DIR}/run_${JOB}/ # Resultats"
    echo ""
    [ "$FOLLOW" = "1" ] && _follow_job "$JOB" "${STUDY_DIR}/aster_run_${JOB}.out"
fi
