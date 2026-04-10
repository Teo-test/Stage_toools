#!/bin/bash
#===============================================================================
#  run_aster.sh — Soumission de calculs Code_Aster via Slurm
#===============================================================================
#
#  Usage :  bash run_aster.sh [-h|--help]
#
#  ARCHITECTURE EN DEUX PHASES DANS UN SEUL FICHIER
#  ─────────────────────────────────────────────────
#    Phase 1 — noeud login (mode interactif uniquement)
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
#  Version : 13.0
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
declare -A PRESET_NODES=(    [court]=1        [moyen]=1        [long]=1)
declare -A PRESET_NTASKS=(   [court]=1        [moyen]=1        [long]=1)
declare -A PRESET_MEM=(      [court]="2G"     [moyen]="20G"    [long]="50G")
declare -A PRESET_TIME=(     [court]="05:00:00" [moyen]="03-00:00:00" [long]="30-00:00:00")

# ══════════════════════════════════════════
#  AFFICHAGE
# ══════════════════════════════════════════

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; MAGENTA='\033[0;35m'
BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'
BG_CYAN='\033[46m'; BG_GREEN='\033[42m'; BG_RED='\033[41m'; FG_BLACK='\033[30m'

info()    { echo -e "  ${BLUE}●${NC}  $*"; }
ok()      { echo -e "  ${GREEN}✔${NC}  $*"; }
warn()    { echo -e "  ${YELLOW}⚠${NC}  $*"; }
err()     { echo -e "  ${RED}✖${NC}  $*" >&2; }
log()     { echo "[$(date +%H:%M:%S)] $*"; }
header()  { echo ""; echo "========================================================"; echo "  $*"; echo "========================================================"; }

section() {
    echo "" >/dev/tty
    echo -e "  ${CYAN}${BOLD}── $* ──${NC}" >/dev/tty
}

banner() {
    local w=48
    echo "" >/dev/tty
    echo -e "  ${CYAN}╔$(printf '═%.0s' $(seq 1 $w))╗${NC}" >/dev/tty
    printf "  ${CYAN}║${NC}${BOLD}%-${w}s${NC}${CYAN}║${NC}\n" "  🔧  RUN ASTER — Mode interactif" >/dev/tty
    printf "  ${CYAN}║${NC}${DIM}%-${w}s${NC}${CYAN}║${NC}\n" "  Navigation : ↑↓  sélection : entrée" >/dev/tty
    echo -e "  ${CYAN}╚$(printf '═%.0s' $(seq 1 $w))╝${NC}" >/dev/tty
    echo "" >/dev/tty
}

# Barre de progression des étapes
_ETAPE_COURANTE=0
_ETAPES=("Dossier" "Fichiers" "Sorties" "Ressources" "Options" "Confirmation")

afficher_progression() {
    local n=${#_ETAPES[@]}
    local bar=""
    for ((i=0; i<n; i++)); do
        if [ "$i" -lt "$_ETAPE_COURANTE" ]; then
            bar+="${GREEN}●${NC} "
        elif [ "$i" -eq "$_ETAPE_COURANTE" ]; then
            bar+="${CYAN}${BOLD}●${NC} "
        else
            bar+="${DIM}○${NC} "
        fi
    done
    echo -e "\n  ${bar} ${DIM}(${_ETAPES[$_ETAPE_COURANTE]})${NC}" >/dev/tty
}

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
            printf "    ${CYAN}${BOLD}❯ %-55s${NC}\n" "${opts[$i]}" >/dev/tty
        else
            printf "      %-55s\n" "${opts[$i]}" >/dev/tty
        fi
    done
}

_COCHES=()
_dessiner_cases() {
    local sel="$1"; shift; local opts=("$@"); local i marq
    for ((i=0; i<${#opts[@]}; i++)); do
        [ "$i" -eq "$sel" ] && marq="${CYAN}${BOLD}❯${NC}" || marq=" "
        if [ "${_COCHES[$i]}" = "1" ]; then
            printf "    %b [${GREEN}✔${NC}] %-51s\n" "$marq" "${opts[$i]}" >/dev/tty
        else
            printf "    %b [ ] %-51s\n"               "$marq" "${opts[$i]}" >/dev/tty
        fi
    done
}

menu_fleches() {
    local msg="$1"; shift; local opts=("$@"); local n=${#opts[@]} sel=0
    printf "\n    ${BOLD}%s${NC}\n" "$msg" >/dev/tty
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
            printf "    ${GREEN}✔ ${BOLD}%-55s${NC}\n" "${opts[$i]}" >/dev/tty
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
    printf "\n    ${BOLD}%s${NC}\n" "$msg" >/dev/tty
    printf "    ${DIM}(espace : cocher  —  a : tout  —  i : inverser  —  entrée : valider)${NC}\n" >/dev/tty
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
        printf "    ${BOLD}%s${NC} [${DIM}%s${NC}] : " "$msg" "$defaut" >/dev/tty
    else
        printf "    ${BOLD}%s${NC} : " "$msg" >/dev/tty
    fi
    IFS= read -r _SAISIE </dev/tty
    [ -z "$_SAISIE" ] && _SAISIE="$defaut"
}

# ══════════════════════════════════════════
#  UTILITAIRES
# ══════════════════════════════════════════

_find_first() {
    local dir="$1" pattern="$2"
    local -a arr=()
    shopt -s nullglob; arr=("${dir}"/${pattern}); shopt -u nullglob
    [ ${#arr[@]} -ge 1 ] && echo "${arr[0]}"
    [ ${#arr[@]} -gt 1 ] && warn "Plusieurs ${pattern} trouves, utilisation du premier" >&2
}

_count_files() {
    local dir="$1" pattern="$2"
    local -a arr=()
    shopt -s nullglob; arr=("${dir}"/${pattern}); shopt -u nullglob
    echo "${#arr[@]}"
}

# ══════════════════════════════════════════
#  ANALYSE DU .comm
# ══════════════════════════════════════════

_COMM_OUTPUTS=()

_flatten_comm() {
    awk '
    BEGIN { buf=""; depth=0 }
    /^[[:space:]]*#/ { next }
    {
        line = $0; gsub(/#.*$/, "", line)
        buf = buf " " line
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

_validate_comm() {
    local comm_file="$1"
    local study_dir="$2"
    local has_med="$3"
    local has_mail="$4"
    local results="$5"
    local errors=0

    local flat
    flat=$(_flatten_comm "$comm_file")

    section "Validation du .comm"

    if echo "$flat" | grep -q "LIRE_MAILLAGE"; then
        if [ -z "$has_med" ] && [ -z "$has_mail" ]; then
            err "Le .comm contient LIRE_MAILLAGE mais aucun fichier .med ou .mail n'est present"
            errors=$((errors + 1))
        else
            ok "LIRE_MAILLAGE : maillage detecte"
        fi
    fi

    if echo "$flat" | grep -q "POURSUITE"; then
        local has_base=0
        shopt -s nullglob
        local -a bases=("${study_dir}"/glob.* "${study_dir}"/pick.*)
        shopt -u nullglob
        [ ${#bases[@]} -gt 0 ] && has_base=1
        if [ "$has_base" -eq 0 ]; then
            warn "Le .comm contient POURSUITE mais aucune base (glob.*/pick.*) trouvee dans $study_dir"
            warn "  → Le calcul va probablement echouer. Specifiez le dossier base dans l'etape suivante."
        else
            ok "POURSUITE : base trouvee (${#bases[@]} fichiers)"
        fi
    fi

    local -a all_unites=()
    while IFS= read -r block; do
        [ -z "$block" ] && continue
        local u
        u=$(echo "$block" | sed -n 's/.*UNITE[[:space:]]*=[[:space:]]*\([0-9]\+\).*/\1/p' | head -1)
        [ -z "$u" ] && continue
        case "$u" in 1|6|8|20|80) continue ;; esac
        if echo "$block" | grep -qE "IMPR_RESU|IMPR_TABLE|DEFI_FICHIER"; then
            all_unites+=("$u")
        fi
    done <<< "$flat"

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
    elif [ ${#all_unites[@]} -gt 0 ]; then
        ok "Toutes les UNITE de sortie (${all_unites[*]}) sont declarees"
    fi

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

    local py_imports
    py_imports=$(grep -oP "^\s*(?:from|import)\s+(\w+)" "$comm_file" 2>/dev/null | \
                 awk '{print $NF}' | sort -u || true)
    while IFS= read -r mod; do
        [ -z "$mod" ] && continue
        case "$mod" in
            os|sys|math|numpy|np|json|re|time|datetime|glob|shutil|pathlib) continue ;;
            code_aster|Cata|Macro|Comportement|Utilitai*) continue ;;
        esac
        if [ -f "${study_dir}/${mod}.py" ]; then
            ok "Import Python : ${mod}.py present"
        fi
    done <<< "$py_imports"

    if ! echo "$flat" | grep -qE "^\s*(DEBUT|POURSUITE)\s*\("; then
        warn "Ni DEBUT() ni POURSUITE() trouve dans le .comm — fichier incomplet ?"
    fi

    if ! echo "$flat" | grep -qE "^\s*FIN\s*\(\s*\)"; then
        warn "FIN() absent du .comm — le calcul risque de ne pas terminer proprement"
    fi

    [ "$errors" -gt 0 ] && return 1
    return 0
}

# ══════════════════════════════════════════
#  AIDE
# ══════════════════════════════════════════

usage() {
    cat <<'EOF'
USAGE
  bash run_aster.sh [-h|--help]

  Lance le mode interactif pour configurer et soumettre un calcul
  Code_Aster via Slurm.

  Navigation : fleches ↑↓, espace pour cocher, entrée pour valider.

VARIABLES D'ENVIRONNEMENT
  ASTER_ROOT     Chemin de Code_Aster  (defaut: /opt/code_aster)
  ASTER_MODULE   Module a charger sur le noeud de calcul
  SCRATCH_BASE   Racine du scratch     (defaut: /scratch)

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

    header "CODE_ASTER — $(date)"
    log "Job       : $SLURM_JOB_ID"
    log "Noeud     : $SLURM_NODELIST"
    log "Scratch   : $__SCRATCH"

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

    header "VERIFICATION"
    log "Contenu scratch :"; _log_dir "$__SCRATCH/"
    log "Contenu .export :"
    while IFS= read -r l; do log "  $l"; done < "$__EXPORT"

    header "CALCUL"
    log "Lancement : $(date)"
    RC=0; set +e; "$EXE" "$__EXPORT"; RC=$?; set -e
    log "Termine : $(date) — code retour $RC"

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
#   PHASE 1 — NOEUD LOGIN (MODE INTERACTIF)
#
# ##########################################################################

# ── Seul --help est traite en argument ────────────────────────
case "${1:-}" in -h|--help) usage ;; esac

# ── Variables ─────────────────────────────────────────────────
STUDY_DIR=""; COMM=""; MED=""; MAIL=""; BASE_DIR=""
PARTITION=""; NODES=""; NTASKS=""; CPUS=""; MEM=""; TIME_LIMIT=""
RESULTS=""; KEEP_SCRATCH=0; DRY_RUN=0; DEBUG=0; FOLLOW=0
NO_VALIDATE=0

# ══════════════════════════════════════════════════════════════
#  MODE INTERACTIF — tout-en-un
# ══════════════════════════════════════════════════════════════

banner

# ── Vérification ASTER_ROOT et SCRATCH_BASE ──────────────────
if [ ! -d "$ASTER_ROOT" ]; then
    warn "ASTER_ROOT = $ASTER_ROOT  (dossier absent)" >/dev/tty
    saisir "Chemin ASTER_ROOT" "$ASTER_ROOT"
    ASTER_ROOT="$_SAISIE"
fi
if [ ! -d "$SCRATCH_BASE" ]; then
    warn "SCRATCH_BASE = $SCRATCH_BASE  (dossier absent)" >/dev/tty
    saisir "Chemin SCRATCH_BASE" "$SCRATCH_BASE"
    SCRATCH_BASE="$_SAISIE"
fi

# ┌─────────────────────────────────────────────────────────────
# │ ÉTAPE 1 — Dossier d'étude
# └─────────────────────────────────────────────────────────────
_ETAPE_COURANTE=0
afficher_progression
section "Dossier d'étude"

local_dossiers=()
while IFS= read -r d; do local_dossiers+=("$d"); done < <(
    find . -maxdepth 2 -name "*.comm" -printf '%h\n' 2>/dev/null | sort -u | sed 's|^\./\?||;/^$/d'
)
[ ${#local_dossiers[@]} -eq 0 ] && local_dossiers=(".")

if [ ${#local_dossiers[@]} -gt 1 ]; then
    menu_fleches "Dossier contenant le .comm :" "${local_dossiers[@]}"
    [ "$_MENU_IDX" -eq -1 ] && { warn "Annulé."; exit 0; }
    STUDY_DIR="${local_dossiers[$_MENU_IDX]}"
else
    saisir "Dossier d'étude" "${local_dossiers[0]}"
    STUDY_DIR="$_SAISIE"
fi

STUDY_DIR="$(realpath "$STUDY_DIR")"
STUDY_NAME="$(basename "$STUDY_DIR")"
[ -d "$STUDY_DIR" ] || { err "Dossier introuvable : $STUDY_DIR"; exit 1; }
if [[ "$STUDY_NAME" =~ [,=\ ] ]]; then
    err "Le nom du dossier ne peut pas contenir de virgule, espace ou '=' : '$STUDY_NAME'"
    exit 1
fi
ok "Dossier : ${STUDY_DIR}" >/dev/tty

# ┌─────────────────────────────────────────────────────────────
# │ ÉTAPE 2 — Détection des fichiers
# └─────────────────────────────────────────────────────────────
_ETAPE_COURANTE=1
afficher_progression
section "Détection des fichiers"

# .comm
COMM=$(_find_first "$STUDY_DIR" "*.comm")
[ -z "$COMM" ] && { err "Aucun .comm dans $STUDY_DIR"; exit 1; }
COMM="$(realpath "$COMM")"
ok ".comm : $(basename "$COMM")" >/dev/tty

# .med
MED=$(_find_first "$STUDY_DIR" "*.med" 2>/dev/null || true)
[ -n "$MED" ] && { MED="$(realpath "$MED")"; ok ".med  : $(basename "$MED")" >/dev/tty; }

# .mail
MAIL=$(_find_first "$STUDY_DIR" "*.mail" 2>/dev/null || true)
[ -n "$MAIL" ] && { MAIL="$(realpath "$MAIL")"; ok ".mail : $(basename "$MAIL")" >/dev/tty; }

# Base de poursuite
if grep -q "POURSUITE" "$COMM" 2>/dev/null; then
    echo "" >/dev/tty
    info "POURSUITE détecté dans le .comm" >/dev/tty
    saisir "Dossier de base (glob.*/pick.*) — vide = auto-detection" ""
    [ -n "$_SAISIE" ] && BASE_DIR="$_SAISIE"
fi

# Fichiers auxiliaires
shopt -s nullglob
local_aux=("$STUDY_DIR"/*.py "$STUDY_DIR"/*.dat "$STUDY_DIR"/*.para \
           "$STUDY_DIR"/*.include "$STUDY_DIR"/*.mfront)
shopt -u nullglob
if [ ${#local_aux[@]} -gt 0 ]; then
    info "${#local_aux[@]} fichier(s) auxiliaire(s) détecté(s)" >/dev/tty
fi

# ┌─────────────────────────────────────────────────────────────
# │ ÉTAPE 3 — Sorties du calcul
# └─────────────────────────────────────────────────────────────
_ETAPE_COURANTE=2
afficher_progression
section "Sorties du calcul"

info "Analyse de $(basename "$COMM")..." >/dev/tty
_parse_comm_outputs "$COMM"

if [ ${#_COMM_OUTPUTS[@]} -gt 0 ]; then
    local_labels=()
    for _item in "${_COMM_OUTPUTS[@]}"; do local_labels+=("${_item%%|*}"); done
    menu_cases "Sorties supplémentaires à inclure :" "${local_labels[@]}"

    local_sel_results=""
    for _idx in "${_MENU_ITEMS[@]}"; do
        _item="${_COMM_OUTPUTS[$_idx]}"
        local_type="${_item#*|}"; local_type="${local_type%%|*}"
        local_unite="${_item##*|}"
        [ -n "$local_sel_results" ] && local_sel_results+=","
        local_sel_results+="${local_type}:${local_unite}"
    done
    [ -n "$local_sel_results" ] && RESULTS="$local_sel_results"
else
    info "Aucune sortie supplémentaire détectée dans le .comm" >/dev/tty
    info "Sorties par défaut : .mess, .resu, .rmed (unite 80)" >/dev/tty
fi

# ┌─────────────────────────────────────────────────────────────
# │ ÉTAPE 4 — Ressources Slurm
# └─────────────────────────────────────────────────────────────
_ETAPE_COURANTE=3
afficher_progression
section "Ressources Slurm"

menu_fleches "Preset de ressources :" \
    "court   — ${PRESET_PARTITION[court]}   │ ${PRESET_MEM[court]}   │ ${PRESET_TIME[court]}" \
    "moyen   — ${PRESET_PARTITION[moyen]}  │ ${PRESET_MEM[moyen]}  │ ${PRESET_TIME[moyen]}" \
    "long    — ${PRESET_PARTITION[long]}    │ ${PRESET_MEM[long]}  │ ${PRESET_TIME[long]}" \
    "Manuel  — saisir les valeurs"
[ "$_MENU_IDX" -eq -1 ] && { warn "Annulé."; exit 0; }

local_presets=(court moyen long)
if [ "$_MENU_IDX" -lt 3 ]; then
    local_p="${local_presets[$_MENU_IDX]}"
    PARTITION="${PRESET_PARTITION[$local_p]}"
    NODES="${PRESET_NODES[$local_p]}"
    NTASKS="${PRESET_NTASKS[$local_p]}"
    MEM="${PRESET_MEM[$local_p]}"
    TIME_LIMIT="${PRESET_TIME[$local_p]}"
    CPUS="$DEFAULT_CPUS"
else
    saisir "Partition"        "$DEFAULT_PARTITION"; PARTITION="$_SAISIE"
    saisir "Nb nœuds"         "$DEFAULT_NODES";     NODES="$_SAISIE"
    saisir "Nb tâches MPI"    "$DEFAULT_NTASKS";    NTASKS="$_SAISIE"
    saisir "CPUs par tâche"   "$DEFAULT_CPUS";      CPUS="$_SAISIE"
    saisir "Mémoire (ex: 8G)" "$DEFAULT_MEM";       MEM="$_SAISIE"
    saisir "Durée max"        "$DEFAULT_TIME";       TIME_LIMIT="$_SAISIE"
fi

# Valeurs par defaut pour ce qui n'a pas ete positionne
: "${PARTITION:=$DEFAULT_PARTITION}"; : "${NODES:=$DEFAULT_NODES}"
: "${NTASKS:=$DEFAULT_NTASKS}"; : "${CPUS:=$DEFAULT_CPUS}"
: "${MEM:=$DEFAULT_MEM}"; : "${TIME_LIMIT:=$DEFAULT_TIME}"

# ┌─────────────────────────────────────────────────────────────
# │ ÉTAPE 5 — Options
# └─────────────────────────────────────────────────────────────
_ETAPE_COURANTE=4
afficher_progression
section "Options"

menu_cases "Options :" \
    "Suivre le job en temps réel" \
    "Conserver le scratch après le calcul" \
    "Dry-run — afficher sans soumettre" \
    "Désactiver la validation du .comm"

for idx in "${_MENU_ITEMS[@]}"; do
    case "$idx" in 0) FOLLOW=1 ;; 1) KEEP_SCRATCH=1 ;; 2) DRY_RUN=1 ;; 3) NO_VALIDATE=1 ;; esac
done

# ┌─────────────────────────────────────────────────────────────
# │ ÉTAPE 6 — Récapitulatif et confirmation
# └─────────────────────────────────────────────────────────────
_ETAPE_COURANTE=5
afficher_progression
section "Récapitulatif"

echo "" >/dev/tty
printf "    ${BOLD}%-14s${NC} %s\n" "Dossier"   "$STUDY_DIR"                      >/dev/tty
printf "    ${BOLD}%-14s${NC} %s\n" ".comm"      "$(basename "$COMM")"             >/dev/tty
[ -n "$MED" ]  && printf "    ${BOLD}%-14s${NC} %s\n" ".med"  "$(basename "$MED")" >/dev/tty
[ -n "$MAIL" ] && printf "    ${BOLD}%-14s${NC} %s\n" ".mail" "$(basename "$MAIL")">/dev/tty
echo -e "    ${DIM}──────────────────────────────────────${NC}"                     >/dev/tty
printf "    ${BOLD}%-14s${NC} %s\n" "Partition"  "$PARTITION"                       >/dev/tty
printf "    ${BOLD}%-14s${NC} %s\n" "Nœuds"      "$NODES"                           >/dev/tty
printf "    ${BOLD}%-14s${NC} %s\n" "Tâches MPI" "$NTASKS"                          >/dev/tty
printf "    ${BOLD}%-14s${NC} %s\n" "CPUs/tâche" "$CPUS"                            >/dev/tty
printf "    ${BOLD}%-14s${NC} %s\n" "Mémoire"    "$MEM"                             >/dev/tty
printf "    ${BOLD}%-14s${NC} %s\n" "Durée max"  "$TIME_LIMIT"                      >/dev/tty
if [ -n "$RESULTS" ]; then
    echo -e "    ${DIM}──────────────────────────────────────${NC}"                  >/dev/tty
    printf "    ${BOLD}%-14s${NC} %s\n" "Sorties"    "$RESULTS"                     >/dev/tty
fi
local_opts_str=""
[ "$FOLLOW"       = "1" ] && local_opts_str+="follow "
[ "$KEEP_SCRATCH" = "1" ] && local_opts_str+="keep-scratch "
[ "$DRY_RUN"      = "1" ] && local_opts_str+="dry-run "
[ "$NO_VALIDATE"  = "1" ] && local_opts_str+="no-validate "
if [ -n "$local_opts_str" ]; then
    echo -e "    ${DIM}──────────────────────────────────────${NC}"                  >/dev/tty
    printf "    ${BOLD}%-14s${NC} %s\n" "Options"    "$local_opts_str"               >/dev/tty
fi
echo "" >/dev/tty

menu_fleches "Confirmer la soumission ?" "✅  Soumettre le calcul" "❌  Annuler"
[ "$_MENU_IDX" -ne 0 ] && { warn "Annulé."; exit 0; }

# ══════════════════════════════════════════════════════════════
#  EXÉCUTION — set -euo pipefail à partir d'ici
# ══════════════════════════════════════════════════════════════

set -euo pipefail

# ── Validation du .comm ──────────────────────────────────────
if [ "$NO_VALIDATE" -eq 0 ]; then
    if ! _validate_comm "$COMM" "$STUDY_DIR" "$MED" "$MAIL" "$RESULTS"; then
        err "Validation du .comm echouee — corrigez les erreurs ci-dessus"
        exit 1
    fi
fi

# ── Preparation du scratch ────────────────────────────────────
section "Préparation du scratch"
SCRATCH="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_$(date +%s)_$$"
mkdir -p "$SCRATCH"
ok "Scratch : $SCRATCH" >/dev/tty

cp "$COMM" "$SCRATCH/"
[ -n "$MED" ]  && cp "$MED" "$SCRATCH/"
[ -n "$MAIL" ] && cp "$MAIL" "$SCRATCH/"

shopt -s nullglob
for f in "$STUDY_DIR"/*.py "$STUDY_DIR"/*.dat "$STUDY_DIR"/*.para \
         "$STUDY_DIR"/*.include "$STUDY_DIR"/*.mfront; do
    cp "$f" "$SCRATCH/"
done
shopt -u nullglob

# ── Copie de la base (POURSUITE) ──────────────────────────────
_BASE_SRC="${BASE_DIR:-}"
if [ -z "$_BASE_SRC" ]; then
    shopt -s nullglob
    _base_check=("$STUDY_DIR"/glob.* "$STUDY_DIR"/pick.* "$STUDY_DIR"/vola.*)
    shopt -u nullglob
    [ ${#_base_check[@]} -gt 0 ] && _BASE_SRC="$STUDY_DIR"
fi
if [ -n "$_BASE_SRC" ]; then
    [ -d "$_BASE_SRC" ] || { err "Dossier base introuvable : $_BASE_SRC"; exit 1; }
    shopt -s nullglob
    for f in "$_BASE_SRC"/glob.* "$_BASE_SRC"/pick.* "$_BASE_SRC"/vola.*; do
        cp -a "$f" "$SCRATCH/"
    done
    shopt -u nullglob
    ok "Base : $_BASE_SRC → scratch" >/dev/tty
fi

# ── Conversion memoire / duree ────────────────────────────────
MEM_MB=$(echo "$MEM" | awk '
    tolower($0) ~ /g$/ { gsub(/[gGiI]/,""); print int($0*1024); next }
    tolower($0) ~ /m$/ { gsub(/[mMiI]/,""); print int($0);      next }
    /^[0-9]+$/          { print int($0); next }
    { print -1 }')
[ "$MEM_MB" -le 0 ] && { err "Memoire invalide : $MEM"; exit 1; }
ASTER_MEM=$(( MEM_MB - 512 ))
if [ "$ASTER_MEM" -lt 512 ]; then
    ASTER_MEM=512
    warn "Mémoire très limitée : Aster fixé au minimum (512 MB)"
elif [ "$ASTER_MEM" -lt 1024 ]; then
    warn "Mémoire Aster faible (${ASTER_MEM} MB) — risque pour les gros modèles"
fi

TIME_SEC=$(echo "$TIME_LIMIT" | awk -F'[-:]' '
    NF==4 {print $1*86400+$2*3600+$3*60+$4; next}
    NF==3 {print $1*3600+$2*60+$3;          next}
    NF==2 {print $1*60+$2;                  next}
    {print $1*60}')

# ── Generation du .export ─────────────────────────────────────
section "Génération du .export"
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

ok "Export : $EXPORT" >/dev/tty
while IFS= read -r line; do info "  $line" >/dev/tty; done < "$EXPORT"

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

        if ! squeue -j "$job" -h &>/dev/null; then
            if [ -f "$logfile" ]; then
                cat "$logfile"
            else
                warn "Fichier log introuvable : $logfile"
            fi
            state=""
        else
            [ -f "$logfile" ] || warn "Fichier log introuvable : $logfile"
            tail -f "$logfile" &
            local TAIL_PID=$!
            # shellcheck disable=SC2064
            trap "kill $TAIL_PID 2>/dev/null; echo ''; info 'Detache — job $job toujours en cours'; exit 0" INT
            while squeue -j "$job" -h &>/dev/null; do sleep 5; done
            sleep 2; kill $TAIL_PID 2>/dev/null; wait $TAIL_PID 2>/dev/null; trap - INT
        fi
    fi

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
section "Soumission Slurm"

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

echo "" >/dev/tty
echo -e "  ${GREEN}${BOLD}✔  Job ${JOB} soumis avec succès${NC}" >/dev/tty
echo "" >/dev/tty
echo -e "  ${DIM}Commandes utiles :${NC}" >/dev/tty
echo "    squeue -j $JOB              # Etat du job" >/dev/tty
echo "    tail -f ${STUDY_DIR}/aster_run_${JOB}.out" >/dev/tty
echo "    scancel $JOB                # Annuler" >/dev/tty
echo "    ls ${STUDY_DIR}/run_${JOB}/ # Resultats" >/dev/tty
echo "" >/dev/tty

[ "$FOLLOW" = "1" ] && _follow_job "$JOB" "${STUDY_DIR}/aster_run_${JOB}.out"
