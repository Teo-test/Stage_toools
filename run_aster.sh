#!/bin/bash
#===============================================================================
#  run_aster — Calcul Code_Aster via sbatch (fichier unique)
#===============================================================================
#
#  Usage :  bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]
#
#  Le script fonctionne en DEUX PHASES dans un seul fichier :
#
#    PHASE 1 (noeud login) : preparation, copie scratch, .export, sbatch
#    PHASE 2 (noeud de calcul) : chargement Aster, calcul, rapatriement
#
#  Auteur   : Teo LEROY
#  Version  : 8.3
#===============================================================================

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION — modifier ici selon l'installation
# ══════════════════════════════════════════════════════════════════════════════

ASTER_ROOT="${ASTER_ROOT:-/opt/code_aster}"
ASTER_MODULE="${ASTER_MODULE:-code_aster}"
SCRATCH_BASE="${SCRATCH_BASE:-/scratch}"

# ── Valeurs par defaut Slurm ─────────────────────────────────────────────────
DEFAULT_PARTITION="court"
DEFAULT_NODES=1
DEFAULT_NTASKS=1
DEFAULT_CPUS_PER_TASK=1
DEFAULT_MEM="5G"
DEFAULT_TIME="05:00:00"

# ── Prereglages ──────────────────────────────────────────────────────────────
PRESET_COURT_PARTITION="court"  ; PRESET_COURT_NTASKS=1 ; PRESET_COURT_MEM="2G"  ; PRESET_COURT_TIME="05:00:00"
PRESET_MOYEN_PARTITION="moyen"  ; PRESET_MOYEN_NTASKS=1 ; PRESET_MOYEN_MEM="8G"  ; PRESET_MOYEN_TIME="03-00:00:00"
PRESET_LONG_PARTITION="long"    ; PRESET_LONG_NTASKS=1  ; PRESET_LONG_MEM="32G"  ; PRESET_LONG_TIME="30-00:00:00"

# ══════════════════════════════════════════════════════════════════════════════
#  AFFICHAGE
# ══════════════════════════════════════════════════════════════════════════════
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()      { echo -e "${GREEN}[ OK ]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()     { echo -e "${RED}[ ERR]${NC}  $*" >&2; }
section() { echo -e "\n${BOLD}${CYAN}> $*${NC}"; echo -e "${CYAN}$(printf -- '-%.0s' {1..60})${NC}"; }
ts()      { date '+%H:%M:%S'; }
log()     { echo "[$(ts)] $*"; }
sep()     { echo ""; echo "======================================================"; echo "  $*"; echo "======================================================"; }

# ══════════════════════════════════════════════════════════════════════════════
#  AIDE
# ══════════════════════════════════════════════════════════════════════════════
usage() {
    cat <<EOF
USAGE
  bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]

  Lance un calcul Code_Aster via Slurm.
  Par defaut, DOSSIER_ETUDE = repertoire courant.
  Le scratch est automatiquement supprime apres rapatriement.

FICHIERS
  -C, --comm FILE      Fichier .comm  (auto-detecte si absent)
  -M, --med  FILE      Fichier .med   (auto-detecte si absent)
  -A, --mail FILE      Fichier .mail  (auto-detecte si absent)

POURSUITE / ENCHAINEMENT
  --save-base          Sauvegarder la base Aster (glob.*, pick.*, vola.*)
                       dans run_JOBID/base/ apres le calcul.
                       Indispensable si un calcul suivant utilise POURSUITE().

  -B, --base DOSSIER   Dossier contenant la base d'un calcul precedent
                       (glob.*, pick.*, vola.*) pour POURSUITE().
                       Typiquement : ~/etude_thermo/latest/base

  Workflow couplage thermo-meca :
    1. bash run_aster.sh --save-base -P moyen ~/etude_thermo/
    2. bash run_aster.sh -B ~/etude_thermo/latest/base -P moyen ~/etude_meca/

  Enchainer 3 calculs :
    1. bash run_aster.sh --save-base ~/etape1/
    2. bash run_aster.sh --save-base -B ~/etape1/latest/base ~/etape2/
    3. bash run_aster.sh -B ~/etape2/latest/base ~/etape3/

RESULTATS SUPPLEMENTAIRES
  -R, --results LIST   Unites additionnelles. Format : "type:unite,..."
                        Types : rmed, resu, mess, csv, table, dat, pos
                        Exemple : --results "rmed:81,csv:38"

RESSOURCES SLURM
  -p, --partition NOM  Partition        [defaut: ${DEFAULT_PARTITION}]
  -n, --nodes     N    Nombre de noeuds [defaut: ${DEFAULT_NODES}]
  -t, --ntasks    N    Taches MPI       [defaut: ${DEFAULT_NTASKS}]
  -c, --cpus      N    CPUs par tache   [defaut: ${DEFAULT_CPUS_PER_TASK}]
  -m, --mem       MEM  Memoire/noeud    [defaut: ${DEFAULT_MEM}]
  -T, --time  H:M:S    Duree max        [defaut: ${DEFAULT_TIME}]

PREREGLAGES
  -P, --preset NOM     court, moyen, long (surchargeable par options)

OPTIONS
  -q, --quiet          Sortie minimale (juste le job ID)
      --keep-scratch   Ne pas supprimer le scratch apres rapatriement
      --dry-run        Afficher la commande sbatch sans la lancer
      --debug          Activer set -x en phase d'execution
  -h, --help           Afficher cette aide
EOF
    exit 0
}

# ##############################################################################
# ##                                                                          ##
# ##   PHASE 2 : EXECUTION SUR LE NOEUD DE CALCUL                            ##
# ##                                                                          ##
# ##############################################################################

if [ "${__ASTER_PHASE:-}" = "RUN" ]; then

    set -uo pipefail
    [ "${__ASTER_DEBUG:-0}" = "1" ] && set -x

    __RAPATRIE_DONE=0

    _rsync_result() {
        local src="$1" dest="$2"
        if rsync -av "$src" "$dest/" > >(while read -r l; do log "$l"; done) 2>&1; then
            return 0
        else
            log "!! Echec rsync : $src -> $dest"
            return 1
        fi
    }

    rapatrier() {
        if [ "$__RAPATRIE_DONE" -eq 1 ]; then return; fi
        __RAPATRIE_DONE=1

        sep "RAPATRIEMENT DES RESULTATS"

        local dest="${__ASTER_STUDY_DIR}/run_${SLURM_JOB_ID}"
        local n=0

        if ! mkdir -p "$dest" 2>/dev/null; then
            log "!! Impossible de creer $dest"
            return
        fi
        log "Destination : $dest"

        # Rapatrier les fichiers de resultat par extension
        shopt -s nullglob
        for ext in mess resu med csv table dat pos rmed txt vtu vtk py; do
            for f in "${__ASTER_SCRATCH_DIR}"/*."${ext}"; do
                if [ -f "$f" ] && [ -s "$f" ]; then
                    _rsync_result "$f" "$dest" && (( n++ )) || true
                fi
            done
        done
        shopt -u nullglob

        # Rapatrier REPE_OUT si present
        if [ -d "${__ASTER_SCRATCH_DIR}/REPE_OUT" ]; then
            _rsync_result "${__ASTER_SCRATCH_DIR}/REPE_OUT" "$dest" && (( n++ )) || true
        fi

        # Rapatrier la base (glob.*, pick.*, vola.*) dans un sous-dossier base/
        # Uniquement si --save-base a ete demande
        if [ "${__ASTER_SAVE_BASE:-0}" = "1" ]; then
            local base_dest="${dest}/base"
            mkdir -p "$base_dest" 2>/dev/null
            shopt -s nullglob
            local base_count=0
            for f in "${__ASTER_SCRATCH_DIR}"/glob.* \
                     "${__ASTER_SCRATCH_DIR}"/pick.* \
                     "${__ASTER_SCRATCH_DIR}"/vola.*; do
                if [ -f "$f" ] && [ -s "$f" ]; then
                    _rsync_result "$f" "$base_dest" && (( base_count++ )) || true
                fi
            done
            shopt -u nullglob
            if [ "$base_count" -gt 0 ]; then
                log "OK $base_count fichier(s) base rapatrie(s) vers $base_dest"
                (( n += base_count ))
            else
                log "!! Aucun fichier base (glob.*, pick.*, vola.*) trouve dans le scratch"
                log "   Le calcul a peut-etre echoue avant de generer la base."
            fi
        fi

        if [ "$n" -eq 0 ]; then
            log "!! Aucun fichier resultat trouve"
            ls -la "${__ASTER_SCRATCH_DIR}/" 2>/dev/null | while IFS= read -r l; do log "   $l"; done
        else
            log "OK $n fichier(s) rapatrie(s) au total vers $dest"
        fi

        log ""
        log "Resultats dans : $dest"
        shopt -s nullglob
        for f in "$dest"/*; do
            log "  $(ls -lh "$f")"
        done
        shopt -u nullglob

        # Lien symbolique latest
        local latest_link="${__ASTER_STUDY_DIR}/latest"
        rm -f "$latest_link" 2>/dev/null
        ln -s "run_${SLURM_JOB_ID}" "$latest_link" 2>/dev/null && \
            log "OK Lien : latest -> run_${SLURM_JOB_ID}"

        # Nettoyage scratch
        if [ "${__ASTER_KEEP_SCRATCH:-0}" != "1" ]; then
            log ""
            log "Nettoyage du scratch..."
            rm -rf "$__ASTER_SCRATCH_DIR" 2>/dev/null && log "OK Scratch supprime" || \
                log "!! Echec suppression scratch"
        else
            log "Scratch conserve : $__ASTER_SCRATCH_DIR"
        fi
    }

    trap rapatrier EXIT
    trap 'rapatrier; exit 143' SIGTERM

    # ── Infos de demarrage ────────────────────────────────────────────────────
    sep "DEBUT CALCUL CODE_ASTER -- $(date)"
    log "Job ID         : $SLURM_JOB_ID"
    log "Etude          : $__ASTER_STUDY_NAME"
    log "Scratch        : $__ASTER_SCRATCH_DIR"
    log "Noeuds         : $SLURM_NODELIST"
    log "Taches MPI     : $SLURM_NTASKS"
    log "Memoire        : $__ASTER_MEM"
    log "Save base      : ${__ASTER_SAVE_BASE:-0}"
    log "Base poursuite : ${__ASTER_BASE_DIR:-aucune}"

    # ── Chargement de Code_Aster ──────────────────────────────────────────────
    sep "CHARGEMENT CODE_ASTER"

    ASTER_LOADED=0
    if command -v module &>/dev/null && [ -n "${__ASTER_MODULE:-}" ]; then
        if module load "${__ASTER_MODULE}" 2>/dev/null; then
            log "Module '${__ASTER_MODULE}' charge."
            ASTER_LOADED=1
        else
            warn "Module '${__ASTER_MODULE}' non disponible."
        fi
    fi

    ASTER_EXE=""
    for candidate in \
        "${__ASTER_ROOT}/bin/run_aster" \
        "${__ASTER_ROOT}/bin/as_run" \
        "$(command -v run_aster 2>/dev/null || true)" \
        "$(command -v as_run   2>/dev/null || true)"
    do
        [ -n "$candidate" ] && [ -x "$candidate" ] && { ASTER_EXE="$candidate"; break; }
    done

    if [ -z "$ASTER_EXE" ]; then
        err "Code_Aster introuvable."
        exit 1
    fi
    log "Executable : $ASTER_EXE"

    if [ "$ASTER_LOADED" -eq 0 ] && [ -n "${__ASTER_MODULE:-}" ]; then
        warn "Module non charge -- le binaire pourrait ne pas avoir le bon environnement."
    fi

    ASTER_VERSION=$("$ASTER_EXE" --version 2>&1 | head -1) || true
    [ -n "$ASTER_VERSION" ] && log "Version : $ASTER_VERSION"

    # ── Debug : contenu du scratch avant lancement ────────────────────────────
    sep "CONTENU SCRATCH"
    ls -la "$__ASTER_SCRATCH_DIR/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done
    log ""
    log "Contenu du .export :"
    cat "$__ASTER_EXPORT_FILE" 2>/dev/null | while IFS= read -r l; do log "  $l"; done

    # ── Lancement ─────────────────────────────────────────────────────────────
    sep "CALCUL EN COURS"
    log "Demarrage : $(date)"

    ASTER_RC=0
    set +e
    log "Commande : $ASTER_EXE $__ASTER_EXPORT_FILE"
    "$ASTER_EXE" "$__ASTER_EXPORT_FILE"
    ASTER_RC=$?
    set -e
    log "Execution terminee : $(date) -- code retour : $ASTER_RC"

    # ── Diagnostic .mess ──────────────────────────────────────────────────────
    sep "DIAGNOSTIC"
    MESS_PATH="${__ASTER_SCRATCH_DIR}/${__ASTER_STUDY_NAME}.mess"
    NB_ALARM=0; NB_FATAL=0; NB_EXCEP=0
    if [ -f "$MESS_PATH" ]; then
        NB_ALARM=$(grep -c "<A>" "$MESS_PATH" 2>/dev/null || true)
        NB_FATAL=$(grep -c "<F>" "$MESS_PATH" 2>/dev/null || true)
        NB_EXCEP=$(grep -c "<S>" "$MESS_PATH" 2>/dev/null || true)
        log "Alarmes <A> : $NB_ALARM | Fatales <F> : $NB_FATAL | Exceptions <S> : $NB_EXCEP"
        if [ "$NB_FATAL" -gt 0 ]; then
            log "--- Premiere erreur fatale ---"
            grep -B2 -A5 "<F>" "$MESS_PATH" | head -20 || true
            log "--- fin ---"
        fi
        if [ "$NB_EXCEP" -gt 0 ]; then
            log "--- Premiere exception ---"
            grep -B2 -A5 "<S>" "$MESS_PATH" | head -20 || true
            log "--- fin ---"
        fi
    else
        log "!! Fichier .mess non trouve"
        ls -la "${__ASTER_SCRATCH_DIR}/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done
    fi

    rapatrier

    # ── sacct ─────────────────────────────────────────────────────────────────
    sep "RESSOURCES UTILISEES"
    if command -v sacct &>/dev/null; then
        sacct -j "$SLURM_JOB_ID" \
              --format=JobID,JobName%20,Elapsed,CPUTime,MaxRSS,MaxVMSize,State,ExitCode \
              2>/dev/null | while IFS= read -r l; do log "$l"; done \
            || log "!! sacct non disponible"
    fi

    sep "RESUME FINAL"
    [ "$ASTER_RC" -eq 0 ] && log "Statut : SUCCES" || log "Statut : ECHEC (code $ASTER_RC)"
    log "Resultats : ${__ASTER_STUDY_DIR}/run_${SLURM_JOB_ID}"
    log "Fin : $(date)"

    exit $ASTER_RC
fi

# ##############################################################################
# ##                                                                          ##
# ##   PHASE 1 : PREPARATION SUR LE NOEUD LOGIN                              ##
# ##                                                                          ##
# ##############################################################################

set -euo pipefail

# ══════════════════════════════════════════════════════════════════════════════
#  PARSING DES ARGUMENTS
# ══════════════════════════════════════════════════════════════════════════════
STUDY_DIR="."
COMM_FILE=""
MED_FILE=""
MAIL_FILE=""
BASE_DIR=""
PRESET=""
PARTITION=""
NODES=""
NTASKS=""
CPUS=""
MEM=""
TIME_LIMIT=""
QUIET=false
RESULT_UNITS=""
OPT_KEEP_SCRATCH=0
OPT_DRY_RUN=0
OPT_DEBUG=0
OPT_SAVE_BASE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -C|--comm)          COMM_FILE="$2";       shift 2 ;;
        -M|--med)           MED_FILE="$2";        shift 2 ;;
        -A|--mail)          MAIL_FILE="$2";       shift 2 ;;
        -B|--base)          BASE_DIR="$2";        shift 2 ;;
        -R|--results)       RESULT_UNITS="$2";    shift 2 ;;
        -P|--preset)        PRESET="$2";          shift 2 ;;
        -p|--partition)     PARTITION="$2";       shift 2 ;;
        -n|--nodes)         NODES="$2";           shift 2 ;;
        -t|--ntasks)        NTASKS="$2";          shift 2 ;;
        -c|--cpus)          CPUS="$2";            shift 2 ;;
        -m|--mem)           MEM="$2";             shift 2 ;;
        -T|--time)          TIME_LIMIT="$2";      shift 2 ;;
        -q|--quiet)         QUIET=true;           shift ;;
        --save-base)        OPT_SAVE_BASE=1;      shift ;;
        --keep-scratch)     OPT_KEEP_SCRATCH=1;   shift ;;
        --dry-run)          OPT_DRY_RUN=1;        shift ;;
        --debug)            OPT_DEBUG=1;          shift ;;
        -h|--help)          usage ;;
        -*)                 err "Option inconnue : $1"; echo ""; usage ;;
        *)                  STUDY_DIR="$1";       shift ;;
    esac
done

# ── Prereglages ───────────────────────────────────────────────────────────────
if [ -n "$PRESET" ]; then
    case "${PRESET,,}" in
        court|short)
            : "${PARTITION:=$PRESET_COURT_PARTITION}"; : "${NTASKS:=$PRESET_COURT_NTASKS}"
            : "${MEM:=$PRESET_COURT_MEM}"; : "${TIME_LIMIT:=$PRESET_COURT_TIME}"
            $QUIET || info "Prereglage : court"
            ;;
        moyen|medium)
            : "${PARTITION:=$PRESET_MOYEN_PARTITION}"; : "${NTASKS:=$PRESET_MOYEN_NTASKS}"
            : "${MEM:=$PRESET_MOYEN_MEM}"; : "${TIME_LIMIT:=$PRESET_MOYEN_TIME}"
            $QUIET || info "Prereglage : moyen"
            ;;
        long)
            : "${PARTITION:=$PRESET_LONG_PARTITION}"; : "${NTASKS:=$PRESET_LONG_NTASKS}"
            : "${MEM:=$PRESET_LONG_MEM}"; : "${TIME_LIMIT:=$PRESET_LONG_TIME}"
            $QUIET || info "Prereglage : long"
            ;;
        *) err "Prereglage inconnu : '$PRESET'"; exit 1 ;;
    esac
fi

: "${PARTITION:=$DEFAULT_PARTITION}"
: "${NODES:=$DEFAULT_NODES}"
: "${NTASKS:=$DEFAULT_NTASKS}"
: "${CPUS:=$DEFAULT_CPUS_PER_TASK}"
: "${MEM:=$DEFAULT_MEM}"
: "${TIME_LIMIT:=$DEFAULT_TIME}"

# ══════════════════════════════════════════════════════════════════════════════
#  DETECTION DES FICHIERS
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Detection de l'etude"

STUDY_DIR="$(realpath "$STUDY_DIR")"
STUDY_NAME="$(basename "$STUDY_DIR")"
[ -d "$STUDY_DIR" ] || { err "Dossier introuvable : $STUDY_DIR"; exit 1; }
$QUIET || info "Dossier : $STUDY_DIR"
$QUIET || info "Etude   : $STUDY_NAME"

# .comm
if [ -z "$COMM_FILE" ]; then
    shopt -s nullglob; COMM_LIST=("$STUDY_DIR"/*.comm); shopt -u nullglob
    case ${#COMM_LIST[@]} in
        0) err "Aucun .comm dans $STUDY_DIR"; exit 1 ;;
        1) COMM_FILE="${COMM_LIST[0]}" ;;
        *) warn "Plusieurs .comm -- selection du premier"; COMM_FILE="${COMM_LIST[0]}" ;;
    esac
fi
COMM_FILE="$(realpath "$COMM_FILE")"
[ -f "$COMM_FILE" ] || { err ".comm introuvable : $COMM_FILE"; exit 1; }
$QUIET || ok "Comm : $COMM_FILE"

# .med
if [ -z "$MED_FILE" ]; then
    shopt -s nullglob; MED_LIST=("$STUDY_DIR"/*.med); shopt -u nullglob
    case ${#MED_LIST[@]} in
        0) : ;; 1) MED_FILE="${MED_LIST[0]}" ;;
        *) warn "Plusieurs .med -- selection du premier"; MED_FILE="${MED_LIST[0]}" ;;
    esac
fi
[ -n "$MED_FILE" ] && MED_FILE="$(realpath "$MED_FILE")"
[ -n "$MED_FILE" ] && { $QUIET || ok "Med  : $MED_FILE"; }

# .mail
if [ -z "$MAIL_FILE" ]; then
    shopt -s nullglob; MAIL_LIST=("$STUDY_DIR"/*.mail); shopt -u nullglob
    case ${#MAIL_LIST[@]} in
        0) : ;; 1) MAIL_FILE="${MAIL_LIST[0]}" ;;
        *) warn "Plusieurs .mail -- selection du premier"; MAIL_FILE="${MAIL_LIST[0]}" ;;
    esac
fi
[ -n "$MAIL_FILE" ] && MAIL_FILE="$(realpath "$MAIL_FILE")"
[ -n "$MAIL_FILE" ] && { $QUIET || ok "Mail : $MAIL_FILE"; }

[ -z "$MED_FILE" ] && [ -z "$MAIL_FILE" ] && warn "Aucun maillage detecte"

# Base de poursuite (-B dossier)
if [ -n "$BASE_DIR" ]; then
    BASE_DIR="$(realpath "$BASE_DIR")"
    [ -d "$BASE_DIR" ] || { err "Dossier base introuvable : $BASE_DIR"; exit 1; }
    if [ ! -f "$BASE_DIR/glob.1" ]; then
        err "Pas de glob.1 dans : $BASE_DIR"
        err "  Avez-vous lance le calcul precedent avec --save-base ?"
        err "  Contenu :"
        ls -la "$BASE_DIR/" 2>/dev/null | while IFS= read -r l; do err "    $l"; done
        exit 1
    fi
    $QUIET || ok "Base POURSUITE : $BASE_DIR"
    # Compter les fichiers base
    shopt -s nullglob
    BASE_FILES=("$BASE_DIR"/glob.* "$BASE_DIR"/pick.* "$BASE_DIR"/vola.*)
    shopt -u nullglob
    $QUIET || info "  ${#BASE_FILES[@]} fichier(s) base : $(ls "$BASE_DIR"/glob.* "$BASE_DIR"/pick.* "$BASE_DIR"/vola.* 2>/dev/null | xargs -n1 basename | tr '\n' ' ')"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  PREPARATION DU SCRATCH
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Preparation du scratch"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)_$$"
SCRATCH_DIR="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_${TIMESTAMP}"
mkdir -p "$SCRATCH_DIR" || { err "Impossible de creer : $SCRATCH_DIR"; exit 1; }
$QUIET || info "Scratch : $SCRATCH_DIR"

_copy_to_scratch() {
    local src="$1"
    rsync -a "$src" "$SCRATCH_DIR/" || { err "Echec copie : $src"; exit 1; }
    $QUIET || ok "Copie : $(basename "$src")"
}

_copy_to_scratch "$COMM_FILE"
[ -n "$MED_FILE"  ] && _copy_to_scratch "$MED_FILE"
[ -n "$MAIL_FILE" ] && _copy_to_scratch "$MAIL_FILE"

# Copier tous les fichiers base (glob.*, pick.*, vola.*) dans le scratch
# Ils doivent etre a la racine du scratch, pas dans un sous-dossier
if [ -n "$BASE_DIR" ]; then
    $QUIET || info "Copie des fichiers base dans le scratch..."
    shopt -s nullglob
    for f in "$BASE_DIR"/glob.* "$BASE_DIR"/pick.* "$BASE_DIR"/vola.*; do
        _copy_to_scratch "$f"
    done
    shopt -u nullglob
fi

# Fichiers annexes
shopt -s nullglob
for ext in py dat para include mfront; do
    for f in "$STUDY_DIR/"*."$ext"; do _copy_to_scratch "$f"; done
done
shopt -u nullglob

# ══════════════════════════════════════════════════════════════════════════════
#  MEMOIRE & TEMPS
# ══════════════════════════════════════════════════════════════════════════════
MEM_MB=$(echo "$MEM" | awk '
    tolower($0) ~ /^[0-9]+(\.[0-9]+)?g$/ { gsub(/[gGiI]/, ""); print int($0 * 1024); next }
    tolower($0) ~ /^[0-9]+(\.[0-9]+)?m$/ { gsub(/[mMiI]/, ""); print int($0);        next }
    /^[0-9]+$/                            { print int($0);                             next }
    { print -1 }
')
if [ "$MEM_MB" -le 0 ] 2>/dev/null; then
    err "Format memoire non reconnu : '$MEM'"; exit 1
fi
ASTER_MEM=$(( MEM_MB - 512 ))
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512

TIME_LIMIT_SEC=$(echo "$TIME_LIMIT" | awk -F'[-:]' '
    NF==4 {print $1*86400 + $2*3600 + $3*60 + $4; next}
    NF==3 {print $1*3600  + $2*60   + $3;         next}
    NF==2 {print $1*60    + $2;                   next}
           {print $1*60}')

# ══════════════════════════════════════════════════════════════════════════════
#  GENERATION DU FICHIER .EXPORT
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Generation du fichier .export"

COMM_BASENAME="$(basename "$COMM_FILE")"
MED_BASENAME="$([ -n "$MED_FILE"  ] && basename "$MED_FILE"  || echo "")"
MAIL_BASENAME="$([ -n "$MAIL_FILE" ] && basename "$MAIL_FILE" || echo "")"

EXPORT_FILE="${SCRATCH_DIR}/${STUDY_NAME}.export"
{
    echo "P actions make_etude"
    echo "P mode interactif"
    echo "P version stable"
    echo "P ncpus ${NTASKS}"
    echo "P memory_limit ${ASTER_MEM}"
    echo "P time_limit ${TIME_LIMIT_SEC}"
    echo ""

    # Fichiers d'entree
    echo "F comm ${SCRATCH_DIR}/${COMM_BASENAME}           D  1"
    [ -n "$MED_BASENAME"  ] && echo "F mmed ${SCRATCH_DIR}/${MED_BASENAME}            D 20"
    [ -n "$MAIL_BASENAME" ] && echo "F mail ${SCRATCH_DIR}/${MAIL_BASENAME}           D 20"

    # Base en ENTREE pour POURSUITE (-B)
    # Les fichiers glob.*, pick.*, vola.* sont a la racine du scratch
    [ -n "$BASE_DIR" ] && echo "F base ${SCRATCH_DIR}/glob.1 D 0"

    # Base en SORTIE (--save-base)
    # Code_Aster ecrit glob.*, pick.*, vola.* dans le scratch
    [ "$OPT_SAVE_BASE" = "1" ] && echo "F base ${SCRATCH_DIR}/glob.1 R 0"

    # Fichiers de sortie par defaut
    echo "F mess ${SCRATCH_DIR}/${STUDY_NAME}.mess         R  6"
    echo "F resu ${SCRATCH_DIR}/${STUDY_NAME}.resu         R  8"
    echo "F rmed ${SCRATCH_DIR}/${STUDY_NAME}_resu.med     R 80"

    # Resultats supplementaires (-R)
    if [ -n "$RESULT_UNITS" ]; then
        IFS=',' read -ra RU_LIST <<< "$RESULT_UNITS"
        for ru in "${RU_LIST[@]}"; do
            R_TYPE="${ru%%:*}"
            R_UNIT="${ru##*:}"
            case "$R_TYPE" in
                rmed)  R_EXT="med"   ;; resu)  R_EXT="resu"  ;;
                mess)  R_EXT="mess"  ;; csv)   R_EXT="csv"   ;;
                table) R_EXT="table" ;; dat)   R_EXT="dat"   ;;
                pos)   R_EXT="pos"   ;; *)     R_EXT="$R_TYPE" ;;
            esac
            echo "F ${R_TYPE} ${SCRATCH_DIR}/${STUDY_NAME}_u${R_UNIT}.${R_EXT}  R ${R_UNIT}"
        done
    fi

    # Repertoire de sortie
    echo "R ${SCRATCH_DIR}/REPE_OUT R 0"

} > "$EXPORT_FILE" || { err "Echec ecriture .export"; exit 1; }

if ! $QUIET; then
    ok "Export : $EXPORT_FILE"
    while IFS= read -r line; do info "  $line"; done < "$EXPORT_FILE"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  AFFICHAGE RESSOURCES
# ══════════════════════════════════════════════════════════════════════════════
if ! $QUIET; then
    section "Ressources Slurm"
    info "Partition  : $PARTITION"
    info "Noeuds     : $NODES"
    info "Taches MPI : $NTASKS"
    info "CPUs/tache : $CPUS"
    info "Memoire    : $MEM  (${ASTER_MEM} MB pour Code_Aster)"
    info "Duree max  : $TIME_LIMIT"
    info "Scratch    : $SCRATCH_DIR"
    [ "$OPT_SAVE_BASE" = "1" ]    && info "Save base  : OUI (glob.*/pick.*/vola.* -> run_JOBID/base/)"
    [ "$OPT_KEEP_SCRATCH" = "1" ] && info "Scratch    : conserve (--keep-scratch)"
    [ "$OPT_DEBUG" = "1" ]        && info "Debug      : set -x actif"
    [ -n "$RESULT_UNITS" ]        && info "Resultats+ : $RESULT_UNITS"
    [ -n "$BASE_DIR" ]            && info "Base       : $BASE_DIR (POURSUITE)"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  SOUMISSION SLURM
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Soumission Slurm"

SELF_SCRIPT="$(realpath "$0")"
SLURM_LOG_OUT="${STUDY_DIR}/aster_%j.out"
SLURM_LOG_ERR="${STUDY_DIR}/aster_%j.err"

EXPORT_VARS="ALL"
EXPORT_VARS+=",__ASTER_PHASE=RUN"
EXPORT_VARS+=",__ASTER_STUDY_DIR=${STUDY_DIR}"
EXPORT_VARS+=",__ASTER_STUDY_NAME=${STUDY_NAME}"
EXPORT_VARS+=",__ASTER_SCRATCH_DIR=${SCRATCH_DIR}"
EXPORT_VARS+=",__ASTER_EXPORT_FILE=${EXPORT_FILE}"
EXPORT_VARS+=",__ASTER_MEM=${MEM}"
EXPORT_VARS+=",__ASTER_ROOT=${ASTER_ROOT}"
EXPORT_VARS+=",__ASTER_MODULE=${ASTER_MODULE}"
EXPORT_VARS+=",__ASTER_KEEP_SCRATCH=${OPT_KEEP_SCRATCH}"
EXPORT_VARS+=",__ASTER_DEBUG=${OPT_DEBUG}"
EXPORT_VARS+=",__ASTER_SAVE_BASE=${OPT_SAVE_BASE}"
EXPORT_VARS+=",__ASTER_BASE_DIR=${BASE_DIR}"

SBATCH_CMD=(
    sbatch --parsable
    --job-name="aster_${STUDY_NAME}"
    --partition="${PARTITION}"
    --nodes="${NODES}"
    --ntasks="${NTASKS}"
    --cpus-per-task="${CPUS}"
    --mem="${MEM}"
    --time="${TIME_LIMIT}"
    --output="${SLURM_LOG_OUT}"
    --error="${SLURM_LOG_ERR}"
    --export="${EXPORT_VARS}"
    "$SELF_SCRIPT"
)

if [ "$OPT_DRY_RUN" = "1" ]; then
    section "DRY RUN -- commande sbatch (non lancee)"
    echo "  ${SBATCH_CMD[*]}"
    echo ""
    info "Export :"
    echo "$EXPORT_VARS" | tr ',' '\n' | while IFS= read -r v; do info "  $v"; done
    exit 0
fi

JOB_ID=$("${SBATCH_CMD[@]}") || { err "Echec sbatch"; exit 1; }
[ -z "$JOB_ID" ] && { err "Job ID vide"; exit 1; }

if $QUIET; then
    echo "$JOB_ID"
else
    ok "Job soumis : ${BOLD}${JOB_ID}${NC}"
    echo ""
    echo -e "  ${BOLD}Commandes utiles :${NC}"
    echo -e "  squeue -j ${JOB_ID}"
    echo -e "  tail -f ${STUDY_DIR}/aster_${JOB_ID}.out"
    echo -e "  scancel ${JOB_ID}"
    echo -e "  ls ${STUDY_DIR}/run_${JOB_ID}/"
    echo -e "  ls -l ${STUDY_DIR}/latest"
    [ -n "$BASE_DIR" ]            && echo -e "  # POURSUITE depuis : ${BASE_DIR}"
    [ "$OPT_SAVE_BASE" = "1" ]    && echo -e "  # Base sauvegardee dans : run_${JOB_ID}/base/"
    echo ""
fi
