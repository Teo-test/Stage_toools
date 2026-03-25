#!/bin/bash
#===============================================================================
#  run_aster.sh — Soumission de calculs Code_Aster via Slurm
#===============================================================================
#
#  Usage :  bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]
#
#  Ce script fonctionne en deux phases dans un seul fichier :
#
#    Phase 1 (noeud login)  : detecte les fichiers, prepare le scratch,
#                             genere le .export, soumet via sbatch.
#
#    Phase 2 (noeud calcul) : charge Code_Aster, lance le calcul,
#                             rapatrie les resultats, nettoie le scratch.
#
#  Notes sur run_aster et la gestion de la base :
#    - run_aster cree son propre repertoire de travail temporaire interne.
#    - Quand on declare "F base /chemin/dossier D 0", run_aster copie le
#      DOSSIER dans son rep de travail, puis deplace tout son contenu
#      (glob.*, pick.*, vola.*) a la racine. C'est pour cela que -B
#      prend un dossier, pas un fichier.
#    - run_aster gere MPI en interne : ne PAS l'appeler via srun.
#
#  Auteur  : Teo LEROY
#  Version : 9.0
#===============================================================================

# ══════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════

ASTER_ROOT="${ASTER_ROOT:-/opt/code_aster}"
ASTER_MODULE="${ASTER_MODULE:-code_aster}"
SCRATCH_BASE="${SCRATCH_BASE:-/scratch}"

DEFAULT_PARTITION="court"
DEFAULT_NODES=1
DEFAULT_NTASKS=1
DEFAULT_CPUS=1
DEFAULT_MEM="5G"
DEFAULT_TIME="05:00:00"

PRESET_COURT_PARTITION="court"  ; PRESET_COURT_NTASKS=1  ; PRESET_COURT_MEM="2G"  ; PRESET_COURT_TIME="05:00:00"
PRESET_MOYEN_PARTITION="moyen"  ; PRESET_MOYEN_NTASKS=1  ; PRESET_MOYEN_MEM="8G"  ; PRESET_MOYEN_TIME="03-00:00:00"
PRESET_LONG_PARTITION="long"    ; PRESET_LONG_NTASKS=1   ; PRESET_LONG_MEM="32G"  ; PRESET_LONG_TIME="30-00:00:00"

# ══════════════════════════════════════════
#  FONCTIONS D'AFFICHAGE
# ══════════════════════════════════════════

info() { echo -e "\033[0;34m[INFO]\033[0m  $*"; }
ok()   { echo -e "\033[0;32m[ OK ]\033[0m  $*"; }
warn() { echo -e "\033[1;33m[WARN]\033[0m  $*"; }
err()  { echo -e "\033[0;31m[ ERR]\033[0m  $*" >&2; }
log()  { echo "[$(date +%H:%M:%S)] $*"; }
header() {
    echo ""
    echo "========================================================"
    echo "  $*"
    echo "========================================================"
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

POURSUITE
  --save-base           Sauvegarder la base (glob/pick/vola) apres le calcul
  -B, --base DOSSIER    Dossier contenant glob.*/pick.*/vola.* d'un calcul
                        precedent (pour POURSUITE)

  Exemples :
    bash run_aster.sh --save-base ~/thermo/            # sauver la base
    bash run_aster.sh -B ~/thermo/latest/base ~/meca/  # reprendre la base

RESULTATS SUPPLEMENTAIRES
  -R, --results LIST    Format "type:unite,..." (ex: "rmed:81,csv:38")

RESSOURCES SLURM
  -P, --preset  NOM     court, moyen ou long
  -p, --partition NOM   Partition Slurm
  -n, --nodes N         Nombre de noeuds
  -t, --ntasks N        Taches MPI
  -c, --cpus N          CPUs par tache
  -m, --mem MEM         Memoire (ex: 8G)
  -T, --time H:M:S      Duree max

OPTIONS
  -q, --quiet           Sortie minimale
      --keep-scratch    Ne pas supprimer le scratch
      --dry-run         Afficher sans lancer
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

    collect_results() {
        [ "$ALREADY_COLLECTED" -eq 1 ] && return
        ALREADY_COLLECTED=1

        header "RAPATRIEMENT"

        local dest="${__STUDY_DIR}/run_${SLURM_JOB_ID}"
        mkdir -p "$dest" || { log "!! Impossible de creer $dest"; return; }

        local count=0

        # Resultats classiques
        for ext in mess resu med csv table dat pos rmed txt vtu vtk py; do
            for f in "${__SCRATCH}"/*."${ext}"; do
                [ -f "$f" ] && [ -s "$f" ] && cp -v "$f" "$dest/" && (( count++ ))
            done
        done 2>/dev/null

        # REPE_OUT
        [ -d "${__SCRATCH}/REPE_OUT" ] && cp -rv "${__SCRATCH}/REPE_OUT" "$dest/" && (( count++ )) 2>/dev/null

        # Base (glob/pick/vola) si --save-base
        if [ "${__SAVE_BASE:-0}" = "1" ]; then
            local base_dest="${dest}/base"
            mkdir -p "$base_dest"
            local bc=0
            for f in "${__SCRATCH}"/glob.* "${__SCRATCH}"/pick.* "${__SCRATCH}"/vola.*; do
                [ -f "$f" ] && [ -s "$f" ] && cp -v "$f" "$base_dest/" && (( bc++ ))
            done 2>/dev/null
            if [ "$bc" -gt 0 ]; then
                log "$bc fichier(s) base sauves dans $base_dest"
                (( count += bc ))
            else
                log "!! Aucun fichier base trouve (glob/pick/vola)"
                log "   Contenu du scratch :"
                ls -la "${__SCRATCH}/" 2>/dev/null
            fi
        fi

        # Lien latest
        rm -f "${__STUDY_DIR}/latest" 2>/dev/null
        ln -s "run_${SLURM_JOB_ID}" "${__STUDY_DIR}/latest" 2>/dev/null

        log "$count fichier(s) rapatrie(s) -> $dest"

        # Nettoyage
        if [ "${__KEEP_SCRATCH:-0}" != "1" ]; then
            rm -rf "$__SCRATCH" 2>/dev/null && log "Scratch supprime"
        else
            log "Scratch conserve : $__SCRATCH"
        fi
    }

    trap collect_results EXIT
    trap 'collect_results; exit 143' SIGTERM

    # --- Demarrage ---
    header "CODE_ASTER — $(date)"
    log "Job       : $SLURM_JOB_ID"
    log "Noeud     : $SLURM_NODELIST"
    log "Scratch   : $__SCRATCH"
    log "Save base : ${__SAVE_BASE:-0}"
    log "Base in   : ${__BASE_DIR:-aucune}"

    # --- Module ---
    if command -v module &>/dev/null && [ -n "${__MODULE:-}" ]; then
        module load "$__MODULE" 2>&1 && log "Module '$__MODULE' charge" \
                                     || warn "Module '$__MODULE' echec"
    fi

    # --- Executable ---
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

    # --- Debug : contenu scratch + export ---
    header "VERIFICATION"
    log "Contenu scratch :"
    ls -la "$__SCRATCH/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done
    if [ -d "${__SCRATCH}/base_in" ]; then
        log "Contenu base_in/ :"
        ls -la "${__SCRATCH}/base_in/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done
    fi
    log ""
    log "Contenu .export :"
    cat "$__EXPORT" 2>/dev/null | while IFS= read -r l; do log "  $l"; done

    # --- Calcul ---
    header "CALCUL"
    log "Lancement : $(date)"
    RC=0
    set +e
    "$EXE" "$__EXPORT"
    RC=$?
    set -e
    log "Termine : $(date) — code retour $RC"

    # --- Diagnostic .mess ---
    header "DIAGNOSTIC"
    MESS="${__SCRATCH}/${__STUDY_NAME}.mess"
    if [ -f "$MESS" ]; then
        NA=$(grep -c "<A>" "$MESS" 2>/dev/null || true)
        NF=$(grep -c "<F>" "$MESS" 2>/dev/null || true)
        NS=$(grep -c "<S>" "$MESS" 2>/dev/null || true)
        log "Alarmes <A>:$NA  Fatales <F>:$NF  Exceptions <S>:$NS"
        [ "$NF" -gt 0 ] && { grep -B2 -A5 "<F>" "$MESS" | head -20; }
        [ "$NS" -gt 0 ] && [ "$NF" -eq 0 ] && { grep -B2 -A5 "<S>" "$MESS" | head -20; }
    else
        log "!! Pas de .mess"
        ls -la "$__SCRATCH/" 2>/dev/null
    fi

    # --- Contenu scratch apres calcul (pour debug) ---
    log ""
    log "Contenu scratch apres calcul :"
    ls -la "$__SCRATCH/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done

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

set -euo pipefail

# --- Arguments ---
STUDY_DIR="."
COMM="" ; MED="" ; MAIL="" ; BASE_DIR=""
PRESET="" ; PARTITION="" ; NODES="" ; NTASKS="" ; CPUS="" ; MEM="" ; TIME_LIMIT=""
QUIET=false ; RESULTS="" ; KEEP_SCRATCH=0 ; DRY_RUN=0 ; DEBUG=0 ; SAVE_BASE=0

while [ $# -gt 0 ]; do
    case "$1" in
        -C|--comm)      COMM="$2";         shift 2 ;;
        -M|--med)       MED="$2";          shift 2 ;;
        -A|--mail)      MAIL="$2";         shift 2 ;;
        -B|--base)      BASE_DIR="$2";     shift 2 ;;
        -R|--results)   RESULTS="$2";      shift 2 ;;
        -P|--preset)    PRESET="$2";       shift 2 ;;
        -p|--partition) PARTITION="$2";    shift 2 ;;
        -n|--nodes)     NODES="$2";        shift 2 ;;
        -t|--ntasks)    NTASKS="$2";       shift 2 ;;
        -c|--cpus)      CPUS="$2";         shift 2 ;;
        -m|--mem)       MEM="$2";          shift 2 ;;
        -T|--time)      TIME_LIMIT="$2";   shift 2 ;;
        -q|--quiet)     QUIET=true;        shift ;;
        --save-base)    SAVE_BASE=1;       shift ;;
        --keep-scratch) KEEP_SCRATCH=1;    shift ;;
        --dry-run)      DRY_RUN=1;         shift ;;
        --debug)        DEBUG=1;           shift ;;
        -h|--help)      usage ;;
        -*)             err "Option inconnue : $1"; usage ;;
        *)              STUDY_DIR="$1";    shift ;;
    esac
done

# --- Presets ---
if [ -n "$PRESET" ]; then
    case "${PRESET,,}" in
        court|short)  : "${PARTITION:=$PRESET_COURT_PARTITION}"; : "${NTASKS:=$PRESET_COURT_NTASKS}"; : "${MEM:=$PRESET_COURT_MEM}"; : "${TIME_LIMIT:=$PRESET_COURT_TIME}" ;;
        moyen|medium) : "${PARTITION:=$PRESET_MOYEN_PARTITION}"; : "${NTASKS:=$PRESET_MOYEN_NTASKS}"; : "${MEM:=$PRESET_MOYEN_MEM}"; : "${TIME_LIMIT:=$PRESET_MOYEN_TIME}" ;;
        long)         : "${PARTITION:=$PRESET_LONG_PARTITION}";  : "${NTASKS:=$PRESET_LONG_NTASKS}";  : "${MEM:=$PRESET_LONG_MEM}";  : "${TIME_LIMIT:=$PRESET_LONG_TIME}" ;;
        *) err "Preset inconnu : $PRESET"; exit 1 ;;
    esac
    $QUIET || info "Preset : $PRESET"
fi
: "${PARTITION:=$DEFAULT_PARTITION}"; : "${NODES:=$DEFAULT_NODES}"
: "${NTASKS:=$DEFAULT_NTASKS}"; : "${CPUS:=$DEFAULT_CPUS}"
: "${MEM:=$DEFAULT_MEM}"; : "${TIME_LIMIT:=$DEFAULT_TIME}"

# --- Detection des fichiers ---
$QUIET || info "=== Detection ==="

STUDY_DIR="$(realpath "$STUDY_DIR")"
STUDY_NAME="$(basename "$STUDY_DIR")"
[ -d "$STUDY_DIR" ] || { err "Dossier introuvable : $STUDY_DIR"; exit 1; }

# .comm
if [ -z "$COMM" ]; then
    shopt -s nullglob; arr=("$STUDY_DIR"/*.comm); shopt -u nullglob
    [ ${#arr[@]} -eq 0 ] && { err "Aucun .comm dans $STUDY_DIR"; exit 1; }
    [ ${#arr[@]} -gt 1 ] && warn "Plusieurs .comm, utilisation du premier"
    COMM="${arr[0]}"
fi
COMM="$(realpath "$COMM")"
$QUIET || ok "Comm : $COMM"

# .med
if [ -z "$MED" ]; then
    shopt -s nullglob; arr=("$STUDY_DIR"/*.med); shopt -u nullglob
    [ ${#arr[@]} -ge 1 ] && MED="${arr[0]}"
    [ ${#arr[@]} -gt 1 ] && warn "Plusieurs .med, utilisation du premier"
fi
[ -n "$MED" ] && { MED="$(realpath "$MED")"; $QUIET || ok "Med  : $MED"; }

# .mail
if [ -z "$MAIL" ]; then
    shopt -s nullglob; arr=("$STUDY_DIR"/*.mail); shopt -u nullglob
    [ ${#arr[@]} -ge 1 ] && MAIL="${arr[0]}"
fi
[ -n "$MAIL" ] && { MAIL="$(realpath "$MAIL")"; $QUIET || ok "Mail : $MAIL"; }

# Base de poursuite
if [ -n "$BASE_DIR" ]; then
    BASE_DIR="$(realpath "$BASE_DIR")"
    [ -d "$BASE_DIR" ] || { err "-B doit etre un dossier : $BASE_DIR"; exit 1; }
    [ -f "$BASE_DIR/glob.1" ] || {
        err "Pas de glob.1 dans $BASE_DIR"
        err "Contenu :"; ls "$BASE_DIR/" 2>/dev/null | while read -r l; do err "  $l"; done
        err "Avez-vous utilise --save-base sur le calcul precedent ?"
        exit 1
    }
    $QUIET || ok "Base : $BASE_DIR"
fi

# --- Scratch ---
$QUIET || info "=== Scratch ==="
SCRATCH="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_$(date +%s)_$$"
mkdir -p "$SCRATCH"
$QUIET || ok "Scratch : $SCRATCH"

# Copie des fichiers
cp "$COMM" "$SCRATCH/"
[ -n "$MED" ]  && cp "$MED" "$SCRATCH/"
[ -n "$MAIL" ] && cp "$MAIL" "$SCRATCH/"

# Base de poursuite : copier dans un sous-dossier base_in/
# run_aster copie le dossier puis deplace son contenu a la racine
# de son repertoire de travail interne (cf. run_aster/run.py copy_datafiles)
if [ -n "$BASE_DIR" ]; then
    mkdir -p "${SCRATCH}/base_in"
    cp "$BASE_DIR"/glob.* "${SCRATCH}/base_in/" 2>/dev/null || true
    cp "$BASE_DIR"/pick.* "${SCRATCH}/base_in/" 2>/dev/null || true
    cp "$BASE_DIR"/vola.* "${SCRATCH}/base_in/" 2>/dev/null || true
    $QUIET || ok "Base copiee dans scratch/base_in/ ($(ls "${SCRATCH}/base_in/" | wc -l) fichiers)"
fi

# Fichiers annexes
shopt -s nullglob
for f in "$STUDY_DIR"/*.py "$STUDY_DIR"/*.dat "$STUDY_DIR"/*.para \
         "$STUDY_DIR"/*.include "$STUDY_DIR"/*.mfront; do
    cp "$f" "$SCRATCH/"
done
shopt -u nullglob

# --- Memoire ---
MEM_MB=$(echo "$MEM" | awk '
    tolower($0) ~ /g$/ { gsub(/[gGiI]/,""); print int($0*1024); next }
    tolower($0) ~ /m$/ { gsub(/[mMiI]/,""); print int($0);      next }
    /^[0-9]+$/          { print int($0); next }
    { print -1 }')
[ "$MEM_MB" -le 0 ] 2>/dev/null && { err "Memoire invalide : $MEM"; exit 1; }
ASTER_MEM=$(( MEM_MB - 512 ))
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512

# --- Temps ---
TIME_SEC=$(echo "$TIME_LIMIT" | awk -F'[-:]' '
    NF==4 {print $1*86400+$2*3600+$3*60+$4; next}
    NF==3 {print $1*3600+$2*60+$3; next}
    NF==2 {print $1*60+$2; next}
    {print $1*60}')

# --- .export ---
$QUIET || info "=== Export ==="
EXPORT="${SCRATCH}/${STUDY_NAME}.export"
{
    echo "P time_limit $TIME_SEC"
    echo "P memory_limit $ASTER_MEM"
    echo "P ncpus $NTASKS"

    # Entrees
    echo "F comm ${SCRATCH}/$(basename "$COMM") D 1"
    [ -n "$MED" ]  && echo "F mmed ${SCRATCH}/$(basename "$MED") D 20"
    [ -n "$MAIL" ] && echo "F mail ${SCRATCH}/$(basename "$MAIL") D 20"

    # Base en entree : dossier contenant glob/pick/vola
    [ -n "$BASE_DIR" ] && echo "F base ${SCRATCH}/base_in D 0"

    # Base en sortie
    [ "$SAVE_BASE" = "1" ] && echo "F base ${SCRATCH}/base_out R 0"

    # Sorties standard
    echo "F mess ${SCRATCH}/${STUDY_NAME}.mess R 6"
    echo "F resu ${SCRATCH}/${STUDY_NAME}.resu R 8"
    echo "F rmed ${SCRATCH}/${STUDY_NAME}_resu.med R 80"

    # Sorties supplementaires
    if [ -n "$RESULTS" ]; then
        IFS=',' read -ra ITEMS <<< "$RESULTS"
        for item in "${ITEMS[@]}"; do
            TYPE="${item%%:*}"; UNIT="${item##*:}"
            case "$TYPE" in
                rmed) EXT="med" ;; resu) EXT="resu" ;; mess) EXT="mess" ;;
                csv) EXT="csv" ;; table) EXT="table" ;; dat) EXT="dat" ;;
                pos) EXT="pos" ;; *) EXT="$TYPE" ;;
            esac
            echo "F ${TYPE} ${SCRATCH}/${STUDY_NAME}_u${UNIT}.${EXT} R ${UNIT}"
        done
    fi

    echo "R ${SCRATCH}/REPE_OUT R 0"

} > "$EXPORT"

if ! $QUIET; then
    ok "Export : $EXPORT"
    while IFS= read -r line; do info "  $line"; done < "$EXPORT"
fi

# --- Affichage ---
if ! $QUIET; then
    info "=== Ressources ==="
    info "Partition : $PARTITION | Noeuds : $NODES | Taches : $NTASKS | CPUs : $CPUS"
    info "Memoire   : $MEM (${ASTER_MEM}MB pour Aster) | Duree : $TIME_LIMIT"
    [ "$SAVE_BASE" = "1" ]    && info "Save base : oui -> run_JOBID/base/"
    [ -n "$BASE_DIR" ]        && info "Base in   : $BASE_DIR"
    [ "$KEEP_SCRATCH" = "1" ] && info "Scratch   : conserve"
fi

# --- Soumission ---
$QUIET || info "=== Soumission ==="

SELF="$(realpath "$0")"

VARS="ALL"
VARS+=",__RUN_PHASE=EXEC"
VARS+=",__STUDY_DIR=${STUDY_DIR}"
VARS+=",__STUDY_NAME=${STUDY_NAME}"
VARS+=",__SCRATCH=${SCRATCH}"
VARS+=",__EXPORT=${EXPORT}"
VARS+=",__ASTER_ROOT=${ASTER_ROOT}"
VARS+=",__MODULE=${ASTER_MODULE}"
VARS+=",__KEEP_SCRATCH=${KEEP_SCRATCH}"
VARS+=",__DEBUG=${DEBUG}"
VARS+=",__SAVE_BASE=${SAVE_BASE}"
VARS+=",__BASE_DIR=${BASE_DIR}"

CMD=(sbatch --parsable
    --job-name="aster_${STUDY_NAME}"
    --partition="$PARTITION"
    --nodes="$NODES"
    --ntasks="$NTASKS"
    --cpus-per-task="$CPUS"
    --mem="$MEM"
    --time="$TIME_LIMIT"
    --output="${STUDY_DIR}/aster_%j.out"
    --error="${STUDY_DIR}/aster_%j.err"
    --export="$VARS"
    "$SELF"
)

if [ "$DRY_RUN" = "1" ]; then
    info "DRY RUN :"
    echo "  ${CMD[*]}"
    exit 0
fi

JOB=$("${CMD[@]}") || { err "sbatch a echoue"; exit 1; }
[ -z "$JOB" ] && { err "Job ID vide"; exit 1; }

if $QUIET; then
    echo "$JOB"
else
    ok "Job $JOB soumis"
    echo ""
    echo "  squeue -j $JOB"
    echo "  tail -f ${STUDY_DIR}/aster_${JOB}.out"
    echo "  scancel $JOB"
    echo "  ls ${STUDY_DIR}/run_${JOB}/"
    [ "$SAVE_BASE" = "1" ] && echo "  ls ${STUDY_DIR}/run_${JOB}/base/   # base pour -B"
    echo ""
fi
