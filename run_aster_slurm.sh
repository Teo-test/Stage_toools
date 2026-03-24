#!/bin/bash
# =============================================================================
#  run_aster_slurm — Calcul Code_Aster via Slurm (fichier unique)
# =============================================================================
#
#  Usage :
#    ./run_aster_slurm.sh [OPTIONS] [DOSSIER_ETUDE]
#    bash run_aster_slurm.sh [OPTIONS] [DOSSIER_ETUDE]
#
#  Le script prépare tout (scratch, .export) puis lance sbatch en interne.
#  Tu ne tapes JAMAIS sbatch toi-même.
#
#  Auteur   : généré pour localcluster
#  Version  : 3.2
# =============================================================================

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION — modifier ici selon l'installation
# ══════════════════════════════════════════════════════════════════════════════

ASTER_ROOT="${ASTER_ROOT:-/opt/code_aster}"
ASTER_MODULE="${ASTER_MODULE:-code_aster}"
SCRATCH_BASE="${SCRATCH_BASE:-/scratch}"

# ── Valeurs par défaut Slurm ─────────────────────────────────────────────────
DEFAULT_PARTITION="court"
DEFAULT_NODES=1
DEFAULT_NTASKS=4
DEFAULT_CPUS_PER_TASK=1
DEFAULT_MEM="4G"
DEFAULT_TIME="24:00:00"

# ── Préréglages (-P court | moyen | long) ────────────────────────────────────
PRESET_COURT_PARTITION="court"  ; PRESET_COURT_NTASKS=2 ; PRESET_COURT_MEM="2G"  ; PRESET_COURT_TIME="02:00:00"
PRESET_MOYEN_PARTITION="court"  ; PRESET_MOYEN_NTASKS=4 ; PRESET_MOYEN_MEM="8G"  ; PRESET_MOYEN_TIME="24:00:00"
PRESET_LONG_PARTITION="court"   ; PRESET_LONG_NTASKS=8  ; PRESET_LONG_MEM="32G"  ; PRESET_LONG_TIME="72:00:00"

# ══════════════════════════════════════════════════════════════════════════════
#  AFFICHAGE
# ══════════════════════════════════════════════════════════════════════════════
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()      { echo -e "${GREEN}[ OK ]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()     { echo -e "${RED}[ ERR]${NC}  $*" >&2; }
section() { echo -e "\n${BOLD}${CYAN}▶ $*${NC}"; echo -e "${CYAN}$(printf '─%.0s' {1..60})${NC}"; }
ts()      { date '+%H:%M:%S'; }
log()     { echo "[$(ts)] $*"; }
sep()     { echo ""; echo "══════════════════════════════════════════════════════"; echo "  $*"; echo "══════════════════════════════════════════════════════"; }

# ══════════════════════════════════════════════════════════════════════════════
#  AIDE
# ══════════════════════════════════════════════════════════════════════════════
usage() {
    cat <<EOF
${BOLD}USAGE${NC}
  ./run_aster_slurm.sh [OPTIONS] [DOSSIER_ETUDE]

  Prépare et soumet automatiquement un calcul Code_Aster via Slurm.
  Par défaut, DOSSIER_ETUDE = répertoire courant.

${BOLD}FICHIERS${NC}
  -C, --comm FILE      Fichier .comm  (auto-détecté si absent)
  -M, --med  FILE      Fichier .med   (auto-détecté si absent)
  -A, --mail FILE      Fichier .mail  (auto-détecté si absent)

${BOLD}RESSOURCES SLURM${NC}
  -p, --partition NOM  Partition        [défaut: ${DEFAULT_PARTITION}]
  -n, --nodes     N    Nombre de nœuds  [défaut: ${DEFAULT_NODES}]
  -t, --ntasks    N    Tâches MPI       [défaut: ${DEFAULT_NTASKS}]
  -c, --cpus      N    CPUs par tâche   [défaut: ${DEFAULT_CPUS_PER_TASK}]
  -m, --mem       MEM  Mémoire/nœud     [défaut: ${DEFAULT_MEM}]
  -T, --time  H:M:S    Durée max        [défaut: ${DEFAULT_TIME}]

${BOLD}PRÉRÉGLAGES${NC}
  -P, --preset NOM     court / moyen / long
                         court : ${PRESET_COURT_NTASKS} tâches, ${PRESET_COURT_MEM}, ${PRESET_COURT_TIME}
                         moyen : ${PRESET_MOYEN_NTASKS} tâches, ${PRESET_MOYEN_MEM}, ${PRESET_MOYEN_TIME}
                         long  : ${PRESET_LONG_NTASKS} tâches, ${PRESET_LONG_MEM}, ${PRESET_LONG_TIME}
                       Les options après -P surchargent le préréglage.

${BOLD}OPTIONS${NC}
  -q, --quiet          Sortie minimale
  -h, --help           Afficher cette aide

${BOLD}EXEMPLES${NC}
  ./run_aster_slurm.sh                             # Dossier courant, défauts
  ./run_aster_slurm.sh ~/calculs/poutre/           # Spécifier un dossier
  ./run_aster_slurm.sh -P court                    # Préréglage 2h
  ./run_aster_slurm.sh -P long                     # Préréglage 72h
  ./run_aster_slurm.sh -P moyen -t 8 -m 16G       # Moyen + surcharges
  ./run_aster_slurm.sh -C calcul.comm -M mesh.med  # Fichiers explicites
EOF
    exit 0
}

# ##############################################################################
# ##############################################################################
# ##                                                                          ##
# ##   PHASE 2 : EXÉCUTION SUR LE NŒUD DE CALCUL                             ##
# ##                                                                          ##
# ##   Activée automatiquement quand __ASTER_PHASE=RUN (transmis par sbatch)  ##
# ##                                                                          ##
# ##############################################################################
# ##############################################################################

if [ "${__ASTER_PHASE:-}" = "RUN" ]; then

    # ── Trap : rapatrier les résultats (fin normale, scancel, timeout) ────────
    rapatrier() {
        sep "RAPATRIEMENT DES RÉSULTATS → $__ASTER_STUDY_DIR"
        local n=0
        for f in \
            "${__ASTER_SCRATCH_DIR}/${__ASTER_STUDY_NAME}.mess" \
            "${__ASTER_SCRATCH_DIR}/${__ASTER_STUDY_NAME}.resu" \
            "${__ASTER_SCRATCH_DIR}/${__ASTER_STUDY_NAME}_resu.med"
        do
            if [ -f "$f" ] && [ -s "$f" ]; then
                cp "$f" "$__ASTER_STUDY_DIR/"
                log "✓ Rapatrié : $(basename "$f")  ($(du -h "$f" | cut -f1))"
                (( n++ )) || true
            fi
        done
        [ -f "$__ASTER_EXPORT_FILE" ] && cp "$__ASTER_EXPORT_FILE" "$__ASTER_STUDY_DIR/"
        for f in "${__ASTER_LOG_DIR}"/aster_${SLURM_JOB_ID:-unknown}.{out,err}; do
            [ -f "$f" ] && cp "$f" "$__ASTER_STUDY_DIR/" 2>/dev/null
        done
        [ "$n" -eq 0 ] && log "⚠  Aucun fichier résultat trouvé"
        [ "$n" -gt 0 ] && log "✓ $n fichier(s) rapatrié(s) dans $__ASTER_STUDY_DIR"
    }
    trap rapatrier EXIT

    # ── Infos de démarrage ────────────────────────────────────────────────────
    sep "DÉBUT CALCUL CODE_ASTER — $(date)"
    log "Job ID         : $SLURM_JOB_ID"
    log "Étude          : $__ASTER_STUDY_NAME"
    log "Scratch        : $__ASTER_SCRATCH_DIR"
    log "Nœuds          : $SLURM_NODELIST"
    log "Tâches MPI     : $SLURM_NTASKS"
    log "CPUs/tâche     : $SLURM_CPUS_PER_TASK"
    log "Mémoire        : $__ASTER_MEM"
    log "Résultats →    : $__ASTER_STUDY_DIR"

    # ── Chargement Code_Aster ─────────────────────────────────────────────────
    sep "CHARGEMENT CODE_ASTER"
    if command -v module &>/dev/null && [ -n "${ASTER_MODULE:-}" ]; then
        module load "${ASTER_MODULE}" 2>/dev/null \
            && log "Module '${ASTER_MODULE}' chargé" \
            || log "Module '${ASTER_MODULE}' non disponible"
    fi

    ASTER_EXE=""
    for candidate in \
        "${ASTER_ROOT}/bin/run_aster" \
        "${ASTER_ROOT}/bin/as_run" \
        "$(command -v run_aster 2>/dev/null || true)" \
        "$(command -v as_run   2>/dev/null || true)"
    do
        [ -n "$candidate" ] && [ -x "$candidate" ] && { ASTER_EXE="$candidate"; break; }
    done

    if [ -z "$ASTER_EXE" ]; then
        log "ERREUR : Code_Aster introuvable !"
        log "  ASTER_ROOT=$ASTER_ROOT"
        log "  ASTER_MODULE=$ASTER_MODULE"
        exit 1
    fi
    log "Exécutable : $ASTER_EXE"

    # ── Lancement du calcul ───────────────────────────────────────────────────
    sep "CALCUL EN COURS"
    START_TIME=$(date +%s)
    log "Démarrage : $(date)"

    if [ "$SLURM_NTASKS" -gt 1 ]; then
        log "Mode parallèle MPI ($SLURM_NTASKS processus)"
        srun --mpi=pmi2 "$ASTER_EXE" "$__ASTER_EXPORT_FILE"
    else
        log "Mode séquentiel"
        "$ASTER_EXE" "$__ASTER_EXPORT_FILE"
    fi
    ASTER_RC=$?

    END_TIME=$(date +%s)
    ELAPSED=$(( END_TIME - START_TIME ))
    ELAPSED_FMT=$(printf '%02dh %02dm %02ds' $((ELAPSED/3600)) $((ELAPSED%3600/60)) $((ELAPSED%60)))

    # ── Diagnostic .mess ──────────────────────────────────────────────────────
    sep "DIAGNOSTIC"
    MESS_PATH="${__ASTER_SCRATCH_DIR}/${__ASTER_STUDY_NAME}.mess"
    if [ -f "$MESS_PATH" ]; then
        NB_ALARM=$(grep -c "<A>" "$MESS_PATH" 2>/dev/null || echo 0)
        NB_FATAL=$(grep -c "<F>" "$MESS_PATH" 2>/dev/null || echo 0)
        NB_EXCEP=$(grep -c "<S>" "$MESS_PATH" 2>/dev/null || echo 0)
        log "Alarmes <A>        : $NB_ALARM"
        log "Erreurs fatales <F>: $NB_FATAL"
        log "Exceptions <S>     : $NB_EXCEP"
        if [ "$NB_FATAL" -gt 0 ]; then
            log "--- Première erreur fatale ---"
            grep -B3 -A8 "<F>" "$MESS_PATH" | head -25
            log "--- fin ---"
        fi
        log ""
        log "--- Dernières lignes du .mess ---"
        tail -15 "$MESS_PATH"
        log "--- fin ---"
    else
        log "⚠  Fichier .mess non trouvé"
    fi

    # ── Résumé ────────────────────────────────────────────────────────────────
    sep "RÉSUMÉ FINAL"
    [ "$ASTER_RC" -eq 0 ] && log "Statut : ✓ SUCCÈS" || log "Statut : ✗ ÉCHEC (code $ASTER_RC)"
    log "Durée          : $ELAPSED_FMT"
    log "Résultats →    : $__ASTER_STUDY_DIR"
    log "Fin            : $(date)"

    exit $ASTER_RC
fi

# ##############################################################################
# ##############################################################################
# ##                                                                          ##
# ##   PHASE 1 : PRÉPARATION (login)                                         ##
# ##                                                                          ##
# ##   Cette partie tourne quand tu lances le script.                         ##
# ##   Elle prépare tout puis fait sbatch en interne.                         ##
# ##                                                                          ##
# ##############################################################################
# ##############################################################################

# ── Sécurité : empêcher sbatch direct ─────────────────────────────────────────
if [ -n "${SLURM_JOB_ID:-}" ]; then
    err "Ce script se lance avec bash ou ./ — PAS avec sbatch !"
    err ""
    err "  ✗  sbatch run_aster_slurm.sh"
    err "  ✓  bash run_aster_slurm.sh"
    err "  ✓  ./run_aster_slurm.sh"
    err ""
    err "Le script fait le sbatch tout seul en interne."
    exit 1
fi

set -euo pipefail

# ══════════════════════════════════════════════════════════════════════════════
#  PARSING DES ARGUMENTS
# ══════════════════════════════════════════════════════════════════════════════
STUDY_DIR="."
COMM_FILE=""
MED_FILE=""
MAIL_FILE=""
PRESET=""
PARTITION=""
NODES=""
NTASKS=""
CPUS=""
MEM=""
TIME_LIMIT=""
QUIET=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -C|--comm)       COMM_FILE="$2";    shift 2 ;;
        -M|--med)        MED_FILE="$2";     shift 2 ;;
        -A|--mail)       MAIL_FILE="$2";    shift 2 ;;
        -P|--preset)     PRESET="$2";       shift 2 ;;
        -p|--partition)  PARTITION="$2";    shift 2 ;;
        -n|--nodes)      NODES="$2";        shift 2 ;;
        -t|--ntasks)     NTASKS="$2";       shift 2 ;;
        -c|--cpus)       CPUS="$2";         shift 2 ;;
        -m|--mem)        MEM="$2";          shift 2 ;;
        -T|--time)       TIME_LIMIT="$2";   shift 2 ;;
        -q|--quiet)      QUIET=true;        shift ;;
        -h|--help)       usage ;;
        -*)              err "Option inconnue : $1"; usage ;;
        *)               STUDY_DIR="$1";    shift ;;
    esac
done

# ── Application du préréglage ─────────────────────────────────────────────────
if [ -n "$PRESET" ]; then
    case "${PRESET,,}" in
        court|short)
            : "${PARTITION:=$PRESET_COURT_PARTITION}"; : "${NTASKS:=$PRESET_COURT_NTASKS}"
            : "${MEM:=$PRESET_COURT_MEM}";             : "${TIME_LIMIT:=$PRESET_COURT_TIME}"
            $QUIET || info "Préréglage : court  (${PRESET_COURT_NTASKS} tâches, ${PRESET_COURT_MEM}, ${PRESET_COURT_TIME})"
            ;;
        moyen|medium)
            : "${PARTITION:=$PRESET_MOYEN_PARTITION}"; : "${NTASKS:=$PRESET_MOYEN_NTASKS}"
            : "${MEM:=$PRESET_MOYEN_MEM}";             : "${TIME_LIMIT:=$PRESET_MOYEN_TIME}"
            $QUIET || info "Préréglage : moyen  (${PRESET_MOYEN_NTASKS} tâches, ${PRESET_MOYEN_MEM}, ${PRESET_MOYEN_TIME})"
            ;;
        long)
            : "${PARTITION:=$PRESET_LONG_PARTITION}"; : "${NTASKS:=$PRESET_LONG_NTASKS}"
            : "${MEM:=$PRESET_LONG_MEM}";             : "${TIME_LIMIT:=$PRESET_LONG_TIME}"
            $QUIET || info "Préréglage : long   (${PRESET_LONG_NTASKS} tâches, ${PRESET_LONG_MEM}, ${PRESET_LONG_TIME})"
            ;;
        *) err "Préréglage inconnu : '$PRESET'  (court, moyen, long)"; exit 1 ;;
    esac
fi

: "${PARTITION:=$DEFAULT_PARTITION}"
: "${NODES:=$DEFAULT_NODES}"
: "${NTASKS:=$DEFAULT_NTASKS}"
: "${CPUS:=$DEFAULT_CPUS_PER_TASK}"
: "${MEM:=$DEFAULT_MEM}"
: "${TIME_LIMIT:=$DEFAULT_TIME}"

# ══════════════════════════════════════════════════════════════════════════════
#  DÉTECTION DES FICHIERS
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Détection de l'étude"

STUDY_DIR="$(realpath "$STUDY_DIR")"
STUDY_NAME="$(basename "$STUDY_DIR")"
[ -d "$STUDY_DIR" ] || { err "Dossier introuvable : $STUDY_DIR"; exit 1; }

$QUIET || info "Dossier : $STUDY_DIR"
$QUIET || info "Étude   : $STUDY_NAME"

# ── .comm ─────────────────────────────────────────────────────────────────────
if [ -z "$COMM_FILE" ]; then
    mapfile -t COMM_LIST < <(find "$STUDY_DIR" -maxdepth 1 -name "*.comm" | sort)
    case ${#COMM_LIST[@]} in
        0) err "Aucun .comm dans $STUDY_DIR"; exit 1 ;;
        1) COMM_FILE="${COMM_LIST[0]}" ;;
        *) warn "Plusieurs .comm — utilisation du premier"
           COMM_FILE="${COMM_LIST[0]}" ;;
    esac
fi
COMM_FILE="$(realpath "$COMM_FILE")"
[ -f "$COMM_FILE" ] || { err ".comm introuvable : $COMM_FILE"; exit 1; }
$QUIET || ok "Commandes : $COMM_FILE"

# ── .med ──────────────────────────────────────────────────────────────────────
if [ -z "$MED_FILE" ]; then
    mapfile -t MED_LIST < <(find "$STUDY_DIR" -maxdepth 1 -name "*.med" | sort)
    case ${#MED_LIST[@]} in
        0) : ;;
        1) MED_FILE="${MED_LIST[0]}" ;;
        *) warn "Plusieurs .med — utilisation du premier"; MED_FILE="${MED_LIST[0]}" ;;
    esac
fi
[ -n "$MED_FILE" ] && MED_FILE="$(realpath "$MED_FILE")"
[ -n "$MED_FILE" ] && { $QUIET || ok "Maillage MED  : $MED_FILE"; }

# ── .mail ─────────────────────────────────────────────────────────────────────
if [ -z "$MAIL_FILE" ]; then
    mapfile -t MAIL_LIST < <(find "$STUDY_DIR" -maxdepth 1 -name "*.mail" | sort)
    case ${#MAIL_LIST[@]} in
        0) : ;;
        1) MAIL_FILE="${MAIL_LIST[0]}" ;;
        *) warn "Plusieurs .mail — utilisation du premier"; MAIL_FILE="${MAIL_LIST[0]}" ;;
    esac
fi
[ -n "$MAIL_FILE" ] && MAIL_FILE="$(realpath "$MAIL_FILE")"
[ -n "$MAIL_FILE" ] && { $QUIET || ok "Maillage ASTER: $MAIL_FILE"; }

[ -z "$MED_FILE" ] && [ -z "$MAIL_FILE" ] && warn "Aucun maillage détecté"

# ══════════════════════════════════════════════════════════════════════════════
#  PRÉPARATION DU SCRATCH
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Préparation du scratch"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SCRATCH_DIR="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_${TIMESTAMP}"
LOG_DIR="${SCRATCH_DIR}/logs"

mkdir -p "$SCRATCH_DIR" "$LOG_DIR"
$QUIET || ok "Scratch : $SCRATCH_DIR"

cp "$COMM_FILE" "$SCRATCH_DIR/"
$QUIET || ok "Copié : $(basename "$COMM_FILE")"

[ -n "$MED_FILE" ]  && [ -f "$MED_FILE" ]  && { cp "$MED_FILE"  "$SCRATCH_DIR/"; $QUIET || ok "Copié : $(basename "$MED_FILE")"; }
[ -n "$MAIL_FILE" ] && [ -f "$MAIL_FILE" ] && { cp "$MAIL_FILE" "$SCRATCH_DIR/"; $QUIET || ok "Copié : $(basename "$MAIL_FILE")"; }

for ext in py dat para include mfront; do
    for f in "$STUDY_DIR/"*."$ext"; do
        [ -f "$f" ] && cp "$f" "$SCRATCH_DIR/" && { $QUIET || ok "Copié : $(basename "$f")"; }
    done
done

# ══════════════════════════════════════════════════════════════════════════════
#  GÉNÉRATION DU .EXPORT
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Génération du .export"

COMM_BASENAME="$(basename "$COMM_FILE")"
MED_BASENAME="";  [ -n "$MED_FILE"  ] && [ -f "$MED_FILE"  ] && MED_BASENAME="$(basename "$MED_FILE")"
MAIL_BASENAME=""; [ -n "$MAIL_FILE" ] && [ -f "$MAIL_FILE" ] && MAIL_BASENAME="$(basename "$MAIL_FILE")"

MEM_MB=$(echo "$MEM" | awk '/[Gg]$/{gsub(/[Gg]/,""); print $1*1024; next} /[Mm]$/{gsub(/[Mm]/,""); print $1; next} {print $1}')
ASTER_MEM=$(( MEM_MB - 512 ))
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512

TIME_LIMIT_SEC=$(echo "$TIME_LIMIT" | awk -F: '
    NF==3 {print $1*3600 + $2*60 + $3; next}
    NF==2 {print $1*60   + $2;         next}
           {print $1*60}')

EXPORT_FILE="${SCRATCH_DIR}/${STUDY_NAME}.export"
{
    echo "P actions make_etude"
    echo "P mode interactif"
    echo "P version stable"
    echo "P ncpus ${NTASKS}"
    echo "P memory_limit ${ASTER_MEM}"
    echo "P time_limit ${TIME_LIMIT_SEC}"
    echo ""
    echo "F comm ${SCRATCH_DIR}/${COMM_BASENAME}           D  1"
    [ -n "$MED_BASENAME"  ] && echo "F mmed ${SCRATCH_DIR}/${MED_BASENAME}            D 20"
    [ -n "$MAIL_BASENAME" ] && echo "F mail ${SCRATCH_DIR}/${MAIL_BASENAME}           D 20"
    echo "F mess ${SCRATCH_DIR}/${STUDY_NAME}.mess         R  6"
    echo "F resu ${SCRATCH_DIR}/${STUDY_NAME}.resu         R  8"
    echo "F rmed ${SCRATCH_DIR}/${STUDY_NAME}_resu.med     R 80"
} > "$EXPORT_FILE"

if ! $QUIET; then
    ok "Export : $EXPORT_FILE"
    while IFS= read -r line; do info "  $line"; done < "$EXPORT_FILE"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  RÉCAPITULATIF
# ══════════════════════════════════════════════════════════════════════════════
if ! $QUIET; then
    section "Ressources Slurm"
    info "Partition  : $PARTITION"
    info "Nœuds      : $NODES"
    info "Tâches MPI : $NTASKS"
    info "CPUs/tâche : $CPUS"
    info "Mémoire    : $MEM"
    info "Durée max  : $TIME_LIMIT"
    info "Scratch    : $SCRATCH_DIR"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  SOUMISSION SBATCH (le script se re-soumet lui-même)
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Soumission Slurm"

SELF_SCRIPT="$(realpath "$0")"

JOB_ID=$(sbatch --parsable \
    --job-name="aster_${STUDY_NAME}" \
    --partition="${PARTITION}" \
    --nodes="${NODES}" \
    --ntasks="${NTASKS}" \
    --cpus-per-task="${CPUS}" \
    --mem="${MEM}" \
    --time="${TIME_LIMIT}" \
    --output="${LOG_DIR}/aster_%j.out" \
    --error="${LOG_DIR}/aster_%j.err" \
    --export="ALL,__ASTER_PHASE=RUN,__ASTER_STUDY_DIR=${STUDY_DIR},__ASTER_STUDY_NAME=${STUDY_NAME},__ASTER_SCRATCH_DIR=${SCRATCH_DIR},__ASTER_LOG_DIR=${LOG_DIR},__ASTER_EXPORT_FILE=${EXPORT_FILE},__ASTER_MEM=${MEM}" \
    "${SELF_SCRIPT}")

if [ -z "$JOB_ID" ]; then
    err "Échec de la soumission Slurm."
    exit 1
fi

if $QUIET; then
    echo "$JOB_ID"
else
    ok "Job soumis : ID = ${BOLD}${JOB_ID}${NC}"
    echo ""
    echo -e "  ${BOLD}Commandes utiles :${NC}"
    echo -e "  squeue -j ${JOB_ID}                                # état du job"
    echo -e "  tail -f ${LOG_DIR}/aster_${JOB_ID}.out            # logs temps réel"
    echo -e "  scancel ${JOB_ID}                                   # annuler"
    echo -e "  ls ${SCRATCH_DIR}/                                 # scratch
   echo ""
fi
