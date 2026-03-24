#!/bin/bash
#===============================================================================
#  run_aster_slurm — Calcul Code_Aster via sbatch (fichier unique)
#===============================================================================
#
#  Usage :  sbatch run_aster_slurm.sh [OPTIONS] [DOSSIER_ETUDE]
#
#  Le script fonctionne en DEUX PHASES dans un seul fichier :
#
#    PHASE 1 (nœud login) : sbatch lance ce script sans directives #SBATCH.
#      Il détecte qu'on est en phase de préparation (__ASTER_PHASE non défini),
#      prépare le scratch, copie les fichiers, génère le .export, puis se
#      RE-SOUMET LUI-MÊME avec les bons paramètres via sbatch --partition=...
#
#    PHASE 2 (nœud de calcul) : le script détecte __ASTER_PHASE=RUN
#      (transmis via --export), charge Code_Aster et lance le calcul.
#      À la fin (ou en cas de scancel/timeout), un trap rapatrie
#      les résultats vers le dossier d'étude d'origine.
#
#  Auteur   : généré pour localcluster
#  Version  : 5.0
#===============================================================================

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION — modifier ici selon l'installation
# ══════════════════════════════════════════════════════════════════════════════

# Chemin vers Code_Aster (surchargeable par variable d'environnement)
ASTER_ROOT="${ASTER_ROOT:-/opt/code_aster}"

# Nom du module Lmod à charger (laisser vide si pas de module)
ASTER_MODULE="${ASTER_MODULE:-code_aster}"

# Répertoire scratch partagé (accessible depuis login ET nœuds de calcul)
SCRATCH_BASE="${SCRATCH_BASE:-/scratch}"

# ── Valeurs par défaut Slurm ─────────────────────────────────────────────────
DEFAULT_PARTITION="court"
DEFAULT_NODES=1
DEFAULT_NTASKS=4
DEFAULT_CPUS_PER_TASK=1
DEFAULT_MEM="4G"
DEFAULT_TIME="24:00:00"

# ── Préréglages de ressources (-P court | -P moyen | -P long) ────────────────
PRESET_COURT_PARTITION="court"  ; PRESET_COURT_NTASKS=2 ; PRESET_COURT_MEM="2G"  ; PRESET_COURT_TIME="02:00:00"
PRESET_MOYEN_PARTITION="court"  ; PRESET_MOYEN_NTASKS=4 ; PRESET_MOYEN_MEM="8G"  ; PRESET_MOYEN_TIME="24:00:00"
PRESET_LONG_PARTITION="court"   ; PRESET_LONG_NTASKS=8  ; PRESET_LONG_MEM="32G"  ; PRESET_LONG_TIME="72:00:00"

# ══════════════════════════════════════════════════════════════════════════════
#  AFFICHAGE (utilisé dans les deux phases)
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
  sbatch run_aster_slurm.sh [OPTIONS] [DOSSIER_ETUDE]

  Lance un calcul Code_Aster via Slurm.
  Les fichiers sont copiés dans ${SCRATCH_BASE}/\$USER/ avant soumission.
  Par défaut, DOSSIER_ETUDE = répertoire courant.

${BOLD}FICHIERS${NC}
  -C, --comm FILE      Fichier .comm  (auto-détecté si absent)
  -M, --med  FILE      Fichier .med   (auto-détecté si absent)
  -A, --mail FILE      Fichier .mail  (format ASTER natif, auto-détecté si absent)

${BOLD}RÉSULTATS SUPPLÉMENTAIRES${NC}
  -R, --results LIST   Unités de résultat additionnelles.
                        Format : "type:unite,type:unite,..."
                        Types : rmed, resu, mess, csv, table, dat, pos
                        Exemples :
                          --results "rmed:81"               un 2ème MED
                          --results "rmed:81,csv:38"        MED + CSV
                          --results "table:39,dat:40"       table + données

                        Dans le .comm, utiliser l'unité correspondante :
                          IMPR_RESU(UNITE=81, ...)
                          IMPR_TABLE(UNITE=38, ...)

${BOLD}RESSOURCES SLURM${NC}
  -p, --partition NOM  Partition        [défaut: ${DEFAULT_PARTITION}]
  -n, --nodes     N    Nombre de nœuds  [défaut: ${DEFAULT_NODES}]
  -t, --ntasks    N    Tâches MPI       [défaut: ${DEFAULT_NTASKS}]
  -c, --cpus      N    CPUs par tâche   [défaut: ${DEFAULT_CPUS_PER_TASK}]
  -m, --mem       MEM  Mémoire/nœud     [défaut: ${DEFAULT_MEM}]
  -T, --time  H:M:S    Durée max        [défaut: ${DEFAULT_TIME}]

${BOLD}PRÉRÉGLAGES${NC}
  -P, --preset NOM     Préréglage de ressources (court, moyen, long)
                         court : ${PRESET_COURT_NTASKS} tâches, ${PRESET_COURT_MEM}, ${PRESET_COURT_TIME}
                         moyen : ${PRESET_MOYEN_NTASKS} tâches, ${PRESET_MOYEN_MEM}, ${PRESET_MOYEN_TIME}
                         long  : ${PRESET_LONG_NTASKS} tâches, ${PRESET_LONG_MEM}, ${PRESET_LONG_TIME}
                       Les options passées après -P surchargent le préréglage.

${BOLD}OPTIONS${NC}
  -q, --quiet          Sortie minimale (juste le job ID)
  -h, --help           Afficher cette aide

${BOLD}EXEMPLES${NC}
  sbatch run_aster_slurm.sh                              # Étude dans le dossier courant
  sbatch run_aster_slurm.sh ~/calculs/poutre/            # Spécifier le dossier
  sbatch run_aster_slurm.sh -P court                     # Préréglage court  (2 h)
  sbatch run_aster_slurm.sh -P moyen                     # Préréglage moyen  (24 h)
  sbatch run_aster_slurm.sh -P long                      # Préréglage long   (72 h)
  sbatch run_aster_slurm.sh -P moyen -t 8                # Moyen mais 8 tâches MPI
  sbatch run_aster_slurm.sh -n 2 -t 8 -m 8G             # Personnalisé
  sbatch run_aster_slurm.sh -p debug -T 01:00:00         # Partition debug, 1h max
  sbatch run_aster_slurm.sh -C mon_calcul.comm -M maillage.med
  sbatch run_aster_slurm.sh -P moyen --results "rmed:81,csv:38"
EOF
    exit 0
}

# ##############################################################################
# ##############################################################################
# ##                                                                          ##
# ##   PHASE 2 : EXÉCUTION SUR LE NŒUD DE CALCUL                             ##
# ##                                                                          ##
# ##   Détecté par __ASTER_PHASE=RUN (transmis via sbatch --export)           ##
# ##   Toutes les variables __ASTER_* contiennent les chemins résolus         ##
# ##                                                                          ##
# ##############################################################################
# ##############################################################################

if [ "${__ASTER_PHASE:-}" = "RUN" ]; then

    # ── Variable pour éviter un double rapatriement ───────────────────────────
    __RAPATRIE_DONE=0

    # ── Trap : rapatrier les résultats même en cas de scancel / timeout ───────
    #
    #  SIGTERM est envoyé par SLURM avant SIGKILL (avec un délai de grâce
    #  configurable, souvent 30s). On trappe SIGTERM pour rapatrier avant
    #  que SIGKILL ne tue le processus.
    #  EXIT est trappé en complément pour le cas normal (fin de script).
    #
    rapatrier() {
        # Éviter le double rapatriement (SIGTERM puis EXIT)
        if [ "$__RAPATRIE_DONE" -eq 1 ]; then
            return
        fi
        __RAPATRIE_DONE=1

        sep "RAPATRIEMENT DES RÉSULTATS"
        local dest="$__ASTER_STUDY_DIR"
        local n=0

        # Vérifier que le dossier destination (work) est accessible
        if [ ! -d "$dest" ]; then
            log "⚠  Dossier destination inaccessible : $dest"
            log "   Tentative de création..."
            mkdir -p "$dest" 2>/dev/null || {
                log "⚠  Impossible de créer $dest — résultats restent dans le scratch"
                log "   Scratch : $__ASTER_SCRATCH_DIR"
                return
            }
        fi

        # Rapatrier TOUS les fichiers de résultat par extension
        # On exclut les fichiers d'entrée (.comm, .export) et le dossier logs
        for ext in mess resu med csv table dat pos rmed txt vtu vtk; do
            for f in "${__ASTER_SCRATCH_DIR}"/*."${ext}"; do
                if [ -f "$f" ] && [ -s "$f" ]; then
                    cp -v "$f" "$dest/" 2>&1 | while read -r line; do log "$line"; done
                    (( n++ )) || true
                fi
            done
        done

        # Rapatrier REPE_OUT si présent
        if [ -d "${__ASTER_SCRATCH_DIR}/REPE_OUT" ]; then
            cp -rv "${__ASTER_SCRATCH_DIR}/REPE_OUT" "$dest/" 2>&1 | while read -r line; do log "$line"; done
            (( n++ )) || true
        fi

        if [ "$n" -eq 0 ]; then
            log "⚠  Aucun fichier résultat trouvé dans ${__ASTER_SCRATCH_DIR}"
            log "   Contenu du scratch :"
            ls -la "${__ASTER_SCRATCH_DIR}/" 2>/dev/null | while read -r line; do log "   $line"; done
        else
            log "✓  $n fichier(s) rapatrié(s) vers $dest"
        fi

        log ""
        log "Résultats dans : $dest"
        ls -lh "$dest"/*.{mess,resu,med,csv,table,dat,pos,rmed,txt,vtu,vtk} 2>/dev/null | while read -r line; do log "  $line"; done
    }

    # Trapper SIGTERM (envoyé par scancel / timeout SLURM) ET EXIT
    trap rapatrier EXIT
    trap 'rapatrier; exit 143' SIGTERM

    # ── Infos de démarrage ────────────────────────────────────────────────────
    sep "DÉBUT CALCUL CODE_ASTER — $(date)"
    log "Job ID         : $SLURM_JOB_ID"
    log "Étude          : $__ASTER_STUDY_NAME"
    log "Scratch        : $__ASTER_SCRATCH_DIR"
    log "Destination    : $__ASTER_STUDY_DIR"
    log "Nœuds alloués  : $SLURM_NODELIST"
    log "Tâches MPI     : $SLURM_NTASKS"
    log "CPUs par tâche : $SLURM_CPUS_PER_TASK"
    log "Mémoire        : $__ASTER_MEM"

    # ── Chargement de Code_Aster ──────────────────────────────────────────────
    sep "CHARGEMENT CODE_ASTER"

    if command -v module &>/dev/null && [ -n "${ASTER_MODULE:-}" ]; then
        module load "${ASTER_MODULE}" 2>/dev/null \
            && log "Module '${ASTER_MODULE}' chargé." \
            || log "Module '${ASTER_MODULE}' non disponible."
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
        log "⚠  Code_Aster introuvable."
        log "   → Définir ASTER_ROOT=/chemin/code_aster avant de relancer."
        log "   → Ou créer un module Lmod (ASTER_MODULE)."
        exit 1
    fi
    log "Exécutable : $ASTER_EXE"

    # ── Lancement du calcul ───────────────────────────────────────────────────
    sep "CALCUL EN COURS"
    log "Démarrage : $(date)"

    if [ "$SLURM_NTASKS" -gt 1 ]; then
        log "Mode parallèle MPI ($SLURM_NTASKS processus)"
        srun --mpi=pmi2 "$ASTER_EXE" "$__ASTER_EXPORT_FILE"
    else
        log "Mode séquentiel"
        "$ASTER_EXE" "$__ASTER_EXPORT_FILE"
    fi
    ASTER_RC=$?

    # ── Diagnostic rapide du .mess ────────────────────────────────────────────
    sep "DIAGNOSTIC"
    MESS_PATH="${__ASTER_SCRATCH_DIR}/${__ASTER_STUDY_NAME}.mess"
    if [ -f "$MESS_PATH" ]; then
        NB_ALARM=$(grep -c "<A>" "$MESS_PATH" 2>/dev/null || true)
        NB_FATAL=$(grep -c "<F>" "$MESS_PATH" 2>/dev/null || true)
        NB_EXCEP=$(grep -c "<S>" "$MESS_PATH" 2>/dev/null || true)
        NB_ALARM=${NB_ALARM:-0}
        NB_FATAL=${NB_FATAL:-0}
        NB_EXCEP=${NB_EXCEP:-0}
        log "Alarmes <A>        : $NB_ALARM"
        log "Erreurs fatales <F>: $NB_FATAL"
        log "Exceptions <S>     : $NB_EXCEP"
        if [ "$NB_FATAL" -gt 0 ]; then
            log "--- Première erreur fatale ---"
            grep -B2 -A5 "<F>" "$MESS_PATH" | head -20
            log "--- fin ---"
        fi
    else
        log "⚠  Fichier .mess non trouvé (échec au démarrage ?)"
    fi

    # ── Résumé (le trap EXIT fera le rapatriement) ────────────────────────────
    sep "RÉSUMÉ FINAL"
    if [ "$ASTER_RC" -eq 0 ]; then
        log "Statut    : SUCCÈS ✓"
    else
        log "Statut    : ÉCHEC  ✗  (code $ASTER_RC)"
    fi
    log "Étude     : $__ASTER_STUDY_NAME"
    log "Résultats : $__ASTER_STUDY_DIR"
    log "Scratch   : $__ASTER_SCRATCH_DIR"
    log "Fin       : $(date)"

    exit $ASTER_RC
fi

# ##############################################################################
# ##############################################################################
# ##                                                                          ##
# ##   PHASE 1 : PRÉPARATION SUR LE NŒUD LOGIN                               ##
# ##                                                                          ##
# ##   Détection fichiers, copie scratch, .export, re-soumission sbatch       ##
# ##                                                                          ##
# ##############################################################################
# ##############################################################################

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
RESULT_UNITS=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -C|--comm)       COMM_FILE="$2";    shift 2 ;;
        -M|--med)        MED_FILE="$2";     shift 2 ;;
        -A|--mail)       MAIL_FILE="$2";    shift 2 ;;
        -R|--results)    RESULT_UNITS="$2"; shift 2 ;;
        -P|--preset)     PRESET="$2";       shift 2 ;;
        -p|--partition)  PARTITION="$2";    shift 2 ;;
        -n|--nodes)      NODES="$2";        shift 2 ;;
        -t|--ntasks)     NTASKS="$2";       shift 2 ;;
        -c|--cpus)       CPUS="$2";         shift 2 ;;
        -m|--mem)        MEM="$2";          shift 2 ;;
        -T|--time)       TIME_LIMIT="$2";   shift 2 ;;
        -q|--quiet)      QUIET=true;        shift ;;
        -h|--help)       usage ;;
        -*)              err "Option inconnue : $1"; echo ""; usage ;;
        *)               STUDY_DIR="$1";    shift ;;
    esac
done

# ── Application du préréglage (les options explicites ont priorité) ───────────
if [ -n "$PRESET" ]; then
    case "${PRESET,,}" in
        court|short)
            : "${PARTITION:=$PRESET_COURT_PARTITION}"
            : "${NTASKS:=$PRESET_COURT_NTASKS}"
            : "${MEM:=$PRESET_COURT_MEM}"
            : "${TIME_LIMIT:=$PRESET_COURT_TIME}"
            $QUIET || info "Préréglage : court  (${PRESET_COURT_NTASKS} tâches, ${PRESET_COURT_MEM}, ${PRESET_COURT_TIME})"
            ;;
        moyen|medium)
            : "${PARTITION:=$PRESET_MOYEN_PARTITION}"
            : "${NTASKS:=$PRESET_MOYEN_NTASKS}"
            : "${MEM:=$PRESET_MOYEN_MEM}"
            : "${TIME_LIMIT:=$PRESET_MOYEN_TIME}"
            $QUIET || info "Préréglage : moyen  (${PRESET_MOYEN_NTASKS} tâches, ${PRESET_MOYEN_MEM}, ${PRESET_MOYEN_TIME})"
            ;;
        long)
            : "${PARTITION:=$PRESET_LONG_PARTITION}"
            : "${NTASKS:=$PRESET_LONG_NTASKS}"
            : "${MEM:=$PRESET_LONG_MEM}"
            : "${TIME_LIMIT:=$PRESET_LONG_TIME}"
            $QUIET || info "Préréglage : long   (${PRESET_LONG_NTASKS} tâches, ${PRESET_LONG_MEM}, ${PRESET_LONG_TIME})"
            ;;
        *) err "Préréglage inconnu : '$PRESET'  (valeurs : court, moyen, long)"; exit 1 ;;
    esac
fi

# Valeurs par défaut pour tout paramètre non fixé par option ou préréglage
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

$QUIET || info "Dossier  : $STUDY_DIR"
$QUIET || info "Étude    : $STUDY_NAME"

# ── Fichier .comm ─────────────────────────────────────────────────────────────
if [ -z "$COMM_FILE" ]; then
    mapfile -t COMM_LIST < <(find "$STUDY_DIR" -maxdepth 1 -name "*.comm" | sort)
    case ${#COMM_LIST[@]} in
        0) err "Aucun fichier .comm dans $STUDY_DIR"; exit 1 ;;
        1) COMM_FILE="${COMM_LIST[0]}" ;;
        *) warn "Plusieurs .comm trouvés — sélection du premier :"
           for f in "${COMM_LIST[@]}"; do warn "  $f"; done
           COMM_FILE="${COMM_LIST[0]}" ;;
    esac
fi
COMM_FILE="$(realpath "$COMM_FILE")"
[ -f "$COMM_FILE" ] || { err "Fichier .comm introuvable : $COMM_FILE"; exit 1; }
$QUIET || ok "Commandes: $COMM_FILE"

# ── Fichier .med (optionnel) ──────────────────────────────────────────────────
if [ -z "$MED_FILE" ]; then
    mapfile -t MED_LIST < <(find "$STUDY_DIR" -maxdepth 1 -name "*.med" | sort)
    case ${#MED_LIST[@]} in
        0) : ;;
        1) MED_FILE="${MED_LIST[0]}" ;;
        *) warn "Plusieurs .med trouvés — sélection du premier :"
           for f in "${MED_LIST[@]}"; do warn "  $f"; done
           MED_FILE="${MED_LIST[0]}" ;;
    esac
fi
[ -n "$MED_FILE" ] && MED_FILE="$(realpath "$MED_FILE")"
[ -n "$MED_FILE" ] && { $QUIET || ok "Maillage MED  : $MED_FILE"; }

# ── Fichier .mail (format ASTER natif, optionnel) ────────────────────────────
if [ -z "$MAIL_FILE" ]; then
    mapfile -t MAIL_LIST < <(find "$STUDY_DIR" -maxdepth 1 -name "*.mail" | sort)
    case ${#MAIL_LIST[@]} in
        0) : ;;
        1) MAIL_FILE="${MAIL_LIST[0]}" ;;
        *) warn "Plusieurs .mail trouvés — sélection du premier :"
           for f in "${MAIL_LIST[@]}"; do warn "  $f"; done
           MAIL_FILE="${MAIL_LIST[0]}" ;;
    esac
fi
[ -n "$MAIL_FILE" ] && MAIL_FILE="$(realpath "$MAIL_FILE")"
[ -n "$MAIL_FILE" ] && { $QUIET || ok "Maillage ASTER: $MAIL_FILE"; }

[ -z "$MED_FILE" ] && [ -z "$MAIL_FILE" ] && \
    warn "Aucun maillage (.med ou .mail) — calcul sans maillage externe ?"

# ══════════════════════════════════════════════════════════════════════════════
#  PRÉPARATION DU SCRATCH
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Préparation du scratch"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SCRATCH_DIR="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_${TIMESTAMP}"

$QUIET || info "Création : $SCRATCH_DIR"
mkdir -p "$SCRATCH_DIR"

# ── Copie des fichiers ───────────────────────────────────────────────────────
cp "$COMM_FILE" "$SCRATCH_DIR/"
$QUIET || ok "Copié : $(basename "$COMM_FILE")"

if [ -n "$MED_FILE" ]; then
    cp "$MED_FILE" "$SCRATCH_DIR/"
    $QUIET || ok "Copié : $(basename "$MED_FILE")"
fi

if [ -n "$MAIL_FILE" ]; then
    cp "$MAIL_FILE" "$SCRATCH_DIR/"
    $QUIET || ok "Copié : $(basename "$MAIL_FILE")"
fi

# Fichiers annexes optionnels (.py, .dat, .para, .include, .mfront)
for ext in py dat para include mfront; do
    for f in "$STUDY_DIR/"*."$ext"; do
        [ -f "$f" ] && cp "$f" "$SCRATCH_DIR/" && { $QUIET || ok "Copié : $(basename "$f")"; }
    done
done

# ══════════════════════════════════════════════════════════════════════════════
#  GÉNÉRATION DU FICHIER .EXPORT
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Génération du fichier .export"

COMM_BASENAME="$(basename "$COMM_FILE")"
MED_BASENAME="$([ -n "$MED_FILE"   ] && basename "$MED_FILE"   || echo "")"
MAIL_BASENAME="$([ -n "$MAIL_FILE"  ] && basename "$MAIL_FILE"  || echo "")"

MEM_MB=$(echo "$MEM" | awk '/G$/{print $1*1024} /M$/{print $1} /^[0-9]+$/{print $1}')
ASTER_MEM=$(( MEM_MB - 512 ))
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512

# Parsing robuste du temps (accepte HH:MM:SS, MM:SS, ou SS)
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

    # ── Fichiers d'entrée (D = données) ──
    echo "F comm ${SCRATCH_DIR}/${COMM_BASENAME}           D  1"
    [ -n "$MED_BASENAME"  ] && echo "F mmed ${SCRATCH_DIR}/${MED_BASENAME}            D 20"
    [ -n "$MAIL_BASENAME" ] && echo "F mail ${SCRATCH_DIR}/${MAIL_BASENAME}           D 20"

    # ── Fichiers de sortie par défaut ──
    echo "F mess ${SCRATCH_DIR}/${STUDY_NAME}.mess         R  6"
    echo "F resu ${SCRATCH_DIR}/${STUDY_NAME}.resu         R  8"
    echo "F rmed ${SCRATCH_DIR}/${STUDY_NAME}_resu.med     R 80"

    # ── Résultats supplémentaires (--results / -R) ──
    #
    #  Format : "type:unite,type:unite,..."
    #
    #  Types reconnus :
    #    rmed  → .med    (résultat MED)
    #    resu  → .resu   (résultat texte)
    #    mess  → .mess   (messages)
    #    csv   → .csv    (tableau CSV)
    #    table → .table  (table ASTER)
    #    dat   → .dat    (données brutes)
    #    pos   → .pos    (post-traitement)
    #
    #  Exemple : --results "rmed:81,csv:38"
    #    → dans le .comm : IMPR_RESU(UNITE=81, ...)
    #                       IMPR_TABLE(UNITE=38, ...)
    #
    if [ -n "$RESULT_UNITS" ]; then
        IFS=',' read -ra RU_LIST <<< "$RESULT_UNITS"
        for ru in "${RU_LIST[@]}"; do
            R_TYPE="${ru%%:*}"
            R_UNIT="${ru##*:}"
            case "$R_TYPE" in
                rmed)  R_EXT="med"   ;;
                resu)  R_EXT="resu"  ;;
                mess)  R_EXT="mess"  ;;
                csv)   R_EXT="csv"   ;;
                table) R_EXT="table" ;;
                dat)   R_EXT="dat"   ;;
                pos)   R_EXT="pos"   ;;
                *)     R_EXT="$R_TYPE" ;;
            esac
            R_FILENAME="${STUDY_NAME}_u${R_UNIT}.${R_EXT}"
            echo "F ${R_TYPE} ${SCRATCH_DIR}/${R_FILENAME}  R ${R_UNIT}"
        done
    fi

    # ── Répertoire de sortie REPE_OUT ──
    echo "R ${SCRATCH_DIR}/REPE_OUT R 0"

} > "$EXPORT_FILE"

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
    info "Nœuds      : $NODES"
    info "Tâches MPI : $NTASKS"
    info "CPUs/tâche : $CPUS"
    info "Mémoire    : $MEM"
    info "Durée max  : $TIME_LIMIT"
    info "Scratch    : $SCRATCH_DIR"
    [ -n "$RESULT_UNITS" ] && info "Résultats+ : $RESULT_UNITS"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  RE-SOUMISSION DE CE MÊME SCRIPT AVEC LES DIRECTIVES SLURM EN LIGNE
# ══════════════════════════════════════════════════════════════════════════════
#
#  Astuce : on ne met PAS de #SBATCH dans le fichier. On passe TOUT en
#  arguments de la commande sbatch. Ainsi :
#    - La phase 1 (ce code-ci) tourne sur le login sans allocation
#    - sbatch re-soumet CE MÊME script avec --partition, --time, etc.
#    - La phase 2 (code ci-dessus) s'active grâce à __ASTER_PHASE=RUN
#
#  LOGS : --output et --error pointent vers le dossier d'étude (work)
#         pour être accessibles immédiatement avec tail -f, sans
#         devoir aller sur le scratch.
#
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Soumission Slurm"

# Chemin absolu vers ce script (pour la re-soumission)
SELF_SCRIPT="$(realpath "$0")"

# Fichiers de log SLURM directement dans le dossier d'étude (work)
SLURM_LOG_OUT="${STUDY_DIR}/aster_%j.out"
SLURM_LOG_ERR="${STUDY_DIR}/aster_%j.err"

JOB_ID=$(sbatch --parsable \
    --job-name="aster_${STUDY_NAME}" \
    --partition="${PARTITION}" \
    --nodes="${NODES}" \
    --ntasks="${NTASKS}" \
    --cpus-per-task="${CPUS}" \
    --mem="${MEM}" \
    --time="${TIME_LIMIT}" \
    --output="${SLURM_LOG_OUT}" \
    --error="${SLURM_LOG_ERR}" \
    --export="ALL,__ASTER_PHASE=RUN,__ASTER_STUDY_DIR=${STUDY_DIR},__ASTER_STUDY_NAME=${STUDY_NAME},__ASTER_SCRATCH_DIR=${SCRATCH_DIR},__ASTER_EXPORT_FILE=${EXPORT_FILE},__ASTER_MEM=${MEM}" \
    "$SELF_SCRIPT")

if [ -z "$JOB_ID" ]; then
    err "Échec de la soumission Slurm."
    exit 1
fi

if $QUIET; then
    echo "$JOB_ID"
else
    # Résoudre le chemin du log avec le vrai job ID
    ACTUAL_LOG_OUT="${STUDY_DIR}/aster_${JOB_ID}.out"
    ACTUAL_LOG_ERR="${STUDY_DIR}/aster_${JOB_ID}.err"

    ok "Job soumis   : ID = ${BOLD}${JOB_ID}${NC}"
    echo ""
    echo -e "  ${BOLD}Commandes utiles :${NC}"
    echo -e "  squeue -j ${JOB_ID}                                   # état du job"
    echo -e "  tail -f ${ACTUAL_LOG_OUT}          # logs temps réel"
    echo -e "  scancel ${JOB_ID}                                      # annuler"
    echo -e "  ls ${SCRATCH_DIR}/                                    # scratch"
    echo -e "  ls ${STUDY_DIR}/                                      # résultats rapatriés"
    echo ""
fi