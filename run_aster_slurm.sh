#!/bin/bash
#SBATCH --job-name=code_aster
#SBATCH --partition=court
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=24:00:00
#SBATCH --output=aster_%j.out
#SBATCH --error=aster_%j.err
#===============================================================================
#  run_aster_slurm — Calcul Code_Aster via sbatch
#===============================================================================
#
#  Usage :
#    sbatch run_aster_slurm.sh                          # valeurs par défaut
#    sbatch --partition=long --time=72:00:00 --mem=32G run_aster_slurm.sh
#    sbatch run_aster_slurm.sh ~/calculs/poutre/
#    sbatch run_aster_slurm.sh -C calcul.comm -M mesh.med
#
#  Les options SLURM (partition, temps, mémoire…) se passent AVANT le nom
#  du script, comme arguments de sbatch. Elles surchargent les #SBATCH ci-dessus.
#
#  Les options du script (fichiers, dossier) se passent APRÈS le nom du script.
#
#  Exemples complets :
#    sbatch --time=02:00:00 --mem=2G --ntasks=2 run_aster_slurm.sh
#    sbatch --time=72:00:00 --mem=32G --ntasks=8 run_aster_slurm.sh ~/calculs/
#    sbatch run_aster_slurm.sh -C mon_calcul.comm -M maillage.med
#    sbatch run_aster_slurm.sh -C mon_calcul.comm -A maillage.mail
#
#  Auteur   : généré pour localcluster
#  Version  : 4.0
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

# ══════════════════════════════════════════════════════════════════════════════
#  FONCTIONS D'AFFICHAGE
# ══════════════════════════════════════════════════════════════════════════════
ts()  { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*"; }
sep() {
    echo ""
    echo "══════════════════════════════════════════════════════════════"
    echo "  $*"
    echo "══════════════════════════════════════════════════════════════"
}

# ══════════════════════════════════════════════════════════════════════════════
#  PARSING DES ARGUMENTS DU SCRIPT (passés après le nom du script dans sbatch)
# ══════════════════════════════════════════════════════════════════════════════
STUDY_DIR=""
COMM_FILE=""
MED_FILE=""
MAIL_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -C|--comm)  COMM_FILE="$2"; shift 2 ;;
        -M|--med)   MED_FILE="$2";  shift 2 ;;
        -A|--mail)  MAIL_FILE="$2"; shift 2 ;;
        -h|--help)
            echo "Usage : sbatch [OPTIONS_SLURM] run_aster_slurm.sh [OPTIONS_SCRIPT] [DOSSIER]"
            echo ""
            echo "OPTIONS SCRIPT :"
            echo "  -C, --comm FILE    Fichier .comm (auto-détecté si absent)"
            echo "  -M, --med  FILE    Fichier .med  (auto-détecté si absent)"
            echo "  -A, --mail FILE    Fichier .mail (auto-détecté si absent)"
            echo ""
            echo "OPTIONS SLURM (à passer AVANT le script) :"
            echo "  sbatch --partition=court --time=02:00:00 --mem=2G --ntasks=2 run_aster_slurm.sh"
            echo "  sbatch --partition=court --time=24:00:00 --mem=8G --ntasks=4 run_aster_slurm.sh"
            echo "  sbatch --partition=court --time=72:00:00 --mem=32G --ntasks=8 run_aster_slurm.sh"
            exit 0
            ;;
        -*)
            log "⚠  Option inconnue ignorée : $1"
            shift ;;
        *)
            STUDY_DIR="$1"; shift ;;
    esac
done

# ══════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 1 : INFORMATIONS DU JOB
# ══════════════════════════════════════════════════════════════════════════════
sep "DÉBUT DU JOB CODE_ASTER"

log "Date           : $(date)"
log "Job ID         : ${SLURM_JOB_ID:-local}"
log "Nœud           : ${SLURM_NODELIST:-$(hostname)}"
log "Partition      : ${SLURM_JOB_PARTITION:-inconnue}"
log "Tâches MPI     : ${SLURM_NTASKS:-1}"
log "CPUs/tâche     : ${SLURM_CPUS_PER_TASK:-1}"
log "Mémoire        : ${SLURM_MEM_PER_NODE:-inconnue} Mo"
log "Temps max      : ${SLURM_TIMELIMIT:-inconnu}"
log "Répertoire     : ${SLURM_SUBMIT_DIR:-$(pwd)}"

# ══════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 2 : DÉTECTION DE L'ÉTUDE
# ══════════════════════════════════════════════════════════════════════════════
sep "DÉTECTION DE L'ÉTUDE"

# Le dossier d'étude : argument, ou SLURM_SUBMIT_DIR, ou pwd
if [ -n "$STUDY_DIR" ]; then
    # Si chemin relatif, le résoudre par rapport à SLURM_SUBMIT_DIR
    if [[ "$STUDY_DIR" != /* ]]; then
        STUDY_DIR="${SLURM_SUBMIT_DIR:-$(pwd)}/${STUDY_DIR}"
    fi
else
    STUDY_DIR="${SLURM_SUBMIT_DIR:-$(pwd)}"
fi

# Normaliser le chemin
STUDY_DIR="$(realpath "$STUDY_DIR" 2>/dev/null || echo "$STUDY_DIR")"
STUDY_NAME="$(basename "$STUDY_DIR")"

if [ ! -d "$STUDY_DIR" ]; then
    log "ERREUR : Dossier introuvable : $STUDY_DIR"
    exit 1
fi

log "Dossier étude  : $STUDY_DIR"
log "Nom étude      : $STUDY_NAME"

# ── Fichier .comm ─────────────────────────────────────────────────────────────
if [ -z "$COMM_FILE" ]; then
    mapfile -t COMM_LIST < <(find "$STUDY_DIR" -maxdepth 1 -name "*.comm" 2>/dev/null | sort)
    case ${#COMM_LIST[@]} in
        0) log "ERREUR : Aucun fichier .comm dans $STUDY_DIR"; exit 1 ;;
        1) COMM_FILE="${COMM_LIST[0]}" ;;
        *) log "ATTENTION : Plusieurs .comm trouvés — utilisation du premier"
           for f in "${COMM_LIST[@]}"; do log "  - $f"; done
           COMM_FILE="${COMM_LIST[0]}" ;;
    esac
else
    # Résoudre chemin relatif
    [[ "$COMM_FILE" != /* ]] && COMM_FILE="${STUDY_DIR}/${COMM_FILE}"
fi
COMM_FILE="$(realpath "$COMM_FILE" 2>/dev/null || echo "$COMM_FILE")"

if [ ! -f "$COMM_FILE" ]; then
    log "ERREUR : Fichier .comm introuvable : $COMM_FILE"
    exit 1
fi
log "Fichier .comm  : $COMM_FILE"

# ── Fichier .med (optionnel) ──────────────────────────────────────────────────
if [ -z "$MED_FILE" ]; then
    mapfile -t MED_LIST < <(find "$STUDY_DIR" -maxdepth 1 -name "*.med" 2>/dev/null | sort)
    case ${#MED_LIST[@]} in
        0) : ;;
        1) MED_FILE="${MED_LIST[0]}" ;;
        *) log "ATTENTION : Plusieurs .med — utilisation du premier"
           MED_FILE="${MED_LIST[0]}" ;;
    esac
else
    [[ "$MED_FILE" != /* ]] && MED_FILE="${STUDY_DIR}/${MED_FILE}"
fi
[ -n "$MED_FILE" ] && MED_FILE="$(realpath "$MED_FILE" 2>/dev/null || echo "$MED_FILE")"
[ -n "$MED_FILE" ] && log "Fichier .med   : $MED_FILE"

# ── Fichier .mail (optionnel) ─────────────────────────────────────────────────
if [ -z "$MAIL_FILE" ]; then
    mapfile -t MAIL_LIST < <(find "$STUDY_DIR" -maxdepth 1 -name "*.mail" 2>/dev/null | sort)
    case ${#MAIL_LIST[@]} in
        0) : ;;
        1) MAIL_FILE="${MAIL_LIST[0]}" ;;
        *) log "ATTENTION : Plusieurs .mail — utilisation du premier"
           MAIL_FILE="${MAIL_LIST[0]}" ;;
    esac
else
    [[ "$MAIL_FILE" != /* ]] && MAIL_FILE="${STUDY_DIR}/${MAIL_FILE}"
fi
[ -n "$MAIL_FILE" ] && MAIL_FILE="$(realpath "$MAIL_FILE" 2>/dev/null || echo "$MAIL_FILE")"
[ -n "$MAIL_FILE" ] && log "Fichier .mail  : $MAIL_FILE"

[ -z "$MED_FILE" ] && [ -z "$MAIL_FILE" ] && \
    log "ATTENTION : Aucun maillage (.med ou .mail) détecté"

# ══════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 3 : PRÉPARATION DU SCRATCH
# ══════════════════════════════════════════════════════════════════════════════
sep "PRÉPARATION DU SCRATCH"

SCRATCH_DIR="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_${SLURM_JOB_ID:-$$}"

log "Création scratch : $SCRATCH_DIR"
mkdir -p "$SCRATCH_DIR"

# ── Copie des fichiers ───────────────────────────────────────────────────────
log "Copie du .comm : $(basename "$COMM_FILE")"
cp "$COMM_FILE" "$SCRATCH_DIR/"

if [ -n "$MED_FILE" ] && [ -f "$MED_FILE" ]; then
    log "Copie du .med  : $(basename "$MED_FILE") ($(du -h "$MED_FILE" | cut -f1))"
    cp "$MED_FILE" "$SCRATCH_DIR/"
fi

if [ -n "$MAIL_FILE" ] && [ -f "$MAIL_FILE" ]; then
    log "Copie du .mail : $(basename "$MAIL_FILE") ($(du -h "$MAIL_FILE" | cut -f1))"
    cp "$MAIL_FILE" "$SCRATCH_DIR/"
fi

# Fichiers annexes (.py, .dat, .para, .include, .mfront)
EXTRA_COUNT=0
for ext in py dat para include mfront; do
    for f in "$STUDY_DIR/"*."$ext"; do
        if [ -f "$f" ]; then
            cp "$f" "$SCRATCH_DIR/"
            log "Copie annexe   : $(basename "$f")"
            (( EXTRA_COUNT++ )) || true
        fi
    done
done
[ "$EXTRA_COUNT" -gt 0 ] && log "$EXTRA_COUNT fichier(s) annexe(s) copié(s)"

log "Contenu du scratch :"
ls -lh "$SCRATCH_DIR/"

# ══════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 4 : GÉNÉRATION DU FICHIER .EXPORT
# ══════════════════════════════════════════════════════════════════════════════
sep "GÉNÉRATION DU FICHIER .EXPORT"

COMM_BASENAME="$(basename "$COMM_FILE")"
MED_BASENAME=""
MAIL_BASENAME=""
[ -n "$MED_FILE"  ] && [ -f "$MED_FILE"  ] && MED_BASENAME="$(basename "$MED_FILE")"
[ -n "$MAIL_FILE" ] && [ -f "$MAIL_FILE" ] && MAIL_BASENAME="$(basename "$MAIL_FILE")"

# Mémoire Aster = mémoire SLURM - 512 Mo de réserve système
SLURM_MEM_MB="${SLURM_MEM_PER_NODE:-4096}"
ASTER_MEM=$(( SLURM_MEM_MB - 512 ))
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512

# Nombre de CPUs depuis SLURM
ASTER_NCPUS="${SLURM_NTASKS:-4}"

# Temps en secondes depuis la variable SLURM (format mm:ss ou hh:mm:ss)
# Fallback : 24h
if [ -n "${SLURM_TIMELIMIT:-}" ]; then
    ASTER_TIME_SEC=$(echo "$SLURM_TIMELIMIT" | awk -F: '
        NF==3 {print $1*3600 + $2*60 + $3; next}
        NF==2 {print $1*60   + $2;         next}
               {print $1*60}')
else
    ASTER_TIME_SEC=86400
fi

EXPORT_FILE="${SCRATCH_DIR}/${STUDY_NAME}.export"
{
    echo "P actions make_etude"
    echo "P mode interactif"
    echo "P version stable"
    echo "P ncpus ${ASTER_NCPUS}"
    echo "P memory_limit ${ASTER_MEM}"
    echo "P time_limit ${ASTER_TIME_SEC}"
    echo ""
    echo "F comm ${SCRATCH_DIR}/${COMM_BASENAME}           D  1"
    [ -n "$MED_BASENAME"  ] && echo "F mmed ${SCRATCH_DIR}/${MED_BASENAME}            D 20"
    [ -n "$MAIL_BASENAME" ] && echo "F mail ${SCRATCH_DIR}/${MAIL_BASENAME}           D 20"
    echo "F mess ${SCRATCH_DIR}/${STUDY_NAME}.mess         R  6"
    echo "F resu ${SCRATCH_DIR}/${STUDY_NAME}.resu         R  8"
    echo "F rmed ${SCRATCH_DIR}/${STUDY_NAME}_resu.med     R 80"
} > "$EXPORT_FILE"

log "Fichier .export généré : $EXPORT_FILE"
log "--- contenu ---"
cat "$EXPORT_FILE"
log "--- fin ---"

# ══════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 5 : TRAP — RAPATRIEMENT AUTOMATIQUE (fin normale, scancel, timeout)
# ══════════════════════════════════════════════════════════════════════════════
rapatrier() {
    sep "RAPATRIEMENT DES RÉSULTATS → $STUDY_DIR"
    local n=0

    for f in \
        "${SCRATCH_DIR}/${STUDY_NAME}.mess" \
        "${SCRATCH_DIR}/${STUDY_NAME}.resu" \
        "${SCRATCH_DIR}/${STUDY_NAME}_resu.med"
    do
        if [ -f "$f" ] && [ -s "$f" ]; then
            cp "$f" "$STUDY_DIR/"
            log "✓ Rapatrié : $(basename "$f")  ($(du -h "$f" | cut -f1))"
            (( n++ )) || true
        fi
    done

    # Copier le .export pour reproductibilité
    [ -f "$EXPORT_FILE" ] && cp "$EXPORT_FILE" "$STUDY_DIR/"

    # Copier les logs SLURM
    for f in "${SLURM_SUBMIT_DIR:-$STUDY_DIR}"/aster_${SLURM_JOB_ID:-$$}.{out,err}; do
        [ -f "$f" ] && cp "$f" "$STUDY_DIR/" 2>/dev/null
    done

    if [ "$n" -gt 0 ]; then
        log "✓ $n fichier(s) rapatrié(s) dans $STUDY_DIR"
    else
        log "⚠  Aucun fichier résultat trouvé dans $SCRATCH_DIR"
    fi

    log "Contenu du dossier d'étude :"
    ls -lh "$STUDY_DIR/"*.{mess,resu,med,export} 2>/dev/null || log "  (aucun résultat)"
}
trap rapatrier EXIT

# ══════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 6 : CHARGEMENT DE CODE_ASTER
# ══════════════════════════════════════════════════════════════════════════════
sep "CHARGEMENT DE CODE_ASTER"

# Charger le module si disponible
if command -v module &>/dev/null && [ -n "${ASTER_MODULE:-}" ]; then
    module load "${ASTER_MODULE}" 2>/dev/null \
        && log "Module '${ASTER_MODULE}' chargé" \
        || log "Module '${ASTER_MODULE}' non disponible"
fi

# Chercher l'exécutable
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
    log "  Vérifiez ASTER_ROOT=$ASTER_ROOT"
    log "  Ou chargez le bon module : module load <nom_module>"
    log "  Exécutables testés :"
    log "    - ${ASTER_ROOT}/bin/run_aster"
    log "    - ${ASTER_ROOT}/bin/as_run"
    log "    - run_aster (PATH)"
    log "    - as_run (PATH)"
    exit 1
fi
log "Exécutable     : $ASTER_EXE"

# ══════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 7 : LANCEMENT DU CALCUL
# ══════════════════════════════════════════════════════════════════════════════
sep "LANCEMENT DU CALCUL"

START_TIME=$(date +%s)
log "Démarrage : $(date)"

NTASKS_RUN="${SLURM_NTASKS:-1}"
if [ "$NTASKS_RUN" -gt 1 ]; then
    log "Mode parallèle MPI ($NTASKS_RUN processus)"
    srun --mpi=pmi2 "$ASTER_EXE" "$EXPORT_FILE"
else
    log "Mode séquentiel"
    "$ASTER_EXE" "$EXPORT_FILE"
fi
ASTER_RC=$?

END_TIME=$(date +%s)
ELAPSED=$(( END_TIME - START_TIME ))
ELAPSED_FMT=$(printf '%02dh %02dm %02ds' $((ELAPSED/3600)) $((ELAPSED%3600/60)) $((ELAPSED%60)))

# ══════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 8 : DIAGNOSTIC DU FICHIER .MESS
# ══════════════════════════════════════════════════════════════════════════════
sep "DIAGNOSTIC"

MESS_PATH="${SCRATCH_DIR}/${STUDY_NAME}.mess"
if [ -f "$MESS_PATH" ]; then
    NB_ALARM=$(grep -c "<A>" "$MESS_PATH" 2>/dev/null || echo 0)
    NB_FATAL=$(grep -c "<F>" "$MESS_PATH" 2>/dev/null || echo 0)
    NB_EXCEP=$(grep -c "<S>" "$MESS_PATH" 2>/dev/null || echo 0)

    log "Alarmes <A>        : $NB_ALARM"
    log "Erreurs fatales <F>: $NB_FATAL"
    log "Exceptions <S>     : $NB_EXCEP"

    if [ "$NB_FATAL" -gt 0 ]; then
        log ""
        log "--- Première erreur fatale <F> ---"
        grep -B3 -A8 "<F>" "$MESS_PATH" | head -25
        log "--- fin ---"
    fi

    log ""
    log "--- Dernières lignes du .mess ---"
    tail -15 "$MESS_PATH"
    log "--- fin ---"
else
    log "⚠  Fichier .mess non trouvé (le calcul a peut-être échoué au démarrage)"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  ÉTAPE 9 : RÉSUMÉ FINAL
# ══════════════════════════════════════════════════════════════════════════════
sep "RÉSUMÉ FINAL"

if [ "$ASTER_RC" -eq 0 ]; then
    log "Statut         : ✓ SUCCÈS"
else
    log "Statut         : ✗ ÉCHEC (code $ASTER_RC)"
fi
log "Étude          : $STUDY_NAME"
log "Durée          : $ELAPSED_FMT"
log "Scratch        : $SCRATCH_DIR"
log "Résultats →    : $STUDY_DIR"
log "Job ID         : ${SLURM_JOB_ID:-local}"
log "Fin            : $(date)"

# Le trap EXIT appellera rapatrier() automatiquement
exit $ASTER_RC
