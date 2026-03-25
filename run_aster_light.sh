#!/bin/bash
#===============================================================================
#  run_aster_light.sh — Version minimale pour debug
#
#  Usage :  bash run_aster_light.sh DOSSIER_ETUDE
#
#  Pré-requis : un dossier contenant un .comm et un .med ou .mail
#===============================================================================

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG — adapter ici
# ══════════════════════════════════════════════════════════════════════════════
ASTER_ROOT="${ASTER_ROOT:-/opt/code_aster}"
ASTER_MODULE="${ASTER_MODULE:-code_aster}"
SCRATCH_BASE="${SCRATCH_BASE:-/scratch}"

PARTITION="court"
MEM="5G"
TIME="05:00:00"

# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 2 : EXÉCUTION (nœud de calcul)
# ══════════════════════════════════════════════════════════════════════════════
if [ "${__PHASE:-}" = "RUN" ]; then

    echo "=== DÉBUT CALCUL — $(date) ==="
    echo "Job      : $SLURM_JOB_ID"
    echo "Nœud     : $SLURM_NODELIST"
    echo "Scratch  : $__SCRATCH_DIR"
    echo "Export   : $__EXPORT_FILE"
    echo ""

    # Debug : afficher l'environnement
    echo "=== ENVIRONNEMENT ==="
    echo "PATH=$PATH"
    echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-<vide>}"
    echo "which run_aster : $(command -v run_aster 2>/dev/null || echo 'introuvable')"
    echo "which as_run    : $(command -v as_run 2>/dev/null || echo 'introuvable')"
    echo ""

    # Charger le module
    echo "=== CHARGEMENT MODULE ==="
    if command -v module &>/dev/null; then
        module load "$__MODULE" 2>&1 && echo "Module '$__MODULE' chargé" || echo "Module '$__MODULE' ÉCHEC"
    else
        echo "Pas de système de modules"
    fi
    echo ""

    # Trouver l'exécutable
    ASTER_EXE=""
    for c in "${__ROOT}/bin/run_aster" "${__ROOT}/bin/as_run" \
             "$(command -v run_aster 2>/dev/null || true)" \
             "$(command -v as_run 2>/dev/null || true)"; do
        [ -n "$c" ] && [ -x "$c" ] && { ASTER_EXE="$c"; break; }
    done

    if [ -z "$ASTER_EXE" ]; then
        echo "ERREUR : Code_Aster introuvable !"
        echo "  Contenu de ${__ROOT}/bin/ :"
        ls -la "${__ROOT}/bin/" 2>/dev/null || echo "  Dossier inexistant"
        exit 1
    fi
    echo "Exécutable : $ASTER_EXE"
    echo "$ASTER_EXE --version :"
    "$ASTER_EXE" --version 2>&1 || true
    echo ""

    # Lancer le calcul
    echo "=== LANCEMENT ==="
    echo "Commande : $ASTER_EXE $__EXPORT_FILE"
    echo ""
    "$ASTER_EXE" "$__EXPORT_FILE"
    RC=$?
    echo ""
    echo "=== FIN — code retour : $RC — $(date) ==="

    # Rapatrier les résultats
    DEST="$__STUDY_DIR/run_${SLURM_JOB_ID}"
    mkdir -p "$DEST" 2>/dev/null
    cp -v "$__SCRATCH_DIR"/*.{mess,resu,med,csv} "$DEST/" 2>/dev/null || true
    cp -rv "$__SCRATCH_DIR/REPE_OUT" "$DEST/" 2>/dev/null || true
    echo "Résultats copiés dans : $DEST"

    # Nettoyage scratch
    rm -rf "$__SCRATCH_DIR" 2>/dev/null && echo "Scratch supprimé" || true

    exit $RC
fi

# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 1 : PRÉPARATION (nœud login)
# ══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

STUDY_DIR="${1:-.}"
STUDY_DIR="$(realpath "$STUDY_DIR")"
STUDY_NAME="$(basename "$STUDY_DIR")"

echo "Étude    : $STUDY_DIR"

# Trouver les fichiers
COMM=$(find "$STUDY_DIR" -maxdepth 1 -name "*.comm" | head -1)
MED=$(find "$STUDY_DIR" -maxdepth 1 -name "*.med" | head -1)
MAIL=$(find "$STUDY_DIR" -maxdepth 1 -name "*.mail" | head -1)

[ -z "$COMM" ] && { echo "ERREUR : pas de .comm dans $STUDY_DIR"; exit 1; }
echo "Comm     : $COMM"
[ -n "$MED" ]  && echo "Med      : $MED"
[ -n "$MAIL" ] && echo "Mail     : $MAIL"

# Créer le scratch
SCRATCH="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_$(date +%s)_$$"
mkdir -p "$SCRATCH"
echo "Scratch  : $SCRATCH"

# Copier les fichiers
cp "$COMM" "$SCRATCH/"
[ -n "$MED" ]  && cp "$MED" "$SCRATCH/"
[ -n "$MAIL" ] && cp "$MAIL" "$SCRATCH/"
# Fichiers annexes
for f in "$STUDY_DIR"/*.{py,dat,para,include,mfront} 2>/dev/null; do
    [ -f "$f" ] && cp "$f" "$SCRATCH/"
done

# Mémoire en MB
MEM_MB=$(echo "$MEM" | awk 'tolower($0)~/g$/{gsub(/[gG]/,"");print int($0*1024);next} {gsub(/[mM]/,"");print int($0)}')
ASTER_MEM=$(( MEM_MB - 512 ))
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512

# Temps en secondes
TIME_SEC=$(echo "$TIME" | awk -F'[-:]' 'NF==4{print $1*86400+$2*3600+$3*60+$4;next} NF==3{print $1*3600+$2*60+$3;next} {print $1*60}')

# Générer le .export
EXPORT="${SCRATCH}/${STUDY_NAME}.export"
{
    echo "P actions make_etude"
    echo "P mode interactif"
    echo "P version stable"
    echo "P ncpus 1"
    echo "P memory_limit ${ASTER_MEM}"
    echo "P time_limit ${TIME_SEC}"
    echo ""
    echo "F comm ${SCRATCH}/$(basename "$COMM") D 1"
    [ -n "$MED" ]  && echo "F mmed ${SCRATCH}/$(basename "$MED")  D 20"
    [ -n "$MAIL" ] && echo "F mail ${SCRATCH}/$(basename "$MAIL") D 20"
    echo "F mess ${SCRATCH}/${STUDY_NAME}.mess  R 6"
    echo "F resu ${SCRATCH}/${STUDY_NAME}.resu  R 8"
    echo "F rmed ${SCRATCH}/${STUDY_NAME}_resu.med R 80"
    echo "R ${SCRATCH}/REPE_OUT R 0"
} > "$EXPORT"

echo ""
echo "Export   : $EXPORT"
cat "$EXPORT"
echo ""

# Soumettre
SELF="$(realpath "$0")"
JOB_ID=$(sbatch --parsable \
    --job-name="aster_${STUDY_NAME}" \
    --partition="${PARTITION}" \
    --nodes=1 \
    --ntasks=1 \
    --mem="${MEM}" \
    --time="${TIME}" \
    --output="${STUDY_DIR}/aster_%j.out" \
    --error="${STUDY_DIR}/aster_%j.err" \
    --export="ALL,__PHASE=RUN,__STUDY_DIR=${STUDY_DIR},__SCRATCH_DIR=${SCRATCH},__EXPORT_FILE=${EXPORT},__ROOT=${ASTER_ROOT},__MODULE=${ASTER_MODULE}" \
    "$SELF")

echo "Job soumis : $JOB_ID"
echo ""
echo "  squeue -j $JOB_ID"
echo "  tail -f ${STUDY_DIR}/aster_${JOB_ID}.out"
echo "  scancel $JOB_ID"
