#!/bin/bash
#===============================================================================
#  run_aster — Calcul Code_Aster via sbatch (fichier unique)
#===============================================================================
#
#  Usage :  bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]
#
#  Le script fonctionne en DEUX PHASES dans un seul fichier :
#
#    PHASE 1 (nœud login) : bash lance ce script sans directives #SBATCH.
#      Il détecte qu'on est en phase de préparation (__ASTER_PHASE non défini),
#      prépare le scratch, copie les fichiers, génère le .export, puis se
#      RE-SOUMET LUI-MÊME avec les bons paramètres via sbatch --partition=...
#
#    PHASE 2 (nœud de calcul) : le script détecte __ASTER_PHASE=RUN
#      (transmis via --export), charge Code_Aster et lance le calcul.
#      À la fin (ou en cas de scancel/timeout), un trap rapatrie
#      les résultats vers le dossier d'étude d'origine et supprime
#      automatiquement le scratch.
#
#  Auteur   : Téo LEROY
#  Version  : 7.0
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

# Type MPI pour srun (vérifier avec : srun --mpi=list)
# Valeurs courantes : pmi2, pmix, cray_shasta, none
MPI_TYPE="${MPI_TYPE:-cray_shasta}"

# ── Valeurs par défaut Slurm ─────────────────────────────────────────────────
DEFAULT_PARTITION="court"
DEFAULT_NODES=1
DEFAULT_NTASKS=1
DEFAULT_CPUS_PER_TASK=1
DEFAULT_MEM="5G"
DEFAULT_TIME="05:00:00"

# ── Préréglages de ressources (-P court | -P moyen | -P long) ────────────────
PRESET_COURT_PARTITION="court"  ; PRESET_COURT_NTASKS=1 ; PRESET_COURT_MEM="2G"  ; PRESET_COURT_TIME="05:00:00"
PRESET_MOYEN_PARTITION="moyen"  ; PRESET_MOYEN_NTASKS=1 ; PRESET_MOYEN_MEM="8G"  ; PRESET_MOYEN_TIME="03-00:00:00"
PRESET_LONG_PARTITION="long"    ; PRESET_LONG_NTASKS=1  ; PRESET_LONG_MEM="32G"  ; PRESET_LONG_TIME="30-00:00:00"

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
USAGE
  bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]

  Lance un calcul Code_Aster via Slurm.
  Les fichiers sont copiés dans ${SCRATCH_BASE}/\$USER/ avant soumission.
  Par défaut, DOSSIER_ETUDE = répertoire courant.
  Le scratch est automatiquement supprimé après rapatriement des résultats.

FICHIERS
  -C, --comm FILE      Fichier .comm  (auto-détecté si absent)
  -M, --med  FILE      Fichier .med   (auto-détecté si absent)
  -A, --mail FILE      Fichier .mail  (format ASTER natif, auto-détecté si absent)

RÉSULTATS SUPPLÉMENTAIRES
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

RESSOURCES SLURM
  -p, --partition NOM  Partition        [défaut: ${DEFAULT_PARTITION}]
  -n, --nodes     N    Nombre de nœuds  [défaut: ${DEFAULT_NODES}]
  -t, --ntasks    N    Tâches MPI       [défaut: ${DEFAULT_NTASKS}]
  -c, --cpus      N    CPUs par tâche   [défaut: ${DEFAULT_CPUS_PER_TASK}]
  -m, --mem       MEM  Mémoire/nœud     [défaut: ${DEFAULT_MEM}]
  -T, --time  H:M:S    Durée max        [défaut: ${DEFAULT_TIME}]

PRÉRÉGLAGES
  -P, --preset NOM     Préréglage de ressources (court, moyen, long)
                         court : ${PRESET_COURT_NTASKS} tâche(s), ${PRESET_COURT_MEM}, ${PRESET_COURT_TIME}
                         moyen : ${PRESET_MOYEN_NTASKS} tâche(s), ${PRESET_MOYEN_MEM}, ${PRESET_MOYEN_TIME}
                         long  : ${PRESET_LONG_NTASKS} tâche(s), ${PRESET_LONG_MEM}, ${PRESET_LONG_TIME}
                       Les options passées après -P surchargent le préréglage.

OPTIONS
  -q, --quiet          Sortie minimale (juste le job ID)
      --keep-scratch   NE PAS supprimer le scratch après rapatriement
                       (par défaut le scratch est toujours supprimé)
      --dry-run        Afficher la commande sbatch sans la lancer
      --debug          Activer le mode debug bash (set -x) en phase d'exécution
  -h, --help           Afficher cette aide

EXEMPLES
  bash run_aster.sh                              # Étude dans le dossier courant
  bash run_aster.sh ~/calculs/poutre/            # Spécifier le dossier
  bash run_aster.sh -P court                     # Préréglage court
  bash run_aster.sh -P moyen                     # Préréglage moyen
  bash run_aster.sh -P long                      # Préréglage long
  bash run_aster.sh -P moyen -t 8                # Moyen mais 8 tâches MPI
  bash run_aster.sh -n 2 -t 8 -m 8G             # Personnalisé
  bash run_aster.sh -p debug -T 01:00:00         # Partition debug, 1h max
  bash run_aster.sh -C mon_calcul.comm -M maillage.med
  bash run_aster.sh -P moyen --results "rmed:81,csv:38"
  bash run_aster.sh -P long --keep-scratch       # Garder le scratch
  bash run_aster.sh --debug -P court             # Mode debug verbose
  bash run_aster.sh --dry-run -P moyen           # Voir la commande sans lancer
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

    # ── Sécurité Bash en phase d'exécution ────────────────────────────────────
    set -uo pipefail

    # ── Mode debug : active la trace bash si demandé ──────────────────────────
    [ "${__ASTER_DEBUG:-0}" = "1" ] && set -x

    # ── Variable pour éviter un double rapatriement ───────────────────────────
    __RAPATRIE_DONE=0

    # ── Fonction utilitaire : rapatrier un fichier via rsync ─────────────────
    #  Usage : _rsync_result SRC DEST_DIR
    _rsync_result() {
        local src="$1" dest="$2"
        if rsync -av "$src" "$dest/" > >(while read -r l; do log "$l"; done) 2>&1; then
            return 0
        else
            log "⚠ Échec rsync : $src → $dest"
            return 1
        fi
    }

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

        # Sous-dossier unique par job pour éviter les collisions de fichiers
        local dest="${__ASTER_STUDY_DIR}/run_${SLURM_JOB_ID}"
        local n=0

        # Créer le dossier de destination dédié à ce job
        if ! mkdir -p "$dest" 2>/dev/null; then
            log "⚠ Impossible de créer $dest — résultats restent dans le scratch"
            log "  Scratch : $__ASTER_SCRATCH_DIR"
            return
        fi
        log "Destination : $dest"

        # Rapatrier TOUS les fichiers de résultat par extension
        shopt -s nullglob
        for ext in mess resu med csv table dat pos rmed txt vtu vtk py; do
            for f in "${__ASTER_SCRATCH_DIR}"/*."${ext}"; do
                if [ -f "$f" ] && [ -s "$f" ]; then
                    _rsync_result "$f" "$dest" && (( n++ )) || true
                fi
            done
        done
        shopt -u nullglob

        # Rapatrier REPE_OUT si présent
        if [ -d "${__ASTER_SCRATCH_DIR}/REPE_OUT" ]; then
            _rsync_result "${__ASTER_SCRATCH_DIR}/REPE_OUT" "$dest" && (( n++ )) || true
        fi

        if [ "$n" -eq 0 ]; then
            log "⚠ Aucun fichier résultat trouvé dans ${__ASTER_SCRATCH_DIR}"
            log "  Contenu du scratch :"
            ls -la "${__ASTER_SCRATCH_DIR}/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done
        else
            log "✓ $n fichier(s) rapatrié(s) vers $dest"
        fi

        log ""
        log "Résultats dans : $dest"
        shopt -s nullglob
        for f in "$dest"/*.{mess,resu,med,csv,table,dat,pos,rmed,txt,vtu,vtk}; do
            log "  $(ls -lh "$f")"
        done
        shopt -u nullglob

        # ── Lien symbolique "latest" vers le dernier run ─────────────────────
        local latest_link="${__ASTER_STUDY_DIR}/latest"
        rm -f "$latest_link" 2>/dev/null
        if ln -s "run_${SLURM_JOB_ID}" "$latest_link" 2>/dev/null; then
            log "✓ Lien symbolique : latest → run_${SLURM_JOB_ID}"
        fi

        # ── Nettoyage automatique du scratch ─────────────────────────────────
        if [ "${__ASTER_KEEP_SCRATCH:-0}" != "1" ]; then
            log ""
            log "Nettoyage du scratch : $__ASTER_SCRATCH_DIR"
            if rm -rf "$__ASTER_SCRATCH_DIR"; then
                log "✓ Scratch supprimé"
            else
                log "⚠ Échec suppression scratch (droits insuffisants ?)"
            fi
        else
            log ""
            log "Scratch conservé (--keep-scratch) : $__ASTER_SCRATCH_DIR"
        fi
    }

    # Trapper SIGTERM (envoyé par scancel / timeout SLURM) ET EXIT
    trap rapatrier EXIT
    trap 'rapatrier; exit 143' SIGTERM

    # ── Infos de démarrage ────────────────────────────────────────────────────
    sep "DÉBUT CALCUL CODE_ASTER — $(date)"
    log "Job ID         : $SLURM_JOB_ID"
    log "Étude          : $__ASTER_STUDY_NAME"
    log "Scratch        : $__ASTER_SCRATCH_DIR"
    log "Destination    : ${__ASTER_STUDY_DIR}/run_${SLURM_JOB_ID}"
    log "Nœuds alloués  : $SLURM_NODELIST"
    log "Tâches MPI     : $SLURM_NTASKS"
    log "CPUs par tâche : ${SLURM_CPUS_PER_TASK:-1}"
    log "Mémoire        : $__ASTER_MEM"
    log "Keep scratch   : ${__ASTER_KEEP_SCRATCH:-0}"

    # ── Chargement de Code_Aster ──────────────────────────────────────────────
    sep "CHARGEMENT CODE_ASTER"

    ASTER_LOADED=0
    if command -v module &>/dev/null && [ -n "${__ASTER_MODULE:-}" ]; then
        if module load "${__ASTER_MODULE}" 2>/dev/null; then
            log "Module '${__ASTER_MODULE}' chargé."
            ASTER_LOADED=1
        else
            warn "Module '${__ASTER_MODULE}' non disponible — recherche manuelle de l'exécutable."
        fi
    fi

    # Recherche de l'exécutable Code_Aster dans les emplacements courants
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
        err "  → Définir ASTER_ROOT=/chemin/code_aster avant de relancer."
        err "  → Ou charger un module Lmod (ASTER_MODULE)."
        exit 1
    fi
    log "Exécutable : $ASTER_EXE"

    # Vérification de cohérence : si le module a échoué mais qu'on a trouvé
    # un binaire, avertir l'utilisateur
    if [ "$ASTER_LOADED" -eq 0 ] && [ -n "${__ASTER_MODULE:-}" ]; then
        warn "Le module n'a pas été chargé — le binaire trouvé pourrait ne pas"
        warn "avoir le bon environnement (LD_LIBRARY_PATH, etc.)."
    fi

    # Vérification de version : détecte un binaire absent ou incompatible
    ASTER_VERSION=$("$ASTER_EXE" --version 2>&1 | head -1) || true
    if [ -z "$ASTER_VERSION" ]; then
        warn "Impossible d'obtenir la version de Code_Aster (binaire incompatible ou muet ?)"
    else
        log "Version    : $ASTER_VERSION"
    fi

    # ── Lancement du calcul ───────────────────────────────────────────────────
    sep "CALCUL EN COURS"
    log "Démarrage : $(date)"

    # On désactive set -e pour capturer proprement le code retour d'Aster.
    # Avec set -e, certains cas (pipes, sous-shells) peuvent provoquer
    # un arrêt prématuré avant que le code retour ne soit capturé.
    #
    # IMPORTANT : run_aster / as_run sont eux-mêmes des lanceurs qui gèrent
    # MPI en interne (via mpiexec). Les appeler via srun provoque un conflit
    # de double pilotage MPI (erreur "Unreachable in file client.c").
    # On les appelle donc DIRECTEMENT et c'est le .export (ncpus) qui
    # détermine le mode séquentiel ou parallèle.
    ASTER_RC=0
    set +e
    log "Lancement : $ASTER_EXE $__ASTER_EXPORT_FILE"
    log "  (ncpus=${SLURM_NTASKS:-1} — le parallélisme est géré par run_aster via le .export)"
    "$ASTER_EXE" "$__ASTER_EXPORT_FILE"
    ASTER_RC=$?
    set -e
    log "Exécution terminée : $(date) — code retour : $ASTER_RC"

    # ── Diagnostic rapide du .mess ────────────────────────────────────────────
    sep "DIAGNOSTIC"
    MESS_PATH="${__ASTER_SCRATCH_DIR}/${__ASTER_STUDY_NAME}.mess"
    NB_ALARM=0; NB_FATAL=0; NB_EXCEP=0
    if [ -f "$MESS_PATH" ]; then
        NB_ALARM=$(grep -c "<A>" "$MESS_PATH" 2>/dev/null || true)
        NB_FATAL=$(grep -c "<F>" "$MESS_PATH" 2>/dev/null || true)
        NB_EXCEP=$(grep -c "<S>" "$MESS_PATH" 2>/dev/null || true)
        log "Alarmes <A>        : $NB_ALARM"
        log "Erreurs fatales <F>: $NB_FATAL"
        log "Exceptions <S>     : $NB_EXCEP"
        if [ "$NB_FATAL" -gt 0 ]; then
            log "--- Première erreur fatale ---"
            grep -B2 -A5 "<F>" "$MESS_PATH" | head -20 || true
            log "--- fin ---"
        fi
        if [ "$ASTER_RC" -ne 0 ] && [ "$NB_FATAL" -eq 0 ]; then
            log "⚠ Code_Aster a échoué (rc=$ASTER_RC) sans erreur <F> visible dans le .mess"
            log "  → Vérifier les dernières lignes du .mess :"
            tail -20 "$MESS_PATH" | while IFS= read -r l; do log "  $l"; done
        fi
    else
        log "⚠ Fichier .mess non trouvé — échec au démarrage de Code_Aster ?"
        log "  Contenu du scratch :"
        ls -la "${__ASTER_SCRATCH_DIR}/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done
    fi

    # ── Rapatriement explicite scratch → work ─────────────────────────────────
    rapatrier

    # ── Résumé des ressources consommées (sacct) ──────────────────────────────
    sep "RESSOURCES UTILISÉES"
    if command -v sacct &>/dev/null; then
        sacct -j "$SLURM_JOB_ID" \
              --format=JobID,JobName%20,Elapsed,CPUTime,MaxRSS,MaxVMSize,State,ExitCode \
              2>/dev/null | while IFS= read -r l; do log "$l"; done \
            || log "⚠ sacct : données non disponibles"
    else
        log "sacct non disponible sur ce système"
    fi

    # ── Résumé final ──────────────────────────────────────────────────────────
    sep "RÉSUMÉ FINAL"
    if [ "$ASTER_RC" -eq 0 ]; then
        log "Statut    : SUCCÈS ✓"
    else
        log "Statut    : ÉCHEC  ✗  (code $ASTER_RC)"
    fi
    log "Étude     : $__ASTER_STUDY_NAME"
    log "Résultats : ${__ASTER_STUDY_DIR}/run_${SLURM_JOB_ID}"
    log "Scratch   : $__ASTER_SCRATCH_DIR"
    log "Fin       : $(date)"
    log "Alarmes <A> : $NB_ALARM  |  Fatales <F> : $NB_FATAL  |  Exceptions <S> : $NB_EXCEP"

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
OPT_KEEP_SCRATCH=0   # --keep-scratch : ne PAS supprimer le scratch
OPT_DRY_RUN=0        # --dry-run : afficher la commande sans lancer
OPT_DEBUG=0           # --debug : activer set -x en phase d'exécution

while [[ $# -gt 0 ]]; do
    case "$1" in
        -C|--comm)          COMM_FILE="$2";    shift 2 ;;
        -M|--med)           MED_FILE="$2";     shift 2 ;;
        -A|--mail)          MAIL_FILE="$2";    shift 2 ;;
        -R|--results)       RESULT_UNITS="$2"; shift 2 ;;
        -P|--preset)        PRESET="$2";       shift 2 ;;
        -p|--partition)     PARTITION="$2";    shift 2 ;;
        -n|--nodes)         NODES="$2";        shift 2 ;;
        -t|--ntasks)        NTASKS="$2";       shift 2 ;;
        -c|--cpus)          CPUS="$2";         shift 2 ;;
        -m|--mem)           MEM="$2";          shift 2 ;;
        -T|--time)          TIME_LIMIT="$2";   shift 2 ;;
        -q|--quiet)         QUIET=true;        shift ;;
        --keep-scratch)     OPT_KEEP_SCRATCH=1; shift ;;
        --dry-run)          OPT_DRY_RUN=1;     shift ;;
        --debug)            OPT_DEBUG=1;       shift ;;
        -h|--help)          usage ;;
        -*)                 err "Option inconnue : $1"; echo ""; usage ;;
        *)                  STUDY_DIR="$1";    shift ;;
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
            $QUIET || info "Préréglage : court  (${PRESET_COURT_NTASKS} tâche(s), ${PRESET_COURT_MEM}, ${PRESET_COURT_TIME})"
            ;;
        moyen|medium)
            : "${PARTITION:=$PRESET_MOYEN_PARTITION}"
            : "${NTASKS:=$PRESET_MOYEN_NTASKS}"
            : "${MEM:=$PRESET_MOYEN_MEM}"
            : "${TIME_LIMIT:=$PRESET_MOYEN_TIME}"
            $QUIET || info "Préréglage : moyen  (${PRESET_MOYEN_NTASKS} tâche(s), ${PRESET_MOYEN_MEM}, ${PRESET_MOYEN_TIME})"
            ;;
        long)
            : "${PARTITION:=$PRESET_LONG_PARTITION}"
            : "${NTASKS:=$PRESET_LONG_NTASKS}"
            : "${MEM:=$PRESET_LONG_MEM}"
            : "${TIME_LIMIT:=$PRESET_LONG_TIME}"
            $QUIET || info "Préréglage : long   (${PRESET_LONG_NTASKS} tâche(s), ${PRESET_LONG_MEM}, ${PRESET_LONG_TIME})"
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
    shopt -s nullglob
    COMM_LIST=("$STUDY_DIR"/*.comm)
    shopt -u nullglob
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
    shopt -s nullglob
    MED_LIST=("$STUDY_DIR"/*.med)
    shopt -u nullglob
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
    shopt -s nullglob
    MAIL_LIST=("$STUDY_DIR"/*.mail)
    shopt -u nullglob
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

# Timestamp + PID pour garantir l'unicité même si deux jobs sont lancés
# dans la même seconde sur la même étude
TIMESTAMP="$(date +%Y%m%d_%H%M%S)_$$"
SCRATCH_DIR="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_${TIMESTAMP}"

$QUIET || info "Création : $SCRATCH_DIR"
mkdir -p "$SCRATCH_DIR" || { err "Impossible de créer le dossier scratch : $SCRATCH_DIR"; exit 1; }

# ── Fonction de copie unifiée (rsync, plus fiable que cp sur HPC) ─────────────
# Usage : _copy_to_scratch SRC
_copy_to_scratch() {
    local src="$1"
    rsync -a "$src" "$SCRATCH_DIR/" \
        || { err "Échec copie vers scratch : $src"; exit 1; }
    $QUIET || ok "Copié : $(basename "$src")"
}

# ── Copie des fichiers principaux ─────────────────────────────────────────────
_copy_to_scratch "$COMM_FILE"
[ -n "$MED_FILE"  ] && _copy_to_scratch "$MED_FILE"
[ -n "$MAIL_FILE" ] && _copy_to_scratch "$MAIL_FILE"

# ── Fichiers annexes optionnels (.py, .dat, .para, .include, .mfront) ─────────
shopt -s nullglob
for ext in py dat para include mfront; do
    for f in "$STUDY_DIR/"*."$ext"; do
        _copy_to_scratch "$f"
    done
done
shopt -u nullglob

# ══════════════════════════════════════════════════════════════════════════════
#  PARSING ROBUSTE DE LA MÉMOIRE
#  Formats acceptés : 4G, 4g, 1.5G, 4096M, 4096m, 4096 (entier → supposé MB)
# ══════════════════════════════════════════════════════════════════════════════
MEM_MB=$(echo "$MEM" | awk '
    tolower($0) ~ /^[0-9]+(\.[0-9]+)?g$/ { gsub(/[gGiI]/, ""); print int($0 * 1024); next }
    tolower($0) ~ /^[0-9]+(\.[0-9]+)?m$/ { gsub(/[mMiI]/, ""); print int($0);        next }
    /^[0-9]+$/                            { print int($0);                             next }
    { print -1 }
')

if [ "$MEM_MB" -le 0 ] 2>/dev/null; then
    err "Format mémoire non reconnu : '$MEM'  (attendu : 4G, 1.5G, 512M, ou entier en MB)"
    exit 1
fi

# Réserver 512 MB pour le système ; plancher à 512 MB pour Code_Aster
ASTER_MEM=$(( MEM_MB - 512 ))
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512

# ── Parsing robuste du temps (accepte JJ-HH:MM:SS, HH:MM:SS, MM:SS, SS) ──────
TIME_LIMIT_SEC=$(echo "$TIME_LIMIT" | awk -F'[-:]' '
    NF==4 {print $1*86400 + $2*3600 + $3*60 + $4; next}
    NF==3 {print $1*3600  + $2*60   + $3;         next}
    NF==2 {print $1*60    + $2;                   next}
           {print $1*60}')

# ══════════════════════════════════════════════════════════════════════════════
#  GÉNÉRATION DU FICHIER .EXPORT
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Génération du fichier .export"

COMM_BASENAME="$(basename "$COMM_FILE")"
MED_BASENAME="$([ -n "$MED_FILE"   ] && basename "$MED_FILE"   || echo "")"
MAIL_BASENAME="$([ -n "$MAIL_FILE"  ] && basename "$MAIL_FILE"  || echo "")"

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

} > "$EXPORT_FILE" || { err "Échec écriture du fichier .export : $EXPORT_FILE"; exit 1; }

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
    info "Mémoire    : $MEM  (${ASTER_MEM} MB alloués à Code_Aster)"
    info "Durée max  : $TIME_LIMIT"
    info "Scratch    : $SCRATCH_DIR"
    [ "$OPT_KEEP_SCRATCH" = "1" ] && info "Scratch    : conservé après rapatriement (--keep-scratch)"
    [ "$OPT_KEEP_SCRATCH" = "0" ] && info "Scratch    : supprimé automatiquement après rapatriement"
    [ "$OPT_DEBUG" = "1" ] && info "Debug      : set -x actif en phase d'exécution"
    [ -n "$RESULT_UNITS"  ] && info "Résultats+ : $RESULT_UNITS"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  RE-SOUMISSION DE CE MÊME SCRIPT AVEC LES DIRECTIVES SLURM EN LIGNE
# ══════════════════════════════════════════════════════════════════════════════
#
#  On passe uniquement les variables __ASTER_* nécessaires au nœud de calcul
#  au lieu de --export=ALL, pour éviter les conflits d'environnement
#  (PATH, LD_LIBRARY_PATH, etc.) entre le nœud login et les nœuds de calcul.
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

# Variables exportées : variables __ASTER_* + environnement système essentiel.
# Sur Cray/HPC, les nœuds de calcul ont un PATH minimal — il faut
# transmettre PATH, LD_LIBRARY_PATH, etc. pour que les commandes de base
# (grep, date, tee, rsync...) et les bibliothèques soient accessibles.
EXPORT_VARS="__ASTER_PHASE=RUN"
EXPORT_VARS+=",__ASTER_STUDY_DIR=${STUDY_DIR}"
EXPORT_VARS+=",__ASTER_STUDY_NAME=${STUDY_NAME}"
EXPORT_VARS+=",__ASTER_SCRATCH_DIR=${SCRATCH_DIR}"
EXPORT_VARS+=",__ASTER_EXPORT_FILE=${EXPORT_FILE}"
EXPORT_VARS+=",__ASTER_MEM=${MEM}"
EXPORT_VARS+=",__ASTER_ROOT=${ASTER_ROOT}"
EXPORT_VARS+=",__ASTER_MODULE=${ASTER_MODULE}"
EXPORT_VARS+=",__ASTER_KEEP_SCRATCH=${OPT_KEEP_SCRATCH}"
EXPORT_VARS+=",__ASTER_DEBUG=${OPT_DEBUG}"
EXPORT_VARS+=",__ASTER_MPI_TYPE=${MPI_TYPE}"
# Environnement système indispensable sur les nœuds de calcul
EXPORT_VARS+=",PATH=${PATH}"
EXPORT_VARS+=",HOME=${HOME}"
EXPORT_VARS+=",USER=${USER}"
EXPORT_VARS+=",SHELL=${SHELL:-/bin/bash}"
[ -n "${LD_LIBRARY_PATH:-}" ] && EXPORT_VARS+=",LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
[ -n "${PYTHONPATH:-}"      ] && EXPORT_VARS+=",PYTHONPATH=${PYTHONPATH}"
[ -n "${MODULEPATH:-}"      ] && EXPORT_VARS+=",MODULEPATH=${MODULEPATH}"

# Construction de la commande sbatch
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

# ── Mode dry-run : afficher la commande sans lancer ───────────────────────────
if [ "$OPT_DRY_RUN" = "1" ]; then
    section "DRY RUN — commande sbatch (non lancée)"
    echo ""
    echo "  ${SBATCH_CMD[*]}"
    echo ""
    info "Export variables :"
    echo "$EXPORT_VARS" | tr ',' '\n' | while IFS= read -r v; do info "  $v"; done
    echo ""
    info "Pour lancer réellement, retirez --dry-run"
    exit 0
fi

JOB_ID=$("${SBATCH_CMD[@]}") || { err "Échec de la soumission Slurm (sbatch a retourné une erreur)."; exit 1; }

if [ -z "$JOB_ID" ]; then
    err "Échec de la soumission Slurm (job ID vide)."
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
    echo -e "  ls ${STUDY_DIR}/run_${JOB_ID}/                        # résultats rapatriés"
    echo -e "  ls -l ${STUDY_DIR}/latest                             # dernier run (symlink)"
    echo ""
fi
