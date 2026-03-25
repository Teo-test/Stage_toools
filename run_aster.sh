#!/bin/bash
#===============================================================================
#  run_aster — Calcul Code_Aster via sbatch (fichier unique)
#===============================================================================
#
#  Usage :  bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]
#
#  Le script fonctionne en DEUX PHASES dans un seul fichier :
#
#    PHASE 1 (noeud login) : bash lance ce script sans directives #SBATCH.
#      Il detecte qu'on est en phase de preparation (__ASTER_PHASE non defini),
#      prepare le scratch, copie les fichiers, genere le .export, puis se
#      RE-SOUMET LUI-MEME avec les bons parametres via sbatch --partition=...
#
#    PHASE 2 (noeud de calcul) : le script detecte __ASTER_PHASE=RUN
#      (transmis via --export), charge Code_Aster et lance le calcul.
#      A la fin (ou en cas de scancel/timeout), un trap rapatrie
#      les resultats vers le dossier d'etude d'origine et supprime
#      automatiquement le scratch.
#
#  Auteur   : Teo LEROY
#  Version  : 8.0
#===============================================================================

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION — modifier ici selon l'installation
# ══════════════════════════════════════════════════════════════════════════════

# Chemin vers Code_Aster (surchargeable par variable d'environnement)
ASTER_ROOT="${ASTER_ROOT:-/opt/code_aster}"

# Nom du module Lmod a charger (laisser vide si pas de module)
ASTER_MODULE="${ASTER_MODULE:-code_aster}"

# Repertoire scratch partage (accessible depuis login ET noeuds de calcul)
SCRATCH_BASE="${SCRATCH_BASE:-/scratch}"

# ── Valeurs par defaut Slurm ─────────────────────────────────────────────────
DEFAULT_PARTITION="court"
DEFAULT_NODES=1
DEFAULT_NTASKS=1
DEFAULT_CPUS_PER_TASK=1
DEFAULT_MEM="5G"
DEFAULT_TIME="05:00:00"

# ── Prereglages de ressources (-P court | -P moyen | -P long) ────────────────
PRESET_COURT_PARTITION="court"  ; PRESET_COURT_NTASKS=1 ; PRESET_COURT_MEM="2G"  ; PRESET_COURT_TIME="05:00:00"
PRESET_MOYEN_PARTITION="moyen"  ; PRESET_MOYEN_NTASKS=1 ; PRESET_MOYEN_MEM="8G"  ; PRESET_MOYEN_TIME="03-00:00:00"
PRESET_LONG_PARTITION="long"    ; PRESET_LONG_NTASKS=1  ; PRESET_LONG_MEM="32G"  ; PRESET_LONG_TIME="30-00:00:00"

# ══════════════════════════════════════════════════════════════════════════════
#  AFFICHAGE (utilise dans les deux phases)
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
  Les fichiers sont copies dans ${SCRATCH_BASE}/\$USER/ avant soumission.
  Par defaut, DOSSIER_ETUDE = repertoire courant.
  Le scratch est automatiquement supprime apres rapatriement des resultats.

FICHIERS
  -C, --comm FILE      Fichier .comm  (auto-detecte si absent)
  -M, --med  FILE      Fichier .med   (auto-detecte si absent)
  -A, --mail FILE      Fichier .mail  (format ASTER natif, auto-detecte si absent)

POURSUITE (couplage thermo-mecanique, enchainement de calculs)
  -B, --base CHEMIN    Dossier contenant la base du calcul precedent
                       (fichiers glob.* et pick.*).
                       Typiquement un dossier run_JOBID ou le lien "latest"
                       d'un calcul thermo precedent.
                       Exemple : -B ~/etude_thermo/latest

RESULTATS SUPPLEMENTAIRES
  -R, --results LIST   Unites de resultat additionnelles.
                        Format : "type:unite,type:unite,..."
                        Types : rmed, resu, mess, csv, table, dat, pos
                        Exemples :
                          --results "rmed:81"               un 2eme MED
                          --results "rmed:81,csv:38"        MED + CSV
                          --results "table:39,dat:40"       table + donnees

                        Dans le .comm, utiliser l'unite correspondante :
                          IMPR_RESU(UNITE=81, ...)
                          IMPR_TABLE(UNITE=38, ...)

RESSOURCES SLURM
  -p, --partition NOM  Partition        [defaut: ${DEFAULT_PARTITION}]
  -n, --nodes     N    Nombre de noeuds  [defaut: ${DEFAULT_NODES}]
  -t, --ntasks    N    Taches MPI       [defaut: ${DEFAULT_NTASKS}]
  -c, --cpus      N    CPUs par tache   [defaut: ${DEFAULT_CPUS_PER_TASK}]
  -m, --mem       MEM  Memoire/noeud     [defaut: ${DEFAULT_MEM}]
  -T, --time  H:M:S    Duree max        [defaut: ${DEFAULT_TIME}]

PREREGLAGES
  -P, --preset NOM     Prereglage de ressources (court, moyen, long)
                         court : ${PRESET_COURT_NTASKS} tache(s), ${PRESET_COURT_MEM}, ${PRESET_COURT_TIME}
                         moyen : ${PRESET_MOYEN_NTASKS} tache(s), ${PRESET_MOYEN_MEM}, ${PRESET_MOYEN_TIME}
                         long  : ${PRESET_LONG_NTASKS} tache(s), ${PRESET_LONG_MEM}, ${PRESET_LONG_TIME}
                       Les options passees apres -P surchargent le prereglage.

OPTIONS
  -q, --quiet          Sortie minimale (juste le job ID)
      --keep-scratch   NE PAS supprimer le scratch apres rapatriement
                       (par defaut le scratch est toujours supprime)
      --dry-run        Afficher la commande sbatch sans la lancer
      --debug          Activer le mode debug bash (set -x) en phase d'execution
  -h, --help           Afficher cette aide

EXEMPLES
  # Calcul simple
  bash run_aster.sh                              # Etude dans le dossier courant
  bash run_aster.sh ~/calculs/poutre/            # Specifier le dossier
  bash run_aster.sh -P court                     # Prereglage court

  # Couplage thermo-mecanique
  bash run_aster.sh -P moyen ~/etude_thermo/                        # 1. Thermo
  bash run_aster.sh -P moyen -B ~/etude_thermo/latest ~/etude_meca/ # 2. Meca

  # Enchainer 3 calculs
  bash run_aster.sh ~/etape1/                                       # Etape 1
  bash run_aster.sh -B ~/etape1/latest ~/etape2/                    # Etape 2
  bash run_aster.sh -B ~/etape2/latest ~/etape3/                    # Etape 3

  # Autres options
  bash run_aster.sh -P moyen -t 8                # Moyen mais 8 taches MPI
  bash run_aster.sh -n 2 -t 8 -m 8G             # Personnalise
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
# ##   PHASE 2 : EXECUTION SUR LE NOEUD DE CALCUL                            ##
# ##                                                                          ##
# ##   Detecte par __ASTER_PHASE=RUN (transmis via sbatch --export)           ##
# ##   Toutes les variables __ASTER_* contiennent les chemins resolus         ##
# ##                                                                          ##
# ##############################################################################
# ##############################################################################

if [ "${__ASTER_PHASE:-}" = "RUN" ]; then

    # ── Securite Bash en phase d'execution ────────────────────────────────────
    set -uo pipefail

    # ── Mode debug : active la trace bash si demande ──────────────────────────
    [ "${__ASTER_DEBUG:-0}" = "1" ] && set -x

    # ── Variable pour eviter un double rapatriement ───────────────────────────
    __RAPATRIE_DONE=0

    # ── Fonction utilitaire : rapatrier un fichier via rsync ─────────────────
    _rsync_result() {
        local src="$1" dest="$2"
        if rsync -av "$src" "$dest/" > >(while read -r l; do log "$l"; done) 2>&1; then
            return 0
        else
            log "!! Echec rsync : $src -> $dest"
            return 1
        fi
    }

    # ── Trap : rapatrier les resultats meme en cas de scancel / timeout ───────
    rapatrier() {
        if [ "$__RAPATRIE_DONE" -eq 1 ]; then
            return
        fi
        __RAPATRIE_DONE=1

        sep "RAPATRIEMENT DES RESULTATS"

        local dest="${__ASTER_STUDY_DIR}/run_${SLURM_JOB_ID}"
        local n=0

        if ! mkdir -p "$dest" 2>/dev/null; then
            log "!! Impossible de creer $dest -- resultats restent dans le scratch"
            log "  Scratch : $__ASTER_SCRATCH_DIR"
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

        # Rapatrier la base (glob.*, pick.*) pour pouvoir enchainer
        shopt -s nullglob
        for f in "${__ASTER_SCRATCH_DIR}"/glob.* "${__ASTER_SCRATCH_DIR}"/pick.*; do
            if [ -f "$f" ] && [ -s "$f" ]; then
                _rsync_result "$f" "$dest" && (( n++ )) || true
            fi
        done
        shopt -u nullglob

        # Rapatrier REPE_OUT si present
        if [ -d "${__ASTER_SCRATCH_DIR}/REPE_OUT" ]; then
            _rsync_result "${__ASTER_SCRATCH_DIR}/REPE_OUT" "$dest" && (( n++ )) || true
        fi

        if [ "$n" -eq 0 ]; then
            log "!! Aucun fichier resultat trouve dans ${__ASTER_SCRATCH_DIR}"
            log "  Contenu du scratch :"
            ls -la "${__ASTER_SCRATCH_DIR}/" 2>/dev/null | while IFS= read -r l; do log "   $l"; done
        else
            log "OK $n fichier(s) rapatrie(s) vers $dest"
        fi

        log ""
        log "Resultats dans : $dest"
        shopt -s nullglob
        for f in "$dest"/*; do
            log "  $(ls -lh "$f")"
        done
        shopt -u nullglob

        # ── Lien symbolique "latest" vers le dernier run ─────────────────────
        local latest_link="${__ASTER_STUDY_DIR}/latest"
        rm -f "$latest_link" 2>/dev/null
        if ln -s "run_${SLURM_JOB_ID}" "$latest_link" 2>/dev/null; then
            log "OK Lien symbolique : latest -> run_${SLURM_JOB_ID}"
        fi

        # ── Nettoyage automatique du scratch ─────────────────────────────────
        if [ "${__ASTER_KEEP_SCRATCH:-0}" != "1" ]; then
            log ""
            log "Nettoyage du scratch : $__ASTER_SCRATCH_DIR"
            if rm -rf "$__ASTER_SCRATCH_DIR"; then
                log "OK Scratch supprime"
            else
                log "!! Echec suppression scratch (droits insuffisants ?)"
            fi
        else
            log ""
            log "Scratch conserve (--keep-scratch) : $__ASTER_SCRATCH_DIR"
        fi
    }

    # Trapper SIGTERM (envoye par scancel / timeout SLURM) ET EXIT
    trap rapatrier EXIT
    trap 'rapatrier; exit 143' SIGTERM

    # ── Infos de demarrage ────────────────────────────────────────────────────
    sep "DEBUT CALCUL CODE_ASTER -- $(date)"
    log "Job ID         : $SLURM_JOB_ID"
    log "Etude          : $__ASTER_STUDY_NAME"
    log "Scratch        : $__ASTER_SCRATCH_DIR"
    log "Destination    : ${__ASTER_STUDY_DIR}/run_${SLURM_JOB_ID}"
    log "Noeuds alloues : $SLURM_NODELIST"
    log "Taches MPI     : $SLURM_NTASKS"
    log "CPUs par tache : ${SLURM_CPUS_PER_TASK:-1}"
    log "Memoire        : $__ASTER_MEM"
    log "Keep scratch   : ${__ASTER_KEEP_SCRATCH:-0}"
    log "Base poursuite : ${__ASTER_BASE_DIR:-aucune}"

    # ── Chargement de Code_Aster ──────────────────────────────────────────────
    sep "CHARGEMENT CODE_ASTER"

    ASTER_LOADED=0
    if command -v module &>/dev/null && [ -n "${__ASTER_MODULE:-}" ]; then
        if module load "${__ASTER_MODULE}" 2>/dev/null; then
            log "Module '${__ASTER_MODULE}' charge."
            ASTER_LOADED=1
        else
            warn "Module '${__ASTER_MODULE}' non disponible -- recherche manuelle."
        fi
    fi

    # Recherche de l'executable Code_Aster
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
        err "  -> Definir ASTER_ROOT=/chemin/code_aster avant de relancer."
        err "  -> Ou charger un module Lmod (ASTER_MODULE)."
        exit 1
    fi
    log "Executable : $ASTER_EXE"

    # Verification de coherence module/binaire
    if [ "$ASTER_LOADED" -eq 0 ] && [ -n "${__ASTER_MODULE:-}" ]; then
        warn "Le module n'a pas ete charge -- le binaire trouve pourrait ne pas"
        warn "avoir le bon environnement (LD_LIBRARY_PATH, etc.)."
    fi

    ASTER_VERSION=$("$ASTER_EXE" --version 2>&1 | head -1) || true
    if [ -z "$ASTER_VERSION" ]; then
        warn "Impossible d'obtenir la version de Code_Aster"
    else
        log "Version    : $ASTER_VERSION"
    fi

    # ── Lancement du calcul ───────────────────────────────────────────────────
    sep "CALCUL EN COURS"
    log "Demarrage : $(date)"

    # run_aster / as_run gerent MPI en interne via le .export (ncpus).
    # Les appeler via srun provoque un conflit de double pilotage MPI.
    ASTER_RC=0
    set +e
    log "Lancement : $ASTER_EXE $__ASTER_EXPORT_FILE"
    log "  (ncpus=${SLURM_NTASKS:-1} -- le parallelisme est gere par run_aster via le .export)"
    "$ASTER_EXE" "$__ASTER_EXPORT_FILE"
    ASTER_RC=$?
    set -e
    log "Execution terminee : $(date) -- code retour : $ASTER_RC"

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
            log "--- Premiere erreur fatale ---"
            grep -B2 -A5 "<F>" "$MESS_PATH" | head -20 || true
            log "--- fin ---"
        fi
        if [ "$NB_EXCEP" -gt 0 ]; then
            log "--- Premiere exception ---"
            grep -B2 -A5 "<S>" "$MESS_PATH" | head -20 || true
            log "--- fin ---"
        fi
        if [ "$ASTER_RC" -ne 0 ] && [ "$NB_FATAL" -eq 0 ] && [ "$NB_EXCEP" -eq 0 ]; then
            log "!! Code_Aster a echoue (rc=$ASTER_RC) sans erreur <F> ou <S> visible"
            log "  -> Verifier les dernieres lignes du .mess :"
            tail -20 "$MESS_PATH" | while IFS= read -r l; do log "  $l"; done
        fi
    else
        log "!! Fichier .mess non trouve -- echec au demarrage de Code_Aster ?"
        log "  Contenu du scratch :"
        ls -la "${__ASTER_SCRATCH_DIR}/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done
    fi

    # ── Rapatriement explicite scratch -> work ─────────────────────────────────
    rapatrier

    # ── Resume des ressources consommees (sacct) ──────────────────────────────
    sep "RESSOURCES UTILISEES"
    if command -v sacct &>/dev/null; then
        sacct -j "$SLURM_JOB_ID" \
              --format=JobID,JobName%20,Elapsed,CPUTime,MaxRSS,MaxVMSize,State,ExitCode \
              2>/dev/null | while IFS= read -r l; do log "$l"; done \
            || log "!! sacct : donnees non disponibles"
    else
        log "sacct non disponible sur ce systeme"
    fi

    # ── Resume final ──────────────────────────────────────────────────────────
    sep "RESUME FINAL"
    if [ "$ASTER_RC" -eq 0 ]; then
        log "Statut    : SUCCES"
    else
        log "Statut    : ECHEC  (code $ASTER_RC)"
    fi
    log "Etude     : $__ASTER_STUDY_NAME"
    log "Resultats : ${__ASTER_STUDY_DIR}/run_${SLURM_JOB_ID}"
    log "Scratch   : $__ASTER_SCRATCH_DIR"
    log "Fin       : $(date)"
    log "Alarmes <A> : $NB_ALARM  |  Fatales <F> : $NB_FATAL  |  Exceptions <S> : $NB_EXCEP"

    exit $ASTER_RC
fi

# ##############################################################################
# ##############################################################################
# ##                                                                          ##
# ##   PHASE 1 : PREPARATION SUR LE NOEUD LOGIN                              ##
# ##                                                                          ##
# ##   Detection fichiers, copie scratch, .export, re-soumission sbatch       ##
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
        --keep-scratch)     OPT_KEEP_SCRATCH=1;   shift ;;
        --dry-run)          OPT_DRY_RUN=1;        shift ;;
        --debug)            OPT_DEBUG=1;          shift ;;
        -h|--help)          usage ;;
        -*)                 err "Option inconnue : $1"; echo ""; usage ;;
        *)                  STUDY_DIR="$1";       shift ;;
    esac
done

# ── Application du prereglage (les options explicites ont priorite) ───────────
if [ -n "$PRESET" ]; then
    case "${PRESET,,}" in
        court|short)
            : "${PARTITION:=$PRESET_COURT_PARTITION}"
            : "${NTASKS:=$PRESET_COURT_NTASKS}"
            : "${MEM:=$PRESET_COURT_MEM}"
            : "${TIME_LIMIT:=$PRESET_COURT_TIME}"
            $QUIET || info "Prereglage : court  (${PRESET_COURT_NTASKS} tache(s), ${PRESET_COURT_MEM}, ${PRESET_COURT_TIME})"
            ;;
        moyen|medium)
            : "${PARTITION:=$PRESET_MOYEN_PARTITION}"
            : "${NTASKS:=$PRESET_MOYEN_NTASKS}"
            : "${MEM:=$PRESET_MOYEN_MEM}"
            : "${TIME_LIMIT:=$PRESET_MOYEN_TIME}"
            $QUIET || info "Prereglage : moyen  (${PRESET_MOYEN_NTASKS} tache(s), ${PRESET_MOYEN_MEM}, ${PRESET_MOYEN_TIME})"
            ;;
        long)
            : "${PARTITION:=$PRESET_LONG_PARTITION}"
            : "${NTASKS:=$PRESET_LONG_NTASKS}"
            : "${MEM:=$PRESET_LONG_MEM}"
            : "${TIME_LIMIT:=$PRESET_LONG_TIME}"
            $QUIET || info "Prereglage : long   (${PRESET_LONG_NTASKS} tache(s), ${PRESET_LONG_MEM}, ${PRESET_LONG_TIME})"
            ;;
        *) err "Prereglage inconnu : '$PRESET'  (valeurs : court, moyen, long)"; exit 1 ;;
    esac
fi

# Valeurs par defaut pour tout parametre non fixe par option ou prereglage
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

$QUIET || info "Dossier  : $STUDY_DIR"
$QUIET || info "Etude    : $STUDY_NAME"

# ── Fichier .comm ─────────────────────────────────────────────────────────────
if [ -z "$COMM_FILE" ]; then
    shopt -s nullglob
    COMM_LIST=("$STUDY_DIR"/*.comm)
    shopt -u nullglob
    case ${#COMM_LIST[@]} in
        0) err "Aucun fichier .comm dans $STUDY_DIR"; exit 1 ;;
        1) COMM_FILE="${COMM_LIST[0]}" ;;
        *) warn "Plusieurs .comm trouves -- selection du premier :"
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
        *) warn "Plusieurs .med trouves -- selection du premier :"
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
        *) warn "Plusieurs .mail trouves -- selection du premier :"
           for f in "${MAIL_LIST[@]}"; do warn "  $f"; done
           MAIL_FILE="${MAIL_LIST[0]}" ;;
    esac
fi
[ -n "$MAIL_FILE" ] && MAIL_FILE="$(realpath "$MAIL_FILE")"
[ -n "$MAIL_FILE" ] && { $QUIET || ok "Maillage ASTER: $MAIL_FILE"; }

[ -z "$MED_FILE" ] && [ -z "$MAIL_FILE" ] && \
    warn "Aucun maillage (.med ou .mail) -- calcul sans maillage externe ?"

# ── Base de poursuite (-B / --base) ───────────────────────────────────────────
if [ -n "$BASE_DIR" ]; then
    BASE_DIR="$(realpath "$BASE_DIR")"
    if [ ! -f "$BASE_DIR/glob.1" ]; then
        err "Pas de fichier glob.1 dans : $BASE_DIR"
        err "  Le dossier -B doit contenir les fichiers glob.* et pick.*"
        err "  (typiquement un dossier run_JOBID ou le lien 'latest')"
        err ""
        err "  Contenu du dossier :"
        ls -la "$BASE_DIR/" 2>/dev/null | while IFS= read -r l; do err "    $l"; done
        exit 1
    fi
    $QUIET || ok "Base POURSUITE : $BASE_DIR"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  PREPARATION DU SCRATCH
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Preparation du scratch"

# Timestamp + PID pour garantir l'unicite
TIMESTAMP="$(date +%Y%m%d_%H%M%S)_$$"
SCRATCH_DIR="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_${TIMESTAMP}"

$QUIET || info "Creation : $SCRATCH_DIR"
mkdir -p "$SCRATCH_DIR" || { err "Impossible de creer le dossier scratch : $SCRATCH_DIR"; exit 1; }

# ── Fonction de copie unifiee ─────────────────────────────────────────────────
_copy_to_scratch() {
    local src="$1"
    rsync -a "$src" "$SCRATCH_DIR/" \
        || { err "Echec copie vers scratch : $src"; exit 1; }
    $QUIET || ok "Copie : $(basename "$src")"
}

# ── Copie des fichiers principaux ─────────────────────────────────────────────
_copy_to_scratch "$COMM_FILE"
[ -n "$MED_FILE"  ] && _copy_to_scratch "$MED_FILE"
[ -n "$MAIL_FILE" ] && _copy_to_scratch "$MAIL_FILE"

# ── Copie de la base de poursuite ─────────────────────────────────────────────
if [ -n "$BASE_DIR" ]; then
    $QUIET || info "Copie de la base de poursuite..."
    shopt -s nullglob
    for f in "$BASE_DIR"/glob.* "$BASE_DIR"/pick.*; do
        _copy_to_scratch "$f"
    done
    shopt -u nullglob
fi

# ── Fichiers annexes optionnels ───────────────────────────────────────────────
shopt -s nullglob
for ext in py dat para include mfront; do
    for f in "$STUDY_DIR/"*."$ext"; do
        _copy_to_scratch "$f"
    done
done
shopt -u nullglob

# ══════════════════════════════════════════════════════════════════════════════
#  PARSING ROBUSTE DE LA MEMOIRE
#  Formats acceptes : 4G, 4g, 1.5G, 4096M, 4096m, 4096 (entier = MB)
# ══════════════════════════════════════════════════════════════════════════════
MEM_MB=$(echo "$MEM" | awk '
    tolower($0) ~ /^[0-9]+(\.[0-9]+)?g$/ { gsub(/[gGiI]/, ""); print int($0 * 1024); next }
    tolower($0) ~ /^[0-9]+(\.[0-9]+)?m$/ { gsub(/[mMiI]/, ""); print int($0);        next }
    /^[0-9]+$/                            { print int($0);                             next }
    { print -1 }
')

if [ "$MEM_MB" -le 0 ] 2>/dev/null; then
    err "Format memoire non reconnu : '$MEM'  (attendu : 4G, 1.5G, 512M, ou entier en MB)"
    exit 1
fi

# Reserver 512 MB pour le systeme ; plancher a 512 MB pour Code_Aster
ASTER_MEM=$(( MEM_MB - 512 ))
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512

# ── Parsing robuste du temps (accepte JJ-HH:MM:SS, HH:MM:SS, MM:SS, SS) ──────
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

    # ── Fichiers d'entree (D = donnees) ──
    echo "F comm ${SCRATCH_DIR}/${COMM_BASENAME}           D  1"
    [ -n "$MED_BASENAME"  ] && echo "F mmed ${SCRATCH_DIR}/${MED_BASENAME}            D 20"
    [ -n "$MAIL_BASENAME" ] && echo "F mail ${SCRATCH_DIR}/${MAIL_BASENAME}           D 20"

    # ── Base de poursuite (POURSUITE) ──
    if [ -n "$BASE_DIR" ]; then
        echo "F base ${SCRATCH_DIR}/glob.1 D 0"
    fi

    # ── Fichiers de sortie par defaut ──
    echo "F mess ${SCRATCH_DIR}/${STUDY_NAME}.mess         R  6"
    echo "F resu ${SCRATCH_DIR}/${STUDY_NAME}.resu         R  8"
    echo "F rmed ${SCRATCH_DIR}/${STUDY_NAME}_resu.med     R 80"

    # ── Resultats supplementaires (--results / -R) ──
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

    # ── Repertoire de sortie REPE_OUT ──
    echo "R ${SCRATCH_DIR}/REPE_OUT R 0"

} > "$EXPORT_FILE" || { err "Echec ecriture du fichier .export : $EXPORT_FILE"; exit 1; }

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
    info "Memoire    : $MEM  (${ASTER_MEM} MB alloues a Code_Aster)"
    info "Duree max  : $TIME_LIMIT"
    info "Scratch    : $SCRATCH_DIR"
    [ "$OPT_KEEP_SCRATCH" = "1" ] && info "Scratch    : conserve apres rapatriement (--keep-scratch)"
    [ "$OPT_KEEP_SCRATCH" = "0" ] && info "Scratch    : supprime automatiquement apres rapatriement"
    [ "$OPT_DEBUG" = "1" ]        && info "Debug      : set -x actif en phase d'execution"
    [ -n "$RESULT_UNITS" ]        && info "Resultats+ : $RESULT_UNITS"
    [ -n "$BASE_DIR" ]            && info "Base       : $BASE_DIR (POURSUITE)"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  RE-SOUMISSION DE CE MEME SCRIPT AVEC LES DIRECTIVES SLURM EN LIGNE
# ══════════════════════════════════════════════════════════════════════════════
$QUIET || section "Soumission Slurm"

SELF_SCRIPT="$(realpath "$0")"

SLURM_LOG_OUT="${STUDY_DIR}/aster_%j.out"
SLURM_LOG_ERR="${STUDY_DIR}/aster_%j.err"

# Variables exportees : variables __ASTER_* + environnement systeme essentiel.
# Sur Cray/HPC, les noeuds de calcul ont un PATH minimal.
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
EXPORT_VARS+=",__ASTER_BASE_DIR=${BASE_DIR}"

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
    section "DRY RUN -- commande sbatch (non lancee)"
    echo ""
    echo "  ${SBATCH_CMD[*]}"
    echo ""
    info "Export variables :"
    echo "$EXPORT_VARS" | tr ',' '\n' | while IFS= read -r v; do info "  $v"; done
    echo ""
    info "Pour lancer reellement, retirez --dry-run"
    exit 0
fi

JOB_ID=$("${SBATCH_CMD[@]}") || { err "Echec de la soumission Slurm (sbatch a retourne une erreur)."; exit 1; }

if [ -z "$JOB_ID" ]; then
    err "Echec de la soumission Slurm (job ID vide)."
    exit 1
fi

if $QUIET; then
    echo "$JOB_ID"
else
    ACTUAL_LOG_OUT="${STUDY_DIR}/aster_${JOB_ID}.out"

    ok "Job soumis   : ID = ${BOLD}${JOB_ID}${NC}"
    echo ""
    echo -e "  ${BOLD}Commandes utiles :${NC}"
    echo -e "  squeue -j ${JOB_ID}                                   # etat du job"
    echo -e "  tail -f ${ACTUAL_LOG_OUT}          # logs temps reel"
    echo -e "  scancel ${JOB_ID}                                      # annuler"
    echo -e "  ls ${SCRATCH_DIR}/                                    # scratch"
    echo -e "  ls ${STUDY_DIR}/run_${JOB_ID}/                        # resultats rapatries"
    echo -e "  ls -l ${STUDY_DIR}/latest                             # dernier run (symlink)"
    [ -n "$BASE_DIR" ] && echo -e "  # POURSUITE depuis : ${BASE_DIR}"
    echo ""
fi
