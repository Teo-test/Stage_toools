#!/bin/bash
#===============================================================================
#  run_aster.sh — Soumission de calculs Code_Aster via Slurm
#===============================================================================
#
#  Usage :  bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]
#
#  ARCHITECTURE EN DEUX PHASES DANS UN SEUL FICHIER
#  ─────────────────────────────────────────────────
#  Ce script joue deux roles selon le contexte dans lequel il est appele :
#
#    Phase 1 — noeud login (appel direct par l'utilisateur)
#      · Detecte les fichiers d'entree (.comm, .med, .mail, base, rmed)
#      · Cree un dossier scratch unique sur le systeme de fichiers partage
#      · Copie les fichiers d'entree dans le scratch
#      · Genere le fichier .export qui configure le calcul Code_Aster
#      · Soumet CE MEME script via sbatch avec __RUN_PHASE=EXEC
#
#    Phase 2 — noeud de calcul (appel par sbatch)
#      · Detectee par la variable d'environnement __RUN_PHASE=EXEC
#      · Charge Code_Aster via module ou chemin direct
#      · Lance le calcul en passant le .export a run_aster/as_run
#      · Analyse le fichier .mess (alarmes, erreurs)
#      · Rapatrie les resultats vers le dossier d'etude
#      · Nettoie le scratch
#
#  NOTE MPI : run_aster/as_run lance lui-meme les processus MPI en interne.
#  Ne pas l'encapsuler dans srun, cela provoquerait une double initialisation
#  MPI et des conflits de gestionnaires de processus.
#
#  Auteur  : Teo LEROY
#  Version : 10.0
#===============================================================================

# ══════════════════════════════════════════
#  CONFIGURATION GLOBALE
#  Ces trois variables peuvent etre surchargees par variable d'environnement
#  avant d'appeler le script (ex: export ASTER_ROOT=/logiciels/aster/17.1).
#  La syntaxe ${VAR:-valeur} signifie : utiliser $VAR si definie, sinon valeur.
# ══════════════════════════════════════════

ASTER_ROOT="${ASTER_ROOT:-/opt/code_aster}"   # Racine de l'installation Code_Aster
ASTER_MODULE="${ASTER_MODULE:-code_aster}"    # Nom du module Lmod a charger
SCRATCH_BASE="${SCRATCH_BASE:-/scratch}"      # Racine du filesystem scratch (partage login/calcul)

# Ressources Slurm par defaut (utilisees si aucune option ni preset n'est donne)
DEFAULT_PARTITION="court"
DEFAULT_NODES=1
DEFAULT_NTASKS=4     # Nombre de taches MPI par defaut
DEFAULT_CPUS=1       # CPUs par tache MPI (threading OpenMP, generalement 1)
DEFAULT_MEM="5G"
DEFAULT_TIME="05:00:00"

# Presets : raccourcis pour les configurations typiques du cluster.
# Chaque preset definit partition, ntasks, memoire et duree maximale.
# Les valeurs peuvent etre surchargees apres -P (ex: -P moyen -t 8).
PRESET_COURT_PARTITION="court"  ; PRESET_COURT_NTASKS=4  ; PRESET_COURT_MEM="2G"  ; PRESET_COURT_TIME="05:00:00"
PRESET_MOYEN_PARTITION="moyen"  ; PRESET_MOYEN_NTASKS=4  ; PRESET_MOYEN_MEM="8G"  ; PRESET_MOYEN_TIME="03-00:00:00"
PRESET_LONG_PARTITION="long"    ; PRESET_LONG_NTASKS=4   ; PRESET_LONG_MEM="32G"  ; PRESET_LONG_TIME="30-00:00:00"

# ══════════════════════════════════════════
#  FONCTIONS D'AFFICHAGE
#  Codes couleurs ANSI pour le terminal. NC = No Color (reinitialise).
#  Chaque fonction correspond a un niveau de message :
#    info  : information normale (bleu)
#    ok    : validation, succes (vert)
#    warn  : avertissement non bloquant (jaune), stderr
#    err   : erreur bloquante (rouge), stderr
#    log   : message horodate pour les logs du noeud de calcul
#    section : titre de section avec separateur (Phase 1)
#    header  : encadre large (Phase 2, dans les logs sbatch)
# ══════════════════════════════════════════

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()      { echo -e "${GREEN}[ OK ]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()     { echo -e "${RED}[ ERR]${NC}  $*" >&2; }
log()     { echo "[$(date +%H:%M:%S)] $*"; }
section() { echo -e "\n${BOLD}${CYAN}> $*${NC}"; echo -e "${CYAN}$(printf -- '-%.0s' {1..60})${NC}"; }
header()  {
    echo ""
    echo "========================================================"
    echo "  $*"
    echo "========================================================"
}

# ══════════════════════════════════════════
#  AIDE EN LIGNE
#  Affichee par -h / --help, puis le script quitte (exit 0).
#  Le heredoc <<'EOF' (guillemets autour de EOF) desactive l'expansion
#  des variables a l'interieur, ce qui permet d'ecrire $ sans echappement.
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
  -R, --results LIST    Format "type:unite,..." (ex: "rmed:81, csv:38")

RESSOURCES SLURM
  -P, --preset  NOM     court, moyen ou long
  -p, --partition NOM   Partition Slurm
  -n, --nodes N         Nombre de noeuds
  -t, --ntasks N        Taches MPI
  -c, --cpus N          CPUs par tache
  -m, --mem MEM         Memoire (ex: 8G)
  -T, --time DUREE      Duree max (J-HH:MM:SS, HH:MM:SS, MM:SS)

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
#   Ce bloc est execute UNIQUEMENT quand sbatch relance ce script avec
#   la variable __RUN_PHASE=EXEC dans son environnement (transmise via
#   sbatch --export). Sur le noeud login, cette variable est absente,
#   donc le bloc est saute et on tombe directement en Phase 1.
#
# ##########################################################################

if [ "${__RUN_PHASE:-}" = "EXEC" ]; then

    # set -uo pipefail : mode strict
    #   -u : erreur si variable non definie (evite les bugs silencieux)
    #   -o pipefail : echec du pipe si l'une des commandes echoue
    # set -x est active plus bas uniquement si --debug a ete passe.
    set -uo pipefail
    [ "${__DEBUG:-0}" = "1" ] && set -x

    # Verrou anti-double-rapatriement : collect_results peut etre appelee
    # a la fois par le trap EXIT et explicitement en fin de script.
    # Ce drapeau garantit qu'elle ne s'execute qu'une seule fois.
    ALREADY_COLLECTED=0

    # ─────────────────────────────────────────────────────────────────
    # _cp : helper de copie portable
    #   Utilise rsync -a si disponible (meilleur pour les gros fichiers :
    #   delta I/O, reprise sur erreur reseau), sinon cp -a comme fallback.
    #   rsync -a = archive mode : preserve permissions, timestamps, liens
    #   symboliques, et copie recursivement les dossiers.
    # ─────────────────────────────────────────────────────────────────
    _cp() {
        if command -v rsync &>/dev/null; then
            rsync -a "$@"
        else
            cp -a "$@"
        fi
    }

    # ─────────────────────────────────────────────────────────────────
    # collect_results : rapatrie les resultats du scratch vers le dossier
    # d'etude et nettoie le scratch.
    #
    # Cette fonction est enregistree comme trap EXIT et SIGTERM, ce qui
    # garantit qu'elle s'execute meme en cas de :
    #   - fin normale du script
    #   - scancel (Slurm envoie SIGTERM avant de tuer le job)
    #   - timeout (Slurm envoie SIGTERM a l'expiration du --time)
    #
    # Structure du dossier de destination :
    #   $STUDY_DIR/run_$JOBID/          <- resultats du calcul
    #   $STUDY_DIR/latest -> run_$JOBID <- lien symbolique vers le dernier run
    # ─────────────────────────────────────────────────────────────────
    collect_results() {
        [ "$ALREADY_COLLECTED" -eq 1 ] && return   # ne s'execute qu'une fois
        ALREADY_COLLECTED=1

        header "RAPATRIEMENT"

        # Dossier de destination unique par job ID, pour eviter tout ecrasement
        # si plusieurs jobs tournent depuis le meme dossier d'etude.
        local dest="${__STUDY_DIR}/run_${SLURM_JOB_ID}"
        mkdir -p "$dest" || { log "!! Impossible de creer $dest"; return; }

        local count=0   # compteur de fichiers rapatries (informatif)

        # --- Resultats classiques ---
        # On parcourt toutes les extensions de fichiers que Code_Aster peut produire.
        # [ -f "$f" ] : verifie que c'est un fichier regulier (pas un dossier)
        # [ -s "$f" ] : verifie que le fichier n'est pas vide (taille > 0)
        # Les globs sans correspondance retournent la chaine litterale sous bash ;
        # le test -f filtre ces cas (le fichier n'existe pas).
        for ext in mess resu med csv table dat pos rmed txt vtu vtk py; do
            for f in "${__SCRATCH}"/*."${ext}"; do
                if [ -f "$f" ] && [ -s "$f" ]; then
                    _cp "$f" "$dest/"
                    log "  -> $(basename "$f")"
                    (( count++ ))
                fi
            done
        done 2>/dev/null

        # --- Repertoire REPE_OUT ---
        # Code_Aster peut ecrire dans REPE_OUT via des commandes comme IMPR_RESU
        # avec un repertoire de sortie libre. On le rapatrie entierement si present.
        if [ -d "${__SCRATCH}/REPE_OUT" ]; then
            _cp "${__SCRATCH}/REPE_OUT" "$dest/"
            log "  -> REPE_OUT/"
            (( count++ ))
        fi 2>/dev/null

        # --- Lien symbolique "latest" ---
        # Pointe toujours vers le dossier du dernier job termine,
        # pour acceder facilement aux resultats sans connaitre le JOBID :
        #   ls $STUDY_DIR/latest/
        rm -f "${__STUDY_DIR}/latest" 2>/dev/null
        ln -s "run_${SLURM_JOB_ID}" "${__STUDY_DIR}/latest" 2>/dev/null

        log "$count fichier(s) rapatrie(s) -> $dest"

        # --- Nettoyage du scratch ---
        # Sauf si --keep-scratch a ete demande (utile pour deboguer un calcul
        # qui a plante : on peut inspecter l'etat du scratch apres le job).
        if [ "${__KEEP_SCRATCH:-0}" != "1" ]; then
            rm -rf "$__SCRATCH" 2>/dev/null && log "Scratch supprime"
        else
            log "Scratch conserve : $__SCRATCH"
        fi
    }

    # Enregistrement des traps :
    #   EXIT    : s'execute a toute sortie du script (normale ou erreur)
    #   SIGTERM : envoye par Slurm lors d'un scancel ou d'un timeout
    #             -> on rapatrie avant que Slurm ne force la terminaison
    trap collect_results EXIT
    trap 'collect_results; exit 143' SIGTERM   # 143 = 128 + 15 (SIGTERM)

    # ─────────────────────────────────────────────────────────────────
    # En-tete de log : informations contextuelles du job Slurm.
    # Les variables SLURM_* sont injectees automatiquement par Slurm.
    # Les variables __* ont ete transmises depuis la Phase 1 via --export.
    # ─────────────────────────────────────────────────────────────────
    header "CODE_ASTER — $(date)"
    log "Job       : $SLURM_JOB_ID"
    log "Noeud     : $SLURM_NODELIST"
    log "Scratch   : $__SCRATCH"

    # ─────────────────────────────────────────────────────────────────
    # Chargement du module Code_Aster
    #
    # Probleme : en environnement batch non-interactif, /etc/profile et
    # les scripts profile.d ne sont pas sources automatiquement, donc la
    # commande "module" (fournie par Lmod ou Environment Modules) peut
    # etre absente du PATH.
    #
    # Solution : on tente de sourcer manuellement le script d'init du
    # gestionnaire de modules avant d'appeler "module load".
    # On essaie modules.sh (Environment Modules) puis lmod.sh (Lmod).
    # Le "break" sort de la boucle des qu'un sourcing reussit.
    # ─────────────────────────────────────────────────────────────────
    if [ -n "${__MODULE:-}" ]; then
        if ! command -v module &>/dev/null; then
            for _mfile in /etc/profile.d/modules.sh /etc/profile.d/lmod.sh; do
                [ -f "$_mfile" ] && . "$_mfile" && break
            done
        fi
        if command -v module &>/dev/null; then
            # "2>&1" redirige les messages du module vers stdout pour qu'ils
            # apparaissent dans le log du job (aster_JOBID.out).
            module load "$__MODULE" 2>&1 \
                && log "Module '$__MODULE' charge" \
                || warn "Module '$__MODULE' echec"
        else
            warn "Commande module introuvable apres sourcing — module non charge"
        fi
    fi

    # ─────────────────────────────────────────────────────────────────
    # Recherche de l'executable Code_Aster
    #
    # On cherche dans l'ordre :
    #   1. $ASTER_ROOT/bin/run_aster  (installation standard recente)
    #   2. $ASTER_ROOT/bin/as_run     (ancienne denomination)
    #   3. run_aster dans le PATH     (module charge ou installation custom)
    #   4. as_run dans le PATH
    #
    # "command -v" retourne le chemin de l'executable ou rien si absent.
    # "|| true" empeche un exit en cas d'echec sous set -e.
    # On boucle et on prend le premier executable valide trouve.
    # ─────────────────────────────────────────────────────────────────
    EXE=""
    for c in "${__ASTER_ROOT}/bin/run_aster" \
             "${__ASTER_ROOT}/bin/as_run" \
             "$(command -v run_aster 2>/dev/null || true)" \
             "$(command -v as_run 2>/dev/null || true)"; do
        [ -n "$c" ] && [ -x "$c" ] && { EXE="$c"; break; }
    done
    [ -z "$EXE" ] && { err "Code_Aster introuvable"; exit 1; }
    log "Executable : $EXE"
    # Affiche la version dans le log (head -1 pour ne garder que la premiere ligne)
    "$EXE" --version 2>&1 | head -1 | while read -r l; do log "Version : $l"; done

    # ─────────────────────────────────────────────────────────────────
    # Verification pre-calcul : affiche le contenu du scratch et du .export
    # dans le log pour faciliter le diagnostic en cas de probleme.
    # ─────────────────────────────────────────────────────────────────
    header "VERIFICATION"
    log "Contenu scratch :"
    ls -la "$__SCRATCH/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done
    log ""
    log "Contenu .export :"
    # "while IFS= read -r l" preserve les espaces en debut de ligne et les
    # backslashes, contrairement a un simple "while read".
    cat "$__EXPORT" 2>/dev/null | while IFS= read -r l; do log "  $l"; done

    # ─────────────────────────────────────────────────────────────────
    # Lancement du calcul
    #
    # set +e desactive temporairement l'arret sur erreur (set -e) pour
    # capturer le code retour de run_aster sans que le script s'arrete.
    # run_aster peut retourner un code non nul sans que ce soit fatal
    # pour notre logique de rapatriement.
    #
    # IMPORTANT : on appelle run_aster directement, sans srun.
    # run_aster/as_run gere lui-meme le lancement MPI (il appelle mpirun
    # en interne selon la configuration du .export). Encapsuler dans srun
    # provoquerait une double initialisation MPI (srun + mpirun interne)
    # et des conflits entre les gestionnaires de processus Slurm et MPI.
    # ─────────────────────────────────────────────────────────────────
    header "CALCUL"
    log "Lancement : $(date)"
    RC=0
    set +e
    "$EXE" "$__EXPORT"
    RC=$?
    set -e
    log "Termine : $(date) — code retour $RC"

    # ─────────────────────────────────────────────────────────────────
    # Diagnostic du fichier .mess
    #
    # Le fichier .mess est le journal de Code_Aster. Il contient :
    #   <A> : alarmes (non bloquantes, a verifier)
    #   <F> : erreurs fatales (arret du calcul)
    #   <S> : exceptions (erreurs graves mais non fatales)
    #
    # grep -c compte le nombre d'occurrences. "|| true" empeche un exit
    # si grep ne trouve rien (retourne 1).
    # On affiche les 20 premieres lignes autour des erreurs pour le log.
    # ─────────────────────────────────────────────────────────────────
    header "DIAGNOSTIC"
    MESS="${__SCRATCH}/${__STUDY_NAME}.mess"
    if [ -f "$MESS" ]; then
        NA=$(grep -c "<A>" "$MESS" 2>/dev/null || true)
        NF=$(grep -c "<F>" "$MESS" 2>/dev/null || true)
        NS=$(grep -c "<S>" "$MESS" 2>/dev/null || true)
        log "Alarmes <A>:$NA  Fatales <F>:$NF  Exceptions <S>:$NS"
        # Affiche le contexte autour des erreurs fatales (-B2 = 2 lignes avant, -A5 = 5 apres)
        [ "$NF" -gt 0 ] && { grep -B2 -A5 "<F>" "$MESS" | head -20; }
        # N'affiche les exceptions que s'il n'y a pas d'erreur fatale (deja affichee)
        [ "$NS" -gt 0 ] && [ "$NF" -eq 0 ] && { grep -B2 -A5 "<S>" "$MESS" | head -20; }
    else
        log "!! Pas de .mess — le calcul n'a peut-etre pas demarre"
        ls -la "$__SCRATCH/" 2>/dev/null
    fi

    # Etat final du scratch (aide au debug)
    log ""
    log "Contenu scratch apres calcul :"
    ls -la "$__SCRATCH/" 2>/dev/null | while IFS= read -r l; do log "  $l"; done

    # Rapatriement explicite (le trap EXIT le ferait aussi, mais on l'appelle
    # ici pour avoir les logs dans l'ordre avant le header FIN).
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
#   Ce code n'est atteint que si __RUN_PHASE != "EXEC", c'est-a-dire
#   lors de l'appel direct par l'utilisateur depuis le noeud login.
#
# ##########################################################################

# set -euo pipefail : mode strict pour la Phase 1
#   -e : arret immediat si une commande echoue
#   -u : erreur si variable non definie
#   -o pipefail : echec du pipe si l'une des commandes echoue
set -euo pipefail

# ─────────────────────────────────────────────────────────────────
# Initialisation des variables d'arguments
# Toutes les variables sont initialisees a vide ou a leur valeur par
# defaut pour eviter les erreurs "variable non definie" avec set -u.
# ─────────────────────────────────────────────────────────────────
STUDY_DIR="."
COMM="" ; MED="" ; MAIL=""
PRESET="" ; PARTITION="" ; NODES="" ; NTASKS="" ; CPUS="" ; MEM="" ; TIME_LIMIT=""
QUIET=false ; RESULTS="" ; KEEP_SCRATCH=0 ; DRY_RUN=0 ; DEBUG=0

# ─────────────────────────────────────────────────────────────────
# Parsing des arguments en ligne de commande
#
# On boucle tant qu'il reste des arguments ($# > 0).
# "shift" consomme le premier argument ($1 devient l'ancien $2, etc.)
# "shift 2" consomme l'option et sa valeur.
# Le cas *) sans tiret est le DOSSIER_ETUDE positionnel.
# ─────────────────────────────────────────────────────────────────
while [ $# -gt 0 ]; do
    case "$1" in
        -C|--comm)      COMM="$2";        shift 2 ;;   # Fichier .comm explicite
        -M|--med)       MED="$2";         shift 2 ;;   # Maillage MED explicite
        -A|--mail)      MAIL="$2";        shift 2 ;;   # Maillage ASTER natif explicite
        -R|--results)   RESULTS="$2";     shift 2 ;;   # Sorties supplementaires "type:unite,..."
        -P|--preset)    PRESET="$2";      shift 2 ;;   # Preset (court/moyen/long)
        -p|--partition) PARTITION="$2";   shift 2 ;;   # Partition Slurm
        -n|--nodes)     NODES="$2";       shift 2 ;;   # Nombre de noeuds
        -t|--ntasks)    NTASKS="$2";      shift 2 ;;   # Nombre de taches MPI
        -c|--cpus)      CPUS="$2";        shift 2 ;;   # CPUs par tache
        -m|--mem)       MEM="$2";         shift 2 ;;   # Memoire (ex: 8G)
        -T|--time)      TIME_LIMIT="$2";  shift 2 ;;   # Duree max
        -q|--quiet)     QUIET=true;       shift ;;     # Mode silencieux (affiche seulement le JOB ID)
        --keep-scratch) KEEP_SCRATCH=1;   shift ;;     # Ne pas supprimer le scratch
        --dry-run)      DRY_RUN=1;        shift ;;     # Afficher la commande sans la lancer
        --debug)        DEBUG=1;          shift ;;     # Activer set -x en Phase 2
        -h|--help)      usage ;;                       # Afficher l'aide et quitter
        -*)             err "Option inconnue : $1"; usage ;;
        *)              STUDY_DIR="$1";   shift ;;     # Dossier d'etude (argument positionnel)
    esac
done

# ─────────────────────────────────────────────────────────────────
# Application des presets
#
# ${PRESET,,} convertit en minuscules (bash 4+).
# La syntaxe ": ${VAR:=valeur}" assigne valeur a VAR seulement si VAR
# est vide ou non definie — cela permet aux options explicites passees
# apres -P de prendre precedence sur les valeurs du preset.
# Ex: -P moyen -t 8  -> utilise les valeurs de moyen SAUF ntasks=8
# ─────────────────────────────────────────────────────────────────
if [ -n "$PRESET" ]; then
    case "${PRESET,,}" in
        court|short)  : "${PARTITION:=$PRESET_COURT_PARTITION}"; : "${NTASKS:=$PRESET_COURT_NTASKS}"; : "${MEM:=$PRESET_COURT_MEM}"; : "${TIME_LIMIT:=$PRESET_COURT_TIME}" ;;
        moyen|medium) : "${PARTITION:=$PRESET_MOYEN_PARTITION}"; : "${NTASKS:=$PRESET_MOYEN_NTASKS}"; : "${MEM:=$PRESET_MOYEN_MEM}"; : "${TIME_LIMIT:=$PRESET_MOYEN_TIME}" ;;
        long)         : "${PARTITION:=$PRESET_LONG_PARTITION}";  : "${NTASKS:=$PRESET_LONG_NTASKS}";  : "${MEM:=$PRESET_LONG_MEM}";  : "${TIME_LIMIT:=$PRESET_LONG_TIME}" ;;
        *) err "Preset inconnu : $PRESET"; exit 1 ;;
    esac
    $QUIET || info "Preset : $PRESET"
fi
# Applique les valeurs par defaut pour tout ce qui est encore vide
: "${PARTITION:=$DEFAULT_PARTITION}"; : "${NODES:=$DEFAULT_NODES}"
: "${NTASKS:=$DEFAULT_NTASKS}"; : "${CPUS:=$DEFAULT_CPUS}"
: "${MEM:=$DEFAULT_MEM}"; : "${TIME_LIMIT:=$DEFAULT_TIME}"

# ─────────────────────────────────────────────────────────────────
# Detection automatique des fichiers d'entree
#
# Pour chaque type de fichier, si l'utilisateur ne l'a pas specifie
# explicitement avec -C/-M/-A, on cherche dans STUDY_DIR.
# "shopt -s nullglob" : un glob sans correspondance retourne une liste
#   vide plutot que la chaine litterale (ex: *.comm -> rien, pas "*.comm")
# "shopt -u nullglob" : remet le comportement par defaut apres.
# "realpath" resout les chemins relatifs en chemins absolus, ce qui
# est necessaire car les chemins seront utilises depuis le scratch.
# ─────────────────────────────────────────────────────────────────
$QUIET || section "Detection des fichiers"

STUDY_DIR="$(realpath "$STUDY_DIR")"
STUDY_NAME="$(basename "$STUDY_DIR")"   # Nom utilise pour nommer le job et les fichiers
[ -d "$STUDY_DIR" ] || { err "Dossier introuvable : $STUDY_DIR"; exit 1; }

# Fichier .comm (obligatoire) : contient les commandes Code_Aster du calcul
if [ -z "$COMM" ]; then
    shopt -s nullglob; arr=("$STUDY_DIR"/*.comm); shopt -u nullglob
    [ ${#arr[@]} -eq 0 ] && { err "Aucun .comm dans $STUDY_DIR"; exit 1; }
    [ ${#arr[@]} -gt 1 ] && warn "Plusieurs .comm, utilisation du premier"
    COMM="${arr[0]}"
fi
COMM="$(realpath "$COMM")"
$QUIET || ok "Comm : $COMM"

# Fichier .med (optionnel) : maillage au format MED (genere par Salome)
if [ -z "$MED" ]; then
    shopt -s nullglob; arr=("$STUDY_DIR"/*.med); shopt -u nullglob
    [ ${#arr[@]} -ge 1 ] && MED="${arr[0]}"
    [ ${#arr[@]} -gt 1 ] && warn "Plusieurs .med, utilisation du premier"
fi
[ -n "$MED" ] && { MED="$(realpath "$MED")"; $QUIET || ok "Med  : $MED"; }

# Fichier .mail (optionnel) : maillage au format ASTER natif (texte)
if [ -z "$MAIL" ]; then
    shopt -s nullglob; arr=("$STUDY_DIR"/*.mail); shopt -u nullglob
    [ ${#arr[@]} -ge 1 ] && MAIL="${arr[0]}"
fi
[ -n "$MAIL" ] && { MAIL="$(realpath "$MAIL")"; $QUIET || ok "Mail : $MAIL"; }

# ─────────────────────────────────────────────────────────────────
# Creation du dossier scratch
#
# Le scratch est un filesystem local ou NFS haute performance partage
# entre le noeud login et les noeuds de calcul.
# Le nom inclut : utilisateur / nom_etude_timestamp_PID
#   - timestamp (date +%s) : secondes depuis epoch, assure l'unicite
#   - $$ : PID du process courant, protection contre les lancements simultanees
# ─────────────────────────────────────────────────────────────────
$QUIET || section "Preparation du scratch"
SCRATCH="${SCRATCH_BASE}/${USER}/${STUDY_NAME}_$(date +%s)_$$"
mkdir -p "$SCRATCH"
$QUIET || ok "Scratch : $SCRATCH"

# Copie des fichiers d'entree principaux dans le scratch.
# Code_Aster travaille exclusivement dans le scratch ; les chemins du .export
# pointeront vers ces copies (sauf pour la base et le rmed externe).
cp "$COMM" "$SCRATCH/"
[ -n "$MED" ]  && cp "$MED" "$SCRATCH/"
[ -n "$MAIL" ] && cp "$MAIL" "$SCRATCH/"

# Fichiers annexes : copies automatiquement si presents dans le dossier d'etude.
# Utiles pour les calculs qui font appel a des modules Python (.py),
# des parametres externes (.dat, .para), des inclusions (.include),
# ou des lois de comportement MFront (.mfront).
# "shopt -s nullglob" evite les erreurs si aucun fichier du type n'existe.
shopt -s nullglob
for f in "$STUDY_DIR"/*.py "$STUDY_DIR"/*.dat "$STUDY_DIR"/*.para \
         "$STUDY_DIR"/*.include "$STUDY_DIR"/*.mfront; do
    cp "$f" "$SCRATCH/"
done
shopt -u nullglob

# ─────────────────────────────────────────────────────────────────
# Conversion de la memoire en MB
#
# Code_Aster attend memory_limit en MB dans le .export.
# On convertit la valeur fournie par l'utilisateur (ex: "8G") en MB.
# On reserve 512 MB pour le systeme (OS, bibliotheques).
# Le minimum garanti a Aster est 512 MB.
#
# Logique awk :
#   - tolower : normalise en minuscules pour le matching
#   - gsub : supprime les lettres de l'unite (G, g, M, m, i)
#   - int  : convertit en entier
# ─────────────────────────────────────────────────────────────────
MEM_MB=$(echo "$MEM" | awk '
    tolower($0) ~ /g$/ { gsub(/[gGiI]/,""); print int($0*1024); next }
    tolower($0) ~ /m$/ { gsub(/[mMiI]/,""); print int($0);      next }
    /^[0-9]+$/          { print int($0); next }
    { print -1 }')
[ "$MEM_MB" -le 0 ] 2>/dev/null && { err "Memoire invalide : $MEM"; exit 1; }
ASTER_MEM=$(( MEM_MB - 512 ))      # Soustrait 512 MB pour le systeme
[ "$ASTER_MEM" -lt 512 ] && ASTER_MEM=512   # Plancher de securite

# ─────────────────────────────────────────────────────────────────
# Conversion de la duree en secondes
#
# Code_Aster attend time_limit en secondes dans le .export.
# awk utilise -F'[-:]' comme separateur de champs, ce qui decoupe
# a la fois sur '-' et ':', gerant nativement le format avec jours :
#
#   "2-00:00:00"  -> $1=2  $2=00 $3=00 $4=00  NF=4 -> 2*86400 = 172800 s
#   "05:00:00"    -> $1=05 $2=00 $3=00         NF=3 -> 5*3600  = 18000 s
#   "30:00"       -> $1=30 $2=00               NF=2 -> 30*60   = 1800 s
#   "60"          -> $1=60                     NF=1 -> 60*60   = 3600 s
# ─────────────────────────────────────────────────────────────────
TIME_SEC=$(echo "$TIME_LIMIT" | awk -F'[-:]' '
    NF==4 {print $1*86400+$2*3600+$3*60+$4; next}  # J-HH:MM:SS
    NF==3 {print $1*3600+$2*60+$3;          next}  # HH:MM:SS
    NF==2 {print $1*60+$2;                  next}  # MM:SS
    {print $1*60}')                                 # MM

# ─────────────────────────────────────────────────────────────────
# Generation du fichier .export
#
# Le .export est le fichier de configuration de Code_Aster.
# Format de chaque ligne :
#   P param valeur          -> parametre global du calcul
#   F type chemin D|R unite -> fichier (F) ou repertoire (R)
#                              D = entree (Donnee), R = sortie (Resultat)
#                              unite = numero d'unite logique Fortran
#
# Le bloc { ... } > "$EXPORT" redirige toute la sortie standard du groupe
# de commandes vers le fichier .export en une seule operation.
# ─────────────────────────────────────────────────────────────────
$QUIET || section "Generation du .export"
EXPORT="${SCRATCH}/${STUDY_NAME}.export"
{
    # Parametres globaux du calcul
    echo "P time_limit $TIME_SEC"       # Duree max en secondes (arret force par Aster)
    echo "P memory_limit $ASTER_MEM"   # Memoire max en MB
    echo "P ncpus $NTASKS"             # Nombre de CPUs/taches MPI

    # Fichiers d'entree principaux
    echo "F comm ${SCRATCH}/$(basename "$COMM") D 1"    # Unite 1 : fichier de commandes
    [ -n "$MED" ]  && echo "F mmed ${SCRATCH}/$(basename "$MED") D 20"    # Unite 20 : maillage MED
    [ -n "$MAIL" ] && echo "F mail ${SCRATCH}/$(basename "$MAIL") D 20"   # Unite 20 : maillage ASTER

    # Fichiers de sortie standard
    echo "F mess ${SCRATCH}/${STUDY_NAME}.mess R 6"              # Unite 6 : messages (log Aster)
    echo "F resu ${SCRATCH}/${STUDY_NAME}.resu R 8"              # Unite 8 : resultats texte
    echo "F rmed ${SCRATCH}/${STUDY_NAME}_resu.rmed R 80"        # Unite 80 : resultats MED (ParaVis)

    # Sorties supplementaires definies par l'utilisateur via -R.
    # Format attendu : "type:unite,type:unite,..." (ex: "rmed:81,csv:38")
    # Les espaces sont nettoyes pour accepter "rmed:81, csv:38".
    # ${item%%:*} extrait la partie avant ':' (type)
    # ${item##*:} extrait la partie apres ':' (unite)
    if [ -n "$RESULTS" ]; then
        RESULTS_CLEAN="${RESULTS// /}"   # Supprime tous les espaces
        IFS=',' read -ra ITEMS <<< "$RESULTS_CLEAN"
        for item in "${ITEMS[@]}"; do
            TYPE="${item%%:*}"; UNIT="${item##*:}"
            # Correspondance type -> extension de fichier
            case "$TYPE" in
                rmed) EXT="med" ;; resu) EXT="resu" ;; mess) EXT="mess" ;;
                csv) EXT="csv" ;; table) EXT="table" ;; dat) EXT="dat" ;;
                pos) EXT="pos" ;; *) EXT="$TYPE" ;;
            esac
            echo "F ${TYPE} ${SCRATCH}/${STUDY_NAME}_u${UNIT}.${EXT} R ${UNIT}"
        done
    fi

    # Repertoire de sortie libre (REPE_OUT) : certaines commandes Aster
    # (ex: IMPR_RESU avec repertoire) ecrivent dans ce dossier.
    # "R" en debut de ligne = type repertoire (vs "F" pour fichier).
    echo "R ${SCRATCH}/REPE_OUT R 0"

} > "$EXPORT"

# Affiche le contenu du .export genere (sauf en mode quiet)
if ! $QUIET; then
    ok "Export : $EXPORT"
    while IFS= read -r line; do info "  $line"; done < "$EXPORT"
fi

# ─────────────────────────────────────────────────────────────────
# Recapitulatif des ressources demandees
# ─────────────────────────────────────────────────────────────────
if ! $QUIET; then
    section "Ressources Slurm"
    info "Partition : $PARTITION | Noeuds : $NODES | Taches : $NTASKS | CPUs : $CPUS"
    info "Memoire   : $MEM (${ASTER_MEM}MB pour Aster) | Duree : $TIME_LIMIT"
    [ "$KEEP_SCRATCH" = "1" ] && info "Scratch   : conserve"
fi

# ─────────────────────────────────────────────────────────────────
# Soumission via sbatch
#
# sbatch est la commande de soumission de Slurm.
# --parsable : affiche uniquement le JOB ID sur stdout (pas de message)
# --export   : liste des variables d'environnement transmises au job.
#              "ALL" transmet tout l'environnement courant, puis on
#              ajoute les variables __* propres au script.
#              Ces variables permettent a la Phase 2 de retrouver tous
#              les chemins et options sans avoir a les re-parser.
#
# SELF = chemin absolu de ce script (obtenu via realpath $0).
# On soumet CE MEME fichier, qui en Phase 2 entrera dans le bloc
# if [ "${__RUN_PHASE:-}" = "EXEC" ] grace a la variable transmise.
# ─────────────────────────────────────────────────────────────────
$QUIET || section "Soumission Slurm"

SELF="$(realpath "$0")"

# Construction de la chaine de variables a exporter
VARS="ALL"
VARS+=",__RUN_PHASE=EXEC"             # Signal pour activer la Phase 2
VARS+=",__STUDY_DIR=${STUDY_DIR}"     # Chemin absolu du dossier d'etude
VARS+=",__STUDY_NAME=${STUDY_NAME}"   # Nom de l'etude (base des noms de fichiers)
VARS+=",__SCRATCH=${SCRATCH}"         # Chemin absolu du scratch
VARS+=",__EXPORT=${EXPORT}"           # Chemin absolu du .export genere
VARS+=",__ASTER_ROOT=${ASTER_ROOT}"   # Racine Code_Aster pour trouver l'executable
VARS+=",__MODULE=${ASTER_MODULE}"     # Nom du module a charger (peut etre vide)
VARS+=",__KEEP_SCRATCH=${KEEP_SCRATCH}"  # 1 = ne pas supprimer le scratch
VARS+=",__DEBUG=${DEBUG}"             # 1 = activer set -x en Phase 2
# Construction de la commande sbatch sous forme de tableau bash.
# Un tableau evite les problemes de quoting avec les espaces dans les chemins.
CMD=(sbatch --parsable
    --job-name="aster_${STUDY_NAME}"        # Nom visible dans squeue
    --partition="$PARTITION"
    --nodes="$NODES"
    --ntasks="$NTASKS"
    --cpus-per-task="$CPUS"
    --mem="$MEM"
    --time="$TIME_LIMIT"
    --output="${STUDY_DIR}/aster_%j.out"    # %j = JOB ID, remplace automatiquement
    --error="${STUDY_DIR}/aster_%j.err"
    --export="$VARS"
    "$SELF"   # Ce meme script, execute en Phase 2 sur le noeud de calcul
)

# Mode dry-run : affiche la commande sans la soumettre (utile pour verifier)
if [ "$DRY_RUN" = "1" ]; then
    section "DRY RUN — commande sbatch (non lancee)"
    echo "  ${CMD[*]}"
    exit 0
fi

# Soumission reelle : capture le JOB ID retourne par sbatch --parsable
JOB=$("${CMD[@]}") || { err "sbatch a echoue"; exit 1; }
[ -z "$JOB" ] && { err "Job ID vide"; exit 1; }

# Affichage final : mode quiet -> juste le JOB ID (pour scripts)
#                  mode normal -> commandes utiles pour suivre le job
if $QUIET; then
    echo "$JOB"
else
    ok "Job $JOB soumis"
    echo ""
    echo "  squeue -j $JOB"                                        # Etat du job
    echo "  tail -f ${STUDY_DIR}/aster_${JOB}.out"                # Logs temps reel
    echo "  scancel $JOB"                                          # Annuler
    echo "  ls ${STUDY_DIR}/run_${JOB}/"                          # Resultats rapatries
    echo ""
fi
