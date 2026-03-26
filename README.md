# run_aster.sh

Script Bash pour lancer des calculs **Code_Aster** sur un cluster **Slurm**, depuis le nœud login jusqu'au nœud de calcul, en un seul fichier.

---

## Fonctionnement en deux phases

Le script utilise une architecture en deux phases contenues dans le **même fichier** :

```
Nœud login                          Nœud de calcul
──────────────────────────────────  ──────────────────────────────────
bash run_aster.sh mon_etude/        sbatch soumet CE MÊME script
  │                                   avec __RUN_PHASE=EXEC
  ├─ Détecte .comm / .med / .mail
  ├─ Crée le dossier scratch
  ├─ Copie les fichiers
  ├─ Génère le .export                ├─ Charge Code_Aster
  └─ sbatch → ───────────────────►    ├─ Lance le calcul (run_aster gère MPI en interne)
                                      ├─ Diagnostic du .mess
                                      ├─ Rapatrie scratch → run_JOBID/
                                      └─ Résumé final
```

**Phase 1** (nœud login) : préparation et soumission.

**Phase 2** (nœud de calcul) : exécution, détectée par la variable `__RUN_PHASE=EXEC` transmise via `sbatch --export`.

---

## Prérequis

- Slurm (`sbatch`, `squeue`, `scancel`)
- Code_Aster installé sous `ASTER_ROOT` ou accessible via un module Lmod
- Un répertoire scratch partagé entre nœud login et nœuds de calcul

---

## Configuration

En tête du script, trois variables à adapter à l'installation :

```bash
ASTER_ROOT="${ASTER_ROOT:-/opt/code_aster}"   # chemin vers Code_Aster
ASTER_MODULE="${ASTER_MODULE:-code_aster}"    # module Lmod (vide si pas de module)
SCRATCH_BASE="${SCRATCH_BASE:-/scratch}"      # scratch partagé login ↔ calcul
```

Elles peuvent aussi être surchargées par variable d'environnement :

```bash
export ASTER_ROOT=/logiciels/code_aster/17.1
bash run_aster.sh mon_etude/
```

---

## Usage

```bash
bash run_aster.sh [OPTIONS] [DOSSIER_ETUDE]
```

`DOSSIER_ETUDE` est le dossier contenant `.comm` et `.med` / `.mail`.
Par défaut : répertoire courant.

### Fichiers d'entrée

| Option | Description |
|--------|-------------|
| `-C, --comm FILE` | Fichier de commandes `.comm` (auto-détecté si absent) |
| `-M, --med FILE`  | Maillage au format MED (auto-détecté si absent) |
| `-A, --mail FILE` | Maillage au format ASTER natif (auto-détecté si absent) |



Si plusieurs fichiers `.comm` ou `.med` sont trouvés dans le dossier, le premier par ordre alphabétique est sélectionné (avertissement affiché).

Les fichiers annexes (`.py`, `.dat`, `.para`, `.include`, `.mfront`) présents dans le dossier d'étude sont copiés automatiquement dans le scratch.

### Ressources Slurm

| Option | Défaut | Description |
|--------|--------|-------------|
| `-p, --partition NOM` | `court`    | Partition Slurm |
| `-n, --nodes N`       | `1`        | Nombre de nœuds |
| `-t, --ntasks N`      | `1`        | Tâches MPI |
| `-c, --cpus N`        | `1`        | CPUs par tâche |
| `-m, --mem MEM`       | `5G`       | Mémoire par nœud |
| `-T, --time DUREE`    | `05:00:00` | Durée maximale |

Le format de durée accepté est `JJ-HH:MM:SS`, `HH:MM:SS`, `MM:SS` ou `SS`.

### Préréglages `-P`

Raccourcis pour les configurations typiques :

| Préréglage | Partition | Mémoire | Durée max |
|------------|-----------|---------|-----------|
| `court`    | court     | 2 G     | 5 h       |
| `moyen`    | moyen     | 8 G     | 3 jours   |
| `long`     | long      | 32 G    | 30 jours  |

Les options passées **après** `-P` remplacent les paramètres par défaut :

```bash
bash run_aster.sh -P moyen -t 8   # préréglage moyen, mais 8 tâches MPI
```

### Résultats supplémentaires `-R`

Déclare des fichiers de sortie additionnels (au-delà de `.mess`, `.resu`, `_resu.med`) :

```
-R "type:unite,type:unite,..."
```

#### Rappel des unités Code_Aster ####

| Unité | Type fichier | Extension | Direction | Description |
|:-----:|:------------|:---------:|:---------:|:------------|
| 1 | comm | `.comm` | D (entrée) | Fichier de commandes |
| 6 | mess | `.mess` | R (sortie) | Messages d'exécution (log) |
| 8 | resu | `.resu` | R (sortie) | Résultats texte (IMPR_RESU format RESULTAT) |
| 19 | mgib | `.mgib` | D (entrée) | Maillage format GIBI/Castem **(WIP)** |
| 20 | mmed | `.med` | D (entrée) | Maillage format MED (Salome) |
| 20 | mail | `.mail` | D (entrée) | Maillage format ASTER natif |
| 37 | pos | `.pos` | R (sortie) | Post-traitement Gmsh (IMPR_RESU format GMSH) |
| 38 | csv | `.csv` | R (sortie) | Tableau CSV (IMPR_TABLE format TABLEAU) |
| 39 | table | `.table` | R (sortie) | Table ASTER (IMPR_TABLE) |
| 40 | dat | `.dat` | R (sortie) | Données brutes / fichiers annexes |
| 80 | rmed | `.med` | R (sortie) | Résultats format MED (IMPR_RESU format MED) |
| 81+ | rmed | `.med` | R (sortie) | Résultats MED supplémentaires |

Types disponibles : `rmed`, `resu`, `mess`, `csv`, `table`, `dat`, `pos`

Dans le `.comm`, utiliser l'unité correspondante :

```python
IMPR_RESU(UNITE=81, ...)     # avec -R "rmed:81"
IMPR_TABLE(UNITE=38, ...)    # avec -R "csv:38"
```

### Autres options

| Option | Description |
|--------|-------------|
| `-q, --quiet`     | Sortie minimale — affiche uniquement le job ID |
| `-f, --follow`    | Suit le job après soumission : spinner PENDING → `tail -f` automatique en RUNNING → bilan final. Ctrl+C détache sans annuler le calcul. |
| `--keep-scratch`  | Conserver le dossier scratch après le calcul |
| `--dry-run`       | Afficher la commande sbatch sans la lancer |
| `--debug`         | Mode verbose (`set -x`) sur le nœud de calcul |
| `-h, --help`      | Afficher l'aide |

---

## Exemples

```bash
# Dossier courant, ressources par défaut
bash run_aster.sh

# Dossier explicite
bash run_aster.sh ~/calculs/poutre/

# Préréglages
bash run_aster.sh -P court mon_etude/
bash run_aster.sh -P moyen mon_etude/
bash run_aster.sh -P long  mon_etude/

# Préréglage surchargé
bash run_aster.sh -P moyen -t 8 -m 16G mon_etude/

# Fichiers explicites
bash run_aster.sh -C calcul.comm -M maillage.med

# Résultats additionnels
bash run_aster.sh -P moyen -R "rmed:81,csv:38" mon_etude/

# Suivre le job en temps réel
bash run_aster.sh -f mon_etude/
bash run_aster.sh -P moyen -f mon_etude/

# Récupérer juste l'ID (pour scripts)
JOB=$(bash run_aster.sh -q mon_etude/)

# Vérifier sans lancer
bash run_aster.sh --dry-run -P moyen mon_etude/
```

---

## Ce que fait le script automatiquement

### Phase 1 — nœud login

1. Détecte les fichiers `.comm`, `.med`, `.mail` dans le dossier d'étude
2. Crée `$SCRATCH_BASE/$USER/<etude>_<timestamp>_<pid>/`
3. Copie tous les fichiers d'entrée dans le scratch (y compris fichiers annexes)
4. Génère le fichier `.export` (configuration Code_Aster)
5. Soumet le job via `sbatch` en passant toutes les options via `--export`

### Phase 2 — nœud de calcul

1. Charge Code_Aster via `module load` ou chemin direct
2. Affiche le contenu du scratch et du `.export` (vérification)
3. Lance le calcul — `run_aster` gère MPI en interne, ne pas appeler via `srun`
4. Analyse le `.mess` : compte les alarmes `<A>`, erreurs fatales `<F>`, exceptions `<S>`, affiche les premières erreurs si présentes
5. **Rapatrie** les fichiers de résultat du scratch vers `$STUDY_DIR/run_$JOBID/`
6. Crée un lien symbolique `$STUDY_DIR/latest` → `run_$JOBID/`

### Gestion des interruptions (scancel / timeout)

Un `trap` sur `SIGTERM` et `EXIT` garantit que les résultats sont **toujours rapatriés**, même si le job est annulé (`scancel`) ou atteint sa limite de temps. Le double rapatriement est évité par un verrou interne.

---

## Structure des fichiers pendant l'exécution

```
$SCRATCH_BASE/$USER/<etude>_<timestamp>_<pid>/   ← scratch (calcul)
  ├─ mon_etude.comm
  ├─ mon_etude.med
  ├─ mon_etude.export
  ├─ mon_etude.mess
  ├─ mon_etude.resu
  ├─ mon_etude_resu.med
  └─ REPE_OUT/               ← répertoire de sortie libre (si utilisé)

$STUDY_DIR/                                      ← dossier d'étude
  ├─ aster_<jobid>.out       ← logs Slurm (temps réel via tail -f)
  ├─ aster_<jobid>.err
  ├─ latest -> run_<jobid>/  ← lien symbolique vers le dernier run
  └─ run_<jobid>/            ← résultats rapatriés
      ├─ mon_etude.mess
      ├─ mon_etude.resu
      ├─ mon_etude_resu.med
      └─ REPE_OUT/           ← si présent dans le scratch
```

---

## Suivre un calcul en cours

```bash
squeue -j <JOB_ID>                              # état du job
tail -f ~/calculs/mon_etude/aster_<JOB_ID>.out  # logs temps réel
scancel <JOB_ID>                                # annuler (rapatriement automatique)
ls ~/calculs/mon_etude/run_<JOB_ID>/            # résultats rapatriés
ls ~/calculs/mon_etude/latest/                  # dernier run (lien symbolique)
sview                                           # interface graphique de l'état du job
```

## Auteur

Téo LEROY — v9.0
