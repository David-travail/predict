# Détecteur de tendances crypto H24 – Fonctionnement

Résumé reconstruit à partir du code source (`src/`) et de `config.toml`.
L'historique des conversations précédentes n'étant pas accessible, le code fait foi.

## 1. Indicateurs

Valeurs par défaut (`config.toml`) : unité `1h`, EMA 200 + Supertrend, ADX 14 / seuil 22, volume SMA 20 / ×1,2, score minimal d'alerte 80.

### EMA (`indicators/ema.rs`) – filtre de tendance de fond
- Moyenne mobile exponentielle des **clôtures**, période `ema_period` (200).
- Amorçage : première valeur = SMA des 200 premières clôtures ; ensuite `EMA = (prix − EMA_préc) × k + EMA_préc`, avec `k = 2 / (période + 1)`.
- Retourne `None` s'il y a moins de 200 bougies → tendance neutre.
- **Condition** : clôture > EMA → hausse ; clôture < EMA → baisse.

### Supertrend (`indicators/supertrend.rs`) – confirmation de tendance
- Actif si `use_supertrend = true`. Paramètres `supertrend_period` (10) et `supertrend_multiplier` (3,0), modifiables dans `config.toml` et dans l'IHM (« ATR période » et « × », actifs si la case Supertrend est cochée ; appliqués par « Appliquer »).
- True Range = max(haut−bas, |haut−clôture_préc|, |bas−clôture_préc|) ; ATR lissé de Wilder (amorcé par la moyenne des 10 premiers TR).
- Pour chaque bougie : milieu = (haut+bas)/2 ; bande haute = milieu + 3×ATR ; bande basse = milieu − 3×ATR.
  Une bande ne s'éloigne du prix que si la clôture précédente l'a franchie (sinon elle est conservée).
- Bascule : haussier → baissier si clôture < bande basse ; baissier → haussier si clôture > bande haute. Départ en haussier.
- Retourne `true` si la dernière bougie est haussière ; `None` (traité comme `false`) si < 11 bougies.

### ADX (`indicators/adx.rs`) – force de la tendance (pas son sens)
- Lissage de Wilder, période `adx.period` (14). Nécessite ≥ 2×période bougies, sinon `None` → ADX = 0.
- Par bougie : TR, +DM (haut − haut_préc) et −DM (bas_préc − bas), seul le mouvement dominant et positif est retenu.
- Les 14 premières valeurs sont sommées ; ensuite `lissé = lissé − lissé/14 + valeur`.
- `+DI = 100 × +DM_lissé / TR_lissé`, `−DI` idem, `DX = 100 × |+DI − −DI| / (+DI + −DI)`.
- ADX = moyenne des 14 premiers DX, puis `ADX = (ADX_préc × 13 + DX) / 14`.
- **Condition** : `ADX ≥ min_threshold` (22).

### Volume (`indicators/volume.rs`) – confirmation par cassure de volume
- `SMA` = moyenne des `sma_period` (20) derniers volumes (la bougie analysée est incluse dans la série).
- `ratio = volume_bougie / SMA` (affiché « Vol Ratio »). SMA nulle : ratio = ∞ si volume > 0, sinon 0.
- **Condition** : `volume_bougie > multiplier × SMA` (1,2 × SMA). Sans SMA calculable : `false`, ratio 0.

### Score (`core/scoring.rs`) – 0 à 100
| Élément | Points |
|---|---|
| Tendance de fond haussière **ou baissière** (EMA, et Supertrend de même sens si activé) | 40 |
| ADX ≥ seuil | +30 |
| Volume en cassure | +30 |

- **Tendance neutre ⇒ score = 0** (ADX et volume ne comptent pas). Le sens (hausse/baisse) de l'alerte est affiché dans l'IHM et en console.
- Valeurs possibles : 0, 40, 70, 100. **Alerte** si `score ≥ min_score_alert` (80 par défaut ⇒ en pratique seul 100 déclenche une alerte).
- Le seuil est réglable à chaud dans l'IHM (curseur « Seuil d'alerte »).

## 2. Fonctionnement de l'application

### Architecture
- `predict --serve` lance le moteur Tokio et son serveur de contrôle réseau dans la console ; l'IHM lancée séparément se connecte au moteur (`predict` par défaut sur `127.0.0.1:8765`, ou `predict --connect <IP>:8765`). Le serveur écoute par défaut sur `0.0.0.0:8765` ; `--listen` permet de choisir une autre adresse. L'IHM peut changer de mode et arrêter/redémarrer le traitement. Fermer ou perdre l'IHM ne coupe pas le moteur ; seul `Ctrl+C` dans la console du serveur l'arrête.
- `predict --local` lance le moteur et l'IHM ensemble dans le même processus, sans serveur réseau ; fermer la fenêtre de l'IHM arrête également le moteur.
- Les notifications Telegram sont facultatives et se configurent dans la section `[telegram]` de `config.toml`. Renseigner ensemble `bot_token` et `chat_id` pour les activer ; les laisser vides pour les désactiver. Le moteur envoie un message à son démarrage, puis une notification lorsqu'une bougie clôturée fait passer la tendance d'une paire à la hausse ou à la baisse. Les mises à jour provisoires, la simulation et les rejeux historiques ne déclenchent pas de notification de tendance. Les erreurs d'envoi sont consignées dans `predict-erreur.<date>.log` ; si la réponse HTTP n'est pas au format Telegram, un extrait du corps reçu est ajouté au diagnostic (jeton masqué et séquences ANSI retirées).

```toml
[telegram]
bot_token = "jeton fourni par BotFather"
chat_id = "identifiant de la conversation"
```

Le fichier de configuration pouvant contenir le secret du bot, ne pas versionner un jeton réel.

- Le serveur exige le même secret dans la variable d'environnement `PREDICT_TOKEN` côté moteur et côté IHM (au moins 16 caractères). Les messages sont échangés en TCP/JSON ; le trafic n'est pas chiffré, donc utiliser un réseau privé de confiance ou un VPN.
- « Sauvegarder » écrit les paramétrages sur la machine du moteur ; ils seront rechargés au prochain démarrage du serveur.
- `predict --console` reste disponible pour une analyse console autonome (sans serveur ni IHM).
- Les signaux et statuts du moteur sont transmis à l'IHM ; les erreurs des sources sont journalisées et affichées dans une boîte de dialogue ; la barre d'état du temps réel n'affiche que les noms des flux actifs.
- Les messages de niveau INFO sont affichés sur la sortie standard et enregistrés dans `predict.<date>.log`. Les avertissements (WARN) et erreurs (ERROR) sont affichés sur la sortie d'erreur et enregistrés dans `predict-erreur.<date>.log`. La rotation se fait quotidiennement pendant l'exécution ; les deux journaux sont conservés pendant la durée réglée par `[logs].retention_days` dans `config.toml` (7 jours par défaut). Les séquences ANSI de couleur sont désactivées. Les fichiers sont créés dans le dossier de lancement de l'application.
- Modes : **temps-reel** (sources activées dans `[data_sources]` : Binance, Kraken et Coinbase), **simulation** (prix fictifs), **periode** (backtest téléchargé depuis Binance, sauvegardé dans `historique_<début>_<fin>.json`), **fichier** (rejeu d'un JSON). L'unité de temps des bougies (`timeframes.execution`, 1h par défaut) est modifiable dans l'IHM (liste « Période » : 1m … 1d, prise en compte par « Appliquer »). Proxy HTTP possible (`[network] proxy`), délai minimal entre appels REST `rate_limit_ms` (200 ms).

### Sources de données en temps réel

Les trois fournisseurs sont activés par défaut. En mode Temps réel, les cases à cocher « Flux de données » permettent de choisir les sources ; chaque changement relance la connexion avec la sélection. Il faut garder au moins un fournisseur actif. La sélection initiale se règle aussi dans `config.toml` :

```toml
[data_sources]
providers = ["binance", "kraken", "coinbase"]
```

Les bougies sont ramenées au symbole interne (par exemple `BTCEUR`) puis rapprochées par symbole, période et heure d'ouverture. Une bougie est agrégée dès que toutes les sources sélectionnées sont présentes, ou après 5 secondes si au moins deux sources concordent. Avec une seule source sélectionnée, ses bougies sont utilisées telles quelles. Ouverture, haut, bas et clôture sont les moyennes arithmétiques des sources reçues ; le volume est leur somme. Les mises à jour de sources supplémentaires sont intégrées tant que la bougie n'est pas clôturée. Si plusieurs sources sont sélectionnées, le démarrage requiert assez d'historique commun depuis au moins deux d'entre elles.

Coinbase publie des transactions ; l'application reconstruit les bougies sur l'unité de temps configurée. Kraken publie ses bougies OHLC et Binance ses kline. Le préchauffage REST est téléchargé sur chaque source et la bougie courante est amorcée avec les valeurs REST avant le branchement des WebSockets ; les backtests par période restent actuellement basés sur Binance. Les trois API couvrent en commun `1m`, `5m`, `15m`, `1h` et `1d`. Les autres périodes dépendent des intervalles offerts par les fournisseurs ; le préchauffage doit obtenir 200 bougies concordantes sur au moins deux sources, sinon le démarrage échoue explicitement.

### Rejeu d'un historique

- **Fichier local** : sélectionner un fichier JSON contenant une liste de bougies (`symbole`, `unite_temps`, `instant_ouverture`, `ouverture`, `haut`, `bas`, `cloture`, `volume`, `cloturee`). Choisir également les paires à jouer ; chacune doit être présente dans le fichier. Les bougies des autres paires sont ignorées. Un fichier vide, illisible, mal formé ou ne contenant pas une paire sélectionnée provoque une erreur explicite.
- Le moteur force toutes les bougies retenues à l'état clôturé, les trie par heure d'ouverture et les traite une par une, sans délai entre elles. Chaque paire dispose de son propre historique cumulatif, initialement vide ; la bougie traitée y est ajoutée avant le calcul des indicateurs. Cet historique est limité aux 300 dernières bougies.
- À chaque étape, le score et le signal sont calculés avec les paramétrages actifs. L'IHM reçoit les signaux et ajoute les alertes à son journal ; leur horodatage correspond à l'heure d'ouverture de la bougie, affichée dans le fuseau local. La console du mode backtest n'affiche que les alertes. À la fin, un statut indique le nombre de bougies analysées et d'alertes détectées.
- **Backtest par période** : les bougies de la période choisie sont également rejouées chronologiquement, mais les 200 bougies précédant le début sont chargées séparément pour amorcer les indicateurs ; elles ne sont pas comptées parmi les bougies rejouées. Le fichier `historique_<début>_<fin>.json` sauvegardé contient le préchauffage suivi des bougies de la période. Si ce fichier est ensuite ouvert en mode « Fichier local », toutes ses bougies — préchauffage compris — seront rejouées depuis un historique vide.

### Messages de l'API Binance

**REST – historique Binance** : `GET https://api.binance.com/api/v3/klines?symbol=…&interval=1h&limit=…[&startTime=…&endTime=…]`
- Réponse : tableau de tableaux `[ouverture_ms, "open", "high", "low", "close", "volume", clôture_ms, …]` (≥ 7 champs ; prix et volume sont des chaînes).
- Convertie en `Bougie` ; `cloturee = (instant de clôture < maintenant)`.
- Limite 1000 bougies par appel ; une plage longue est paginée (curseur = dernière ouverture + 1 ms).
- Au démarrage en temps réel : **200 bougies de préchauffage** par paire (nécessaires à l'EMA 200 et à l'ADX), seules les bougies clôturées sont gardées.

**WebSocket Binance – temps réel** : `wss://stream.binance.com:443/stream?streams=btceur@kline_1h/ethusdt@kline_1h/…` (flux combiné, un flux par paire).
- Message : `{"stream":"btceur@kline_1h","data":{"e":"kline","s":"BTCEUR","k":{"t":…,"s":…,"i":"1h","o":…,"h":…,"l":…,"c":…,"v":…,"x":true|false}}}`.
- `e != "kline"` : ignoré. `k.x` = `true` → bougie **clôturée** ; `false` → bougie **en cours** (mise à jour environ toutes les 2 s).
- Ping serveur (toutes les 20 s) → Pong ; plus de 60 s sans message → connexion jugée morte.
- Reconnexion automatique avec délai exponentiel 1 s, 2 s, 4 s … plafonné à 60 s (remis à 1 s après une connexion réussie).

### Calcul à chaque heure (bougie 1h)
1. **Démarrage** : téléchargement REST de 200 bougies clôturées par paire ; calcul et affichage immédiat du dernier signal de chaque paire (sans alerte).
2. **Bougie en cours** (`x = false`) : analyse **provisoire** sur `historique + bougie courante` (non stockée). Affichage rafraîchi toutes les ~2 s, horodatage = heure locale `HH:MM:SS`.
3. **Clôture de l'heure** (`x = true`) : la bougie est ajoutée à l'historique (plafonné à 300 bougies, la plus ancienne est retirée), puis l'analyse est recalculée sur cette série.
4. **Pour chaque analyse** (`construire_signal`) :
   - EMA 200 des clôtures → `clôture > EMA` (hausse) ou `clôture < EMA` (baisse) ;
   - si Supertrend activé : il doit confirmer le même sens (haussier pour une hausse, baissier pour une baisse), sinon tendance neutre → `tendance` (Haussière / Baissière / Neutre) ;
   - ADX 14 sur les bougies (haut/bas/clôture) → `adx_valide` ;
   - SMA 20 du volume et ratio → `volume_valide` ;
   - score = 40 / +30 / +30 ; `est_alerte = score ≥ seuil`.
5. **Résultat** : `SignalVisuel` (symbole, prix, prix précédent, score, ADX, ratio volume, drapeaux macro/ADX/volume, alerte, horodatage) envoyé à l'IHM. Une alerte est ajoutée au journal (5000 max, cliquable pour revoir ses données) et, en console, tracée par `🚨 ALERTE | Symbole | Score | Prix | ADX | Vol Ratio` (sinon `📊 ANALYSE`).
6. **Backtest** (periode/fichier) : bougies triées chronologiquement, toutes paires confondues, toutes considérées clôturées ; horodatage = heure d'ouverture de la bougie (`JJ/MM HH:MM`) ; fin par `✅ Test terminé … N bougies analysées, X alertes`.

Remarque : la décision d'alerte « officielle » correspond à la bougie clôturée ; les analyses provisoires en cours d'heure peuvent déclencher une alerte qui disparaît si la bougie se termine autrement.

## 3. Réception et traitement des messages (chaîne complète)

```
Binance WebSocket ──► ecouter_flux_temps_reel ──canal Bougie (100)──► traiter_flux
                                                                          │
                                                              traiter_bougie / construire_signal
                                                                          │
IHM egui (update) ◄──canal SignalVisuel (256)────────────────────────────┘
```

### Étape 1 – Réception (`data/websocket.rs`)
1. Connexion à `wss://stream.binance.com:443/stream?streams=<paire>@kline_1h/...` (directe, ou via tunnel HTTP `CONNECT` si un proxy est configuré).
2. Boucle de lecture avec délai d'inactivité de 60 s :
   - `Text` → `analyser_message` ;
   - `Ping` → réponse `Pong` ;
   - `Close` ou erreur ou silence > 60 s → connexion abandonnée puis reconnexion (1 s, 2 s, 4 s … 60 s max).
3. `analyser_message` : JSON → enveloppe `data` → ignoré si `e != "kline"` → lecture des champs `s, i, t, o, h, l, c, v, x` (prix/volume en chaînes converties en `f64`). Un message mal formé est ignoré avec un avertissement (le flux continue).
4. La `Bougie` (clôturée ou non) est envoyée dans le canal vers le moteur. Si le récepteur est fermé, l'écoute s'arrête.

### Étape 2 – Traitement (`main.rs`, `traiter_bougie`)
1. L'historique de la paire est retrouvé par symbole (majuscules). Symbole inconnu : message ignoré.
2. **Bougie clôturée** : ajoutée à l'historique (max 300 bougies), analyse sur cet historique.
   **Bougie en cours** : analyse sur une copie `historique + bougie`, sans modifier l'historique.
3. `construire_signal` calcule EMA / Supertrend / ADX / volume, le score et l'alerte (voir §1) et fabrique un `SignalVisuel`.
4. Le signal est envoyé à l'IHM ; si l'IHM est fermée, la session s'arrête.

### Étape 3 – Affichage (`ui.rs`, `update`, rappelée en continu)
1. Les signaux sont lus dans le canal ; ceux dont la `generation` ne correspond pas à la session courante (ancien mode) sont jetés.
2. **Indicateur de réception** (modes Temps Réel et Simulation, barre supérieure) : à chaque signal reçu après l'ouverture du flux (statut commençant par 🔴 ou 🟡), le compteur de messages augmente et l'heure du dernier message est mémorisée. Couleur du point :
   - gris : aucun message encore reçu ;
   - vert : dernier message < 10 s ;
   - jaune : entre 10 et 59 s ;
   - rouge : ≥ 60 s (flux probablement coupé, reconnexion en cours).
   Le compteur est remis à zéro à chaque changement de mode. Il compte les signaux traités, pas les messages WebSocket bruts (les événements ignorés ne le font pas avancer).
3. Le signal remplace la dernière valeur de la paire ; s'il est une alerte, il est inséré en tête du journal (5000 max).
4. Les statuts d'exécution et notifications d'erreur arrivent par un canal séparé. Les erreurs des sources sont journalisées (`warn`/`error`) et présentées dans une boîte de dialogue ; la barre d'état en direct conserve uniquement les noms des flux actifs. Les statuts normaux restent affichés dans la barre d'état.
