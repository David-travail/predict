# Communication IHM ↔ moteur : modèle réutilisable

Ce document décrit comment l'IHM (`ui`) dialogue avec le moteur (`engine`) dans
NetDiskMonitor. Il sert de modèle pour reproduire la même architecture dans un autre
projet. Les noms de fichiers et de fonctions renvoient au code actuel.

---

## 1. Vue d'ensemble

```
┌──────────────────────── IHM (eframe/egui) ────────────────────────┐
│                                                                    │
│  Thread graphique (egui)                 Thread réseau (tokio)     │
│  ───────────────────────                 ─────────────────────     │
│  update() à chaque image                 boucle select! :          │
│   ├─ receive_updates() ◄── std mpsc ───   • tick → GetStatus       │
│   │    (try_recv, non bloquant)           • requête reçue de l'IHM │
│   └─ clic/case cochée ── tokio mpsc ──►   → request_engine()       │
│        requests.send(Message…)            → résultat renvoyé       │
└──────────────────────────────────────────────┬─────────────────────┘
                                               │ TCP, 1 connexion
                                               │ par requête,
                                               │ 1 ligne JSON aller,
                                               │ 1 ligne JSON retour
┌──────────────────────── Moteur (tokio) ──────▼─────────────────────┐
│  TcpListener.accept() → tokio::spawn(handle_client)                │
│      lit 1 ligne → match sur l'enum → écrit 1 ligne → ferme        │
│                                                                    │
│  État partagé Arc<EngineState> :                                   │
│   • snapshot : RwLock<Snapshot>   ◄── écrit par les tâches de      │
│   • config   : watch::Sender<Config>   surveillance, lu par        │
│   • ui_clients : Mutex<HashMap<IpAddr, Instant>>   handle_client   │
│                                                                    │
│  Tâches de fond : monitor_disks, monitor_network, … (abonnées      │
│  au canal watch de la config → réveil immédiat si elle change)     │
└────────────────────────────────────────────────────────────────────┘
```

Principes clés :

1. **Le moteur est autonome.** Il surveille en permanence et tient un instantané à jour
   en mémoire, qu'une IHM soit connectée ou non. L'IHM n'est qu'un client.
2. **L'IHM ne garde pas de connexion ouverte.** Chaque échange = une connexion TCP courte :
   une requête, une réponse, fermeture. Pas d'état de session côté moteur, donc rien à
   resynchroniser après une coupure ou un redémarrage du moteur.
3. **Le moteur ne pousse rien.** L'IHM interroge (*polling*) toutes les N secondes avec
   `GetStatus` et reçoit un instantané complet.
4. **L'interface graphique ne bloque jamais.** Tout le réseau tourne dans un thread séparé,
   relié au thread graphique par deux canaux.
5. **Un seul contrat partagé.** Les types des messages sont dans la crate `common`,
   utilisée par les deux binaires : impossible que l'IHM et le moteur divergent à la
   compilation.

---

## 2. Le contrat : crate `common`

Fichier : [common/src/lib.rs](common/src/lib.rs)

Deux enums `serde`, un par sens :

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageClientToEngine {
    GetStatus,                                   // lecture périodique
    ScanLargeFiles { disques: Vec<String> },     // action longue
    ScanDirectories { repertoires: Vec<String> },// action longue, sans taille minimale
    ListDirectory { path: String },              // navigation dans les dossiers du moteur
    DeleteFiles { paths: Vec<String> },          // action longue
    UpdateConfig(Config),                        // (non utilisé par l'IHM actuelle)
    SetMonitoring { disque: bool, reseau: bool },// réglage à chaud
    SetTelegramAlerts(TelegramAlerts),           // réglage à chaud + enregistré
    TestTelegram,                                // action avec retour
    UiClosing,                                   // l'IHM annonce sa fermeture
    SetDeviceAlias { mac: String, alias: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageEngineToClient {
    StatusSnapshot { /* tout l'état affichable */ },
    LargeFilesSnapshot { files: Vec<LargeFileDetails> },
    DirectoryListing { path, parent, directories, tronque, erreur }, // erreur incluse
    FilesDeleted { success: bool, message: String },
    ActionResult { success: bool, message: String }, // réponse générique
}
```

Règles suivies :

- **Réponse générique `ActionResult { success, message }`** pour toutes les commandes qui
  n'ont pas de donnée à renvoyer. Le `message` est déjà rédigé pour l'utilisateur :
  l'IHM l'affiche tel quel. Les erreurs du moteur (JSON invalide, validation, échec
  d'E/S) reviennent aussi sous cette forme, jamais par une coupure muette.
- **`StatusSnapshot` contient tout ce que l'IHM affiche en continu** (disques, appareils,
  événements récents, état des surveillances, seuil, alertes, charge du moteur,
  avertissements). L'IHM remplace son état par l'instantané : pas de différentiel.
- **Compatibilité ascendante** : les champs ajoutés après coup portent
  `#[serde(default)]`, pour qu'une IHM récente parle encore à un moteur plus ancien
  (le champ manquant prend sa valeur par défaut). Les `Option<…>` servent à dire
  « pas encore mesuré ».
- Un test `messages_round_trip_as_json` vérifie l'aller-retour JSON des messages.

### Format sur le fil

Encodage `serde_json` par défaut (enum « externally tagged »), **une ligne par message,
terminée par `\n`** :

```
IHM → moteur : "GetStatus"\n
IHM → moteur : {"ScanLargeFiles":{"disques":["/"]}}\n
IHM → moteur : {"SetMonitoring":{"disque":false,"reseau":true}}\n
moteur → IHM : {"ActionResult":{"success":true,"message":"Configuration mise à jour."}}\n
```

Pratique pour déboguer à la main :

```sh
echo '"GetStatus"' | nc 127.0.0.1 7878
```

---

## 3. Côté moteur

Fichier : [engine/src/main.rs](engine/src/main.rs)

### 3.1 État partagé

```rust
struct EngineState {
    snapshot: RwLock<Snapshot>,          // données mesurées, lues par GetStatus
    config: watch::Sender<Config>,       // config vivante, modifiable à chaud
    aliases: RwLock<HashMap<String, String>>,
    telegram: mpsc::UnboundedSender<TelegramRequest>,
    ui_clients: std::sync::Mutex<HashMap<IpAddr, Instant>>, // IHM vues récemment
    /* … */
}
```

Créé une fois dans `main`, partagé via `Arc` entre les tâches de surveillance et chaque
connexion cliente.

### 3.2 Boucle d'écoute

```rust
let listener = TcpListener::bind(&config.adresse_ecoute_moteur).await?;
loop {
    tokio::select! {
        _ = &mut shutdown => break,                 // Ctrl+C / SIGTERM
        accepted = listener.accept() => if let Ok((stream, _)) = accepted {
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, state).await { eprintln!("{e}"); }
            });
        },
    }
}
```

Une tâche par connexion : une analyse de disque longue ne bloque pas les `GetStatus`
des autres connexions.

### 3.3 Traitement d'une requête : `handle_client`

1. Récupère l'IP du pair (`peer_addr`) — sert à suivre les IHM connectées.
2. Lit **une seule ligne**, avec deux protections :
   - taille maximale (`.take(MAX_REQUEST_BYTES)`, 1 Mio) ;
   - délai maximal (`timeout(CLIENT_REQUEST_TIMEOUT, read_line)`, 10 s).
3. `serde_json::from_str::<MessageClientToEngine>` puis `match` sur la variante.
   Une erreur de décodage produit un `ActionResult { success: false, … }`.
4. Sérialise la réponse, ajoute `\n`, l'écrit, et la connexion se ferme en sortant.

Selon le type de requête :

| Type | Exemple | Traitement |
|---|---|---|
| Lecture | `GetStatus` | lit `snapshot` (RwLock en lecture) et `config.borrow()`, construit l'instantané |
| Réglage à chaud | `SetMonitoring` | `config.send_if_modified(…)` → les tâches abonnées se réveillent aussitôt |
| Réglage persistant | `SetTelegramAlerts` | modifie la config vivante **et** réécrit `config.toml` avec `toml_edit` (commentaires conservés, écriture `.tmp` puis renommage atomique) |
| Action lourde / bloquante | `ScanLargeFiles` | `tokio::task::spawn_blocking` pour ne pas geler le runtime ; résultat stocké dans `snapshot` |
| Lecture du système de fichiers du moteur | `ListDirectory` | l'IHM parcourt les dossiers **de la machine du moteur** (pas les siens) : indispensable quand le moteur tourne ailleurs. L'erreur éventuelle est renvoyée dans `DirectoryListing` lui-même, pour que l'IHM sache à quelle demande elle se rapporte |
| Action sensible | `DeleteFiles` | n'accepte que des chemins présents dans le **dernier résultat de scan** du moteur (l'IHM ne peut pas faire supprimer un chemin arbitraire) |

### 3.4 Configuration vivante : canal `watch`

```rust
// Écriture (handle_client)
state.config.send_if_modified(|c| { c.activer_surveillance_disque = d; changed });

// Lecture (tâche de surveillance)
let mut config_rx = state.config.subscribe();
loop {
    let config = config_rx.borrow().clone();
    if config.activer_surveillance_disque { /* travail */ }
    tokio::select! {
        _ = sleep(intervalle) => {}
        r = config_rx.changed() => if r.is_err() { return; }, // réveil immédiat
    }
}
```

Avantage : une case cochée dans l'IHM prend effet tout de suite, sans attendre la fin
de l'intervalle de scrutation. `send_if_modified` renvoyant `false` permet de changer
une valeur sans réveiller les tâches (utilisé pour les alertes Telegram, relues au
prochain cycle).

### 3.5 Présence des IHM (sans connexion permanente)

Le moteur déduit la présence des IHM du polling :

- **Connexion** : `register_ui_poll` — au premier `GetStatus` d'une IP inconnue,
  l'IP est ajoutée à `ui_clients` et l'événement est notifié.
- **Fermeture propre** : l'IHM envoie `UiClosing` en quittant → `ui_closing` retire l'IP.
- **Disparition brutale** : `watch_ui_clients` vérifie toutes les 5 s ; une IP muette
  depuis plus de `3 × intervalle_actualisation_ihm_secondes + 10 s` est déclarée
  déconnectée.

Limite : l'identification est par IP ; deux IHM sur la même machine comptent pour une.

---

## 4. Côté IHM

Fichier : [ui/src/main.rs](ui/src/main.rs)

### 4.1 Deux canaux, deux threads

```rust
// main()
let (updates_tx, updates_rx)   = std::sync::mpsc::channel();        // réseau → UI
let (requests_tx, requests_rx) = tokio::sync::mpsc::unbounded_channel(); // UI → réseau
start_status_polling(updates_tx, requests_rx, adresse_moteur, intervalle);
eframe::run_native(…, NetDiskMonitorApp::new(updates_rx, requests_tx, …));
announce_ui_closing(&adresse_moteur);   // après la fermeture de la fenêtre
```

- **UI → réseau** : canal `tokio::mpsc` non borné. `send` est synchrone et non
  bloquant, utilisable depuis le code egui ; `recv` est `async`, utilisable dans le
  `select!` du thread réseau.
- **Réseau → UI** : canal `std::mpsc`. Le thread graphique le vide avec `try_recv`
  (jamais bloquant). Il transporte `Result<MessageEngineToClient, String>` : les erreurs
  réseau arrivent par le même chemin que les réponses.

### 4.2 Thread réseau : `start_status_polling`

Un `std::thread` qui crée son propre runtime tokio `current_thread`
(l'IHM n'a pas de runtime tokio global) :

```rust
let mut ticker = tokio::time::interval(refresh_interval);
loop {
    let request = tokio::select! {
        _ = ticker.tick() => MessageClientToEngine::GetStatus,
        r = requests.recv() => match r { Some(r) => r, None => break }, // UI fermée
    };
    let result = request_engine(request, &engine_address).await;
    if updates.send(result).is_err() { break; }                         // UI fermée
}
```

Le thread s'arrête de lui-même quand la fenêtre se ferme (un des canaux est fermé).

### 4.3 Un échange : `request_engine`

1. `TcpStream::connect` avec `CONNECT_TIMEOUT` (5 s).
2. Écrit le JSON + `\n`.
3. Lit une ligne de réponse :
   - avec `RESPONSE_TIMEOUT` (10 s) pour les requêtes rapides ;
   - **sans délai** pour `ScanLargeFiles`, `DeleteFiles`, `TestTelegram`, qui peuvent
     être longues.
4. Ligne vide → « le moteur a fermé la connexion sans réponse » ; sinon décodage JSON.
5. Toute erreur devient un `String` lisible, renvoyé dans le canal.

### 4.4 Thread graphique

- `update()` appelle d'abord `receive_updates()`, qui vide le canal et met à jour
  `MonitorData` (remplacement complet par `StatusSnapshot`, résultats d'action dans
  `status_message`, erreurs dans `last_error`), puis
  `context.request_repaint_after(250 ms)` pour que l'IHM se redessine même sans
  interaction et voie arriver les réponses.
- Une action utilisateur appelle `self.requests.send(Message…)` :
  - **mise à jour optimiste** de l'état local (ex. la case reste cochée tout de suite),
    corrigée au `StatusSnapshot` suivant si le moteur a refusé ;
  - pour les actions longues, `is_working = true` (indicateur d'attente), remis à
    `false` à la réception de la réponse ou d'une erreur.
- Les commandes qui dépendent de l'état du moteur restent **grisées tant que le premier
  instantané n'est pas arrivé** (`Option<…>` à `None`).
- Logique déclenchée par l'instantané : au passage d'un disque au-dessus du seuil, l'IHM
  lance elle-même un `ScanLargeFiles` limité aux disques concernés (front montant,
  mémorisé dans `was_over_threshold`).

### 4.5 Fermeture

Après `run_native`, `announce_ui_closing` crée un petit runtime et envoie `UiClosing`
de façon synchrone. En cas d'échec, pas grave : le moteur détectera l'absence par
délai (§ 3.5).

---

## 5. Configuration partagée

Un seul fichier `config.toml`, désérialisé dans la même struct `common::Config` par les
deux programmes (`#[serde(default)]` sur la struct : les clés absentes prennent la valeur
par défaut). Chacun lit les clés qui le concernent :

- moteur : `adresse_ecoute_moteur` (`127.0.0.1:7878` local, `0.0.0.0:7878` pour le réseau) ;
- IHM : `adresse_moteur`, `intervalle_actualisation_ihm_secondes`, tailles de fenêtre ;
  l'adresse peut être remplacée par le premier argument (`ui 192.168.1.50:7878`).

`Config::validate()` est partagée et appelée des deux côtés.

---

## 6. Points d'attention connus

- **Pas d'authentification ni de chiffrement.** Écouter sur `0.0.0.0` expose toutes
  les commandes (y compris `DeleteFiles`) à tout le réseau local. À réserver à un
  réseau de confiance, ou à protéger (jeton partagé dans chaque message, TLS, tunnel SSH).
- **Le thread réseau de l'IHM traite une requête à la fois.** Pendant un scan long, les
  `GetStatus` attendent ; ensuite, `tokio::time::interval` rattrape les ticks manqués
  en rafale. Pour l'éviter : `ticker.set_missed_tick_behavior(MissedTickBehavior::Skip)`,
  ou lancer les requêtes longues dans une tâche séparée (`tokio::spawn`).
- **Une connexion TCP par requête** : simple et robuste, mais coûte une poignée de main
  à chaque appel. Sans importance à quelques secondes d'intervalle ; à revoir pour un
  rafraîchissement très rapide.
- **Instantané complet à chaque poll** : simple, mais la taille croît avec les données
  (les événements sont plafonnés à `MAX_NETWORK_EVENTS = 100`). Toujours plafonner les
  listes renvoyées.

---

## 7. Transposer à un autre projet : marche à suivre

1. **Workspace Cargo à trois crates** : `common` (types + config), `engine`, `ui`.
   Dépendances partagées dans `[workspace.dependencies]` (`serde`, `serde_json`,
   `tokio`, `toml`).
2. **Dans `common`** : écrire `MessageClientToEngine` et `MessageEngineToClient`.
   Prévoir dès le départ :
   - `GetStatus` → `StatusSnapshot` (tout l'état affichable) ;
   - `ActionResult { success, message }` comme réponse générique ;
   - `ClientClosing` si le moteur doit savoir quand l'IHM part ;
   - `#[serde(default)]` sur tout champ ajouté plus tard.
   Ajouter un test d'aller-retour JSON.
3. **Dans le moteur** :
   - `Arc<State>` avec `RwLock<Snapshot>` (données) et `watch::Sender<Config>` (réglages) ;
   - tâches de fond qui écrivent le snapshot et s'abonnent à la config (`select!` entre
     `sleep` et `changed()`) ;
   - boucle `accept` + `tokio::spawn(handle_client)` + arrêt propre sur signal ;
   - `handle_client` : une ligne lue (taille et délai bornés), `match`, une ligne écrite ;
   - travail bloquant dans `spawn_blocking` ;
   - valider côté moteur tout ce qui vient de l'IHM (jamais de chemin ou commande
     arbitraire acceptés tels quels).
4. **Dans l'IHM** :
   - canal `tokio::mpsc` UI → réseau, canal `std::mpsc` réseau → UI ;
   - un `std::thread` avec runtime `current_thread`, boucle `select!` entre le ticker
     (`GetStatus`) et les requêtes de l'UI ;
   - `request_engine` : connect avec délai, écrire ligne, lire ligne avec délai (sauf
     requêtes longues), erreurs converties en `String` ;
   - dans la boucle de rendu : `try_recv` en boucle, puis `request_repaint_after` ;
   - commandes grisées tant que le premier instantané n'est pas reçu ; mise à jour
     optimiste puis correction par l'instantané suivant.
5. **Config** : un `config.toml` commun lu dans `common::Config`, avec `validate()`
   partagée ; adresse d'écoute (moteur) et adresse cible (IHM) séparées.
6. **Test manuel** : `echo '"GetStatus"' | nc 127.0.0.1 <port>` doit renvoyer un
   instantané JSON sur une ligne.

> Si l'autre projet n'utilise pas egui : seul le § 4.4 change. Le principe reste le même
> pour n'importe quelle IHM à boucle d'événements (canal vidé sans blocage à chaque
> image/tick, envoi non bloquant des commandes). Pour une IHM web, le même protocole
> ligne-JSON peut être exposé derrière un petit serveur HTTP/WebSocket dans le moteur.
