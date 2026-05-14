# ADR-003 : Authentification API — clé statique (X-API-Key) vs OAuth2/JWT

## Statut
✅ Accepté

## Contexte

L'API FastAPI expose trois routes de données (`/regularite`, `/allongement`, `/ecarts`) consommées exclusivement par le dashboard Streamlit, lui-même dans le même réseau Docker. L'API n'est pas destinée à être consommée par des utilisateurs humains ou des clients tiers dans sa version actuelle.

Exigences de sécurité identifiées :
- Empêcher l'accès non authentifié aux données si l'API est exposée sur une VM publique
- Ne pas complexifier inutilement la configuration pour un client unique (Streamlit)
- Garder le mécanisme auditable et simple à faire tourner en CI (tests unitaires)

## Décision

Authentification par **clé API statique** transmise dans le header HTTP `X-API-Key`. La clé est stockée dans la variable d'environnement `RERB_API_KEY`, injectée dans les deux conteneurs (`api` et `streamlit`) via `docker-compose.yml`.

La route `/health` et `/metrics` (Prometheus) restent publiques intentionnellement.

## Alternatives considérées

| Alternative | Raison du rejet |
| ----------- | --------------- |
| **OAuth2 / JWT** | Requiert un serveur d'autorisation (Keycloak, Auth0) ou une implémentation custom de génération/validation de tokens. Complexité non justifiée pour un client unique sur réseau interne |
| **Pas d'authentification** | Inacceptable dès que l'API est exposée sur une IP publique |
| **Basic Auth (login/mot de passe)** | Moins idiomatique pour une API REST, et ne s'intègre pas naturellement avec FastAPI `Security` |

## Conséquences

**Avantages :**
- Implémentation en ~15 lignes via `fastapi.security.APIKeyHeader` et `Depends`
- Testable en CI sans infrastructure externe (`patch.object(main_module, "_API_KEY", "test-key")`)
- Rotation de clé simple : modifier `RERB_API_KEY` dans `.env` et redémarrer les deux conteneurs

**Inconvénients et risques assumés :**
- La clé est un secret long-terme : si elle est compromise, il faut la changer manuellement
- Pas de granularité des permissions (une clé = accès total) — acceptable pour un client unique
- Pas de mécanisme d'expiration — un token JWT serait préférable si l'API devait être consommée par plusieurs clients externes avec des droits différenciés
