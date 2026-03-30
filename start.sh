#!/bin/bash

set -e

echo "🚀 Démarrage du projet RERB"
echo "=========================================================="

# Couleurs pour les messages
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Initialiser Airflow
echo "🔧 Initialisation d'Airflow..."
docker compose up airflow-init

# Démarrer tous les services
echo "🚀 Démarrage de tous les services..."
docker compose up -d

# Attendre que les services soient prêts
echo "⏳ Attente du démarrage des services..."
sleep 15

# Vérifier l'état des services
echo ""
echo "📊 État des services:"
docker compose ps

echo ""
echo -e "${GREEN}✅ Tous les services sont démarrés !${NC}"