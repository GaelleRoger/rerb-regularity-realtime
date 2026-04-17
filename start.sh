#!/bin/bash

set -e

echo "🚀 Démarrage du projet RERB"
echo "=========================================================="

# Couleurs pour les messages
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Démarrer tous les services
echo "🚀 Démarrage de tous les services..."
docker compose up -d

# Attendre que les services soient prêts
echo "⏳ Attente du démarrage des services..."
sleep 10

# Vérifier l'état des services
echo ""
echo "📊 État des services:"
docker compose ps

echo ""
echo -e "${GREEN}✅ Tous les services sont démarrés !${NC}"
echo ""
echo "📡 API        → http://localhost:8000"
echo "📊 Dashboard  → http://localhost:8501"
echo "🗄️  PgAdmin    → http://localhost:8082"
echo "📈 Grafana    → http://localhost:3000"
echo "🔬 Prometheus → http://localhost:9090"
echo ""
echo "📋 Logs pipeline : logs/pipeline_$(date '+%Y-%m-%d').log"
echo "📋 Logs reset    : logs/reset_$(date '+%Y-%m-%d').log"
