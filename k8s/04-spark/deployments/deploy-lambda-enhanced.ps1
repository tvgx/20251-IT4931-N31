# =============================================================================
# DEPLOY LAMBDA ARCHITECTURE - Enhanced Version
# =============================================================================
# PowerShell script để deploy toàn bộ Lambda Architecture lên Kubernetes
# =============================================================================

param(
    [switch]$All,
    [switch]$Infrastructure,
    [switch]$Ingestion,
    [switch]$BatchLayer,
    [switch]$SpeedLayer,
    [switch]$ServingLayer,
    [switch]$Visualization,
    [switch]$Delete
)

$ErrorActionPreference = "Stop"
$NAMESPACE = "bigdata"

Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host "🚀 LAMBDA ARCHITECTURE DEPLOYMENT" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Cyan

# Check minikube status
Write-Host "`n📋 Checking Minikube status..." -ForegroundColor Yellow
$minikubeStatus = minikube status --format='{{.Host}}'
if ($minikubeStatus -ne "Running") {
    Write-Host "❌ Minikube is not running. Please start it first." -ForegroundColor Red
    exit 1
}
Write-Host "✅ Minikube is running" -ForegroundColor Green

# Create namespace if not exists
Write-Host "`n📦 Ensuring namespace '$NAMESPACE' exists..." -ForegroundColor Yellow
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

function Publish-LambdaComponent {
    param(
        [string]$Name,
        [string]$Path,
        [switch]$IsDelete
    )
    
    Write-Host "`n$Name" -ForegroundColor Cyan
    if ($IsDelete) {
        kubectl delete -f $Path --ignore-not-found
        Write-Host "   ✅ Deleted" -ForegroundColor Green
    } else {
        kubectl apply -f $Path
        Write-Host "   ✅ Applied" -ForegroundColor Green
    }
}

# =============================================================================
# INFRASTRUCTURE (Kafka, MongoDB, MinIO, Elasticsearch)
# =============================================================================
if ($All -or $Infrastructure) {
    Write-Host "`n" + "=" * 50 -ForegroundColor Yellow
    Write-Host "🔧 INFRASTRUCTURE LAYER" -ForegroundColor Yellow
    Write-Host "=" * 50 -ForegroundColor Yellow
    
    if ($Delete) {
        Publish-LambdaComponent "Metabase" "k8s/06-metabase/metabase.yaml" -IsDelete
        Publish-LambdaComponent "MinIO" "k8s/05-minio/minio-standalone.yaml" -IsDelete
        Publish-LambdaComponent "Elasticsearch" "k8s/03-elasticsearch/*.yaml" -IsDelete
        Publish-LambdaComponent "MongoDB" "k8s/02-mongo/*.yaml" -IsDelete
        Publish-LambdaComponent "Kafka" "k8s/01-kafka/*.yaml" -IsDelete
        Publish-LambdaComponent "Namespace" "k8s/00-namespace.yaml" -IsDelete
    } else {
        Publish-LambdaComponent "Namespace" "k8s/00-namespace.yaml"
        Publish-LambdaComponent "Kafka" "k8s/01-kafka/kafka-cluster.yaml"
        Publish-LambdaComponent "MongoDB Secret" "k8s/02-mongo/secret.yaml"
        Publish-LambdaComponent "MongoDB Cluster" "k8s/02-mongo/psmdb-cluster.yaml"
        Publish-LambdaComponent "MinIO" "k8s/05-minio/minio-standalone.yaml"
        
        # Wait for infrastructure
        Write-Host "`n⏳ Waiting for infrastructure to be ready..." -ForegroundColor Yellow
        Start-Sleep -Seconds 30
    }
}

# =============================================================================
# DATA INGESTION LAYER
# =============================================================================
if ($All -or $Ingestion) {
    Write-Host "`n" + "=" * 50 -ForegroundColor Yellow
    Write-Host "📥 DATA INGESTION LAYER" -ForegroundColor Yellow
    Write-Host "=" * 50 -ForegroundColor Yellow
    
    if ($Delete) {
        kubectl delete -f k8s/04-spark/deployments/data-ingestion-producer.yaml --ignore-not-found
    } else {
        kubectl apply -f k8s/04-spark/deployments/data-ingestion-producer.yaml
    }
}

# =============================================================================
# BATCH LAYER
# =============================================================================
if ($All -or $BatchLayer) {
    Write-Host "`n" + "=" * 50 -ForegroundColor Yellow
    Write-Host "📦 BATCH LAYER" -ForegroundColor Yellow
    Write-Host "=" * 50 -ForegroundColor Yellow
    
    if ($Delete) {
        kubectl delete -f k8s/04-spark/deployments/enhanced-batch-layer.yaml --ignore-not-found
    } else {
        # Run as Job (one-time batch processing)
        kubectl apply -f k8s/04-spark/deployments/enhanced-batch-layer.yaml
        Write-Host "   💡 To run batch job manually:" -ForegroundColor Cyan
        Write-Host "      kubectl create job --from=cronjob/enhanced-batch-cronjob batch-manual -n bigdata" -ForegroundColor Gray
    }
}

# =============================================================================
# SPEED LAYER
# =============================================================================
if ($All -or $SpeedLayer) {
    Write-Host "`n" + "=" * 50 -ForegroundColor Yellow
    Write-Host "⚡ SPEED LAYER" -ForegroundColor Yellow
    Write-Host "=" * 50 -ForegroundColor Yellow
    
    if ($Delete) {
        kubectl delete -f k8s/04-spark/deployments/enhanced-speed-layer.yaml --ignore-not-found
    } else {
        kubectl apply -f k8s/04-spark/deployments/enhanced-speed-layer.yaml
    }
}

# =============================================================================
# SERVING LAYER
# =============================================================================
if ($All -or $ServingLayer) {
    Write-Host "`n" + "=" * 50 -ForegroundColor Yellow
    Write-Host "🎯 SERVING LAYER" -ForegroundColor Yellow
    Write-Host "=" * 50 -ForegroundColor Yellow
    
    if ($Delete) {
        kubectl delete -f k8s/04-spark/deployments/enhanced-serving-layer.yaml --ignore-not-found
    } else {
        kubectl apply -f k8s/04-spark/deployments/enhanced-serving-layer.yaml
    }
}

# =============================================================================
# VISUALIZATION LAYER (Metabase)
# =============================================================================
if ($All -or $Visualization) {
    Write-Host "`n" + "=" * 50 -ForegroundColor Yellow
    Write-Host "📊 VISUALIZATION LAYER (Metabase)" -ForegroundColor Yellow
    Write-Host "=" * 50 -ForegroundColor Yellow
    
    if ($Delete) {
        kubectl delete -f k8s/06-metabase/metabase.yaml --ignore-not-found
    } else {
        kubectl apply -f k8s/06-metabase/metabase.yaml
    }
}

# =============================================================================
# STATUS
# =============================================================================
if (-not $Delete) {
    Write-Host "`n" + "=" * 70 -ForegroundColor Cyan
    Write-Host "📋 DEPLOYMENT STATUS" -ForegroundColor Cyan
    Write-Host "=" * 70 -ForegroundColor Cyan
    
    Write-Host "`n🔍 Pods:" -ForegroundColor Yellow
    kubectl get pods -n $NAMESPACE -o wide
    
    Write-Host "`n🔍 Services:" -ForegroundColor Yellow
    kubectl get svc -n $NAMESPACE
    
    Write-Host "`n🔍 Jobs:" -ForegroundColor Yellow
    kubectl get jobs -n $NAMESPACE
    
    Write-Host "`n" + "=" * 70 -ForegroundColor Cyan
    Write-Host "📌 PORT FORWARDING COMMANDS" -ForegroundColor Cyan
    Write-Host "=" * 70 -ForegroundColor Cyan
    Write-Host "
# MongoDB (Serving Layer)
kubectl port-forward svc/mycluster-mongos 27018:27017 -n bigdata

# MinIO Console (Storage Layer)
kubectl port-forward svc/minio 9001:9001 -n bigdata

# Serving Layer API
kubectl port-forward svc/serving-layer 5000:5000 -n bigdata

# Metabase Dashboard
kubectl port-forward svc/metabase 3000:3000 -n bigdata

# Kafka (if needed)
kubectl port-forward svc/kafka-cluster-kafka-bootstrap 9092:9092 -n bigdata
" -ForegroundColor Gray

    Write-Host "`n" + "=" * 70 -ForegroundColor Green
    Write-Host "✅ DEPLOYMENT COMPLETE!" -ForegroundColor Green
    Write-Host "=" * 70 -ForegroundColor Green
}

# =============================================================================
# USAGE EXAMPLES
# =============================================================================
Write-Host "
📚 USAGE:
  .\deploy-lambda-enhanced.ps1 -All              # Deploy everything
  .\deploy-lambda-enhanced.ps1 -Infrastructure   # Deploy infrastructure only
  .\deploy-lambda-enhanced.ps1 -Ingestion        # Deploy data ingestion
  .\deploy-lambda-enhanced.ps1 -BatchLayer       # Deploy batch layer
  .\deploy-lambda-enhanced.ps1 -SpeedLayer       # Deploy speed layer
  .\deploy-lambda-enhanced.ps1 -ServingLayer     # Deploy serving layer
  .\deploy-lambda-enhanced.ps1 -Visualization    # Deploy Metabase
  .\deploy-lambda-enhanced.ps1 -All -Delete      # Delete everything
" -ForegroundColor Gray
