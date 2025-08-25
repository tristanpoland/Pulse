# Pulse Workflow Engine - Windows Cluster Setup Script
# This PowerShell script sets up a basic working Pulse cluster on Windows

param(
    [int]$ClusterSize = 3,
    [string]$BaseDir = "C:\pulse",
    [string]$LogLevel = "info",
    [switch]$Help
)

# Configuration
$PULSE_VERSION = "0.1.0"
$CLUSTER_NAME = "pulse-cluster"
$WORKER_PORT_START = 8080
$P2P_PORT_START = 9000

# Colors for output
$RED = "Red"
$GREEN = "Green"
$YELLOW = "Yellow"
$BLUE = "Blue"

function Write-Header {
    Write-Host "=============================================" -ForegroundColor Blue
    Write-Host "     Pulse Workflow Engine Setup (Windows)" -ForegroundColor Blue
    Write-Host "=============================================" -ForegroundColor Blue
}

function Write-Step {
    param($Message)
    Write-Host "[STEP] $Message" -ForegroundColor Green
}

function Write-Info {
    param($Message)
    Write-Host "[INFO] $Message" -ForegroundColor Blue
}

function Write-Warning {
    param($Message)
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

function Write-Error {
    param($Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Test-Administrator {
    $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentUser)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Test-Dependencies {
    Write-Step "Checking system dependencies..."
    
    if (-not (Test-Administrator)) {
        Write-Error "This script must be run as Administrator"
        exit 1
    }
    
    # Check for required tools
    $deps = @("docker", "curl", "tar")
    foreach ($dep in $deps) {
        if (-not (Get-Command $dep -ErrorAction SilentlyContinue)) {
            Write-Error "Required dependency not found: $dep"
            exit 1
        }
    }
    
    Write-Info "All dependencies satisfied"
}

function New-ClusterDirectories {
    Write-Step "Creating cluster directories..."
    
    # Create base directory structure
    New-Item -ItemType Directory -Force -Path "$BaseDir" | Out-Null
    New-Item -ItemType Directory -Force -Path "$BaseDir\bin" | Out-Null
    New-Item -ItemType Directory -Force -Path "$BaseDir\config" | Out-Null
    New-Item -ItemType Directory -Force -Path "$BaseDir\data" | Out-Null
    New-Item -ItemType Directory -Force -Path "$BaseDir\logs" | Out-Null
    New-Item -ItemType Directory -Force -Path "$BaseDir\scripts" | Out-Null
    New-Item -ItemType Directory -Force -Path "$BaseDir\artifacts" | Out-Null
    
    # Create node directories
    for ($i = 1; $i -le $ClusterSize; $i++) {
        $nodeDir = "$BaseDir\nodes\node-$i"
        New-Item -ItemType Directory -Force -Path "$nodeDir\data" | Out-Null
        New-Item -ItemType Directory -Force -Path "$nodeDir\logs" | Out-Null
        New-Item -ItemType Directory -Force -Path "$nodeDir\config" | Out-Null
        New-Item -ItemType Directory -Force -Path "$nodeDir\workspace" | Out-Null
        New-Item -ItemType Directory -Force -Path "$nodeDir\artifacts" | Out-Null
    }
    
    Write-Info "Created directory structure at $BaseDir"
}

function Get-PulseBinaries {
    Write-Step "Building Pulse binaries..."
    
    # Get the project root directory
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $projectRoot = Split-Path -Parent $scriptDir
    
    Write-Info "Building Pulse binaries from source..."
    Push-Location $projectRoot
    
    try {
        # Build the workspace
        cargo build --release --workspace
        
        # Copy binaries to cluster directory
        Copy-Item "target\release\pulse-worker.exe" "$BaseDir\bin\" -Force
        Copy-Item "target\release\pulse-cli.exe" "$BaseDir\bin\" -Force
        
        Write-Info "Binaries installed to $BaseDir\bin\"
    }
    finally {
        Pop-Location
    }
}

function New-ClusterConfig {
    Write-Step "Generating cluster configuration..."
    
    # Generate cluster-wide config
    $clusterConfig = @"
[cluster]
name = "$CLUSTER_NAME"
size = $ClusterSize
replication_factor = 2
election_timeout_ms = 5000
heartbeat_interval_ms = 1000

[storage]
data_dir = "$($BaseDir.Replace('\', '/'))/data"
max_storage_size_mb = 10240
sync_interval_seconds = 30

[network]
discovery_port = 8000
gossip_port = 8001
api_port = 8002

[logging]
level = "$LogLevel"
log_dir = "$($BaseDir.Replace('\', '/'))/logs"
max_log_files = 10
max_log_size_mb = 100

[security]
tls_enabled = false
auth_enabled = false
"@
    
    Set-Content -Path "$BaseDir\config\cluster.toml" -Value $clusterConfig
    
    # Generate individual node configs
    for ($i = 1; $i -le $ClusterSize; $i++) {
        $nodeDir = "$BaseDir\nodes\node-$i"
        $workerPort = $WORKER_PORT_START + $i - 1
        $p2pPort = $P2P_PORT_START + $i - 1
        
        $bootstrapNodes = @()
        for ($j = 1; $j -le $ClusterSize; $j++) {
            if ($j -ne $i) {
                $bootstrapPort = $P2P_PORT_START + $j - 1
                $bootstrapNodes += "  `"/ip4/127.0.0.1/tcp/$bootstrapPort`","
            }
        }
        
        $nodeConfig = @"
[node]
id = "node-$i"
name = "pulse-worker-$i"
role = "worker"
worker_port = $workerPort
p2p_port = $p2pPort

[worker]
max_concurrent_tasks = 10
working_directory = "$($nodeDir.Replace('\', '/'))/workspace"
artifacts_directory = "$($nodeDir.Replace('\', '/'))/artifacts"
available_executors = ["shell", "docker", "checkout"]

[storage]
data_dir = "$($nodeDir.Replace('\', '/'))/data"
replication_factor = 2

[network]
listen_address = "0.0.0.0:$workerPort"
p2p_listen_address = "0.0.0.0:$p2pPort"
bootstrap_nodes = [
$($bootstrapNodes -join "`n")
]

[logging]
level = "$LogLevel"
log_file = "$($nodeDir.Replace('\', '/'))/logs/worker.log"
"@
        
        Set-Content -Path "$nodeDir\config\node.toml" -Value $nodeConfig
    }
    
    Write-Info "Generated configuration files"
}

function New-WindowsServices {
    Write-Step "Creating Windows services..."
    
    # Create service for each worker node
    for ($i = 1; $i -le $ClusterSize; $i++) {
        $serviceName = "PulseWorker$i"
        $nodeDir = "$BaseDir\nodes\node-$i"
        $exePath = "$BaseDir\bin\pulse-worker.exe"
        $configPath = "$nodeDir\config\node.toml"
        
        # Remove existing service if it exists
        if (Get-Service -Name $serviceName -ErrorAction SilentlyContinue) {
            Stop-Service -Name $serviceName -Force -ErrorAction SilentlyContinue
            sc.exe delete $serviceName | Out-Null
        }
        
        # Create new service
        sc.exe create $serviceName binpath= "`"$exePath`" --config `"$configPath`"" start= demand | Out-Null
        sc.exe description $serviceName "Pulse Worker Node $i" | Out-Null
        
        Write-Info "Created service: $serviceName"
    }
}

function New-ClusterManagerScript {
    Write-Step "Creating cluster management script..."
    
    $managerScript = @"
# Pulse Cluster Manager for Windows
param([string]`$Action, [int]`$Node = 1)

switch (`$Action) {
    "start" {
        Write-Host "Starting Pulse cluster..." -ForegroundColor Green
        for (`$i = 1; `$i -le $ClusterSize; `$i++) {
            Start-Service -Name "PulseWorker`$i"
            Start-Sleep -Seconds 2
        }
        Write-Host "Cluster started" -ForegroundColor Green
    }
    "stop" {
        Write-Host "Stopping Pulse cluster..." -ForegroundColor Yellow
        for (`$i = 1; `$i -le $ClusterSize; `$i++) {
            Stop-Service -Name "PulseWorker`$i" -Force
        }
        Write-Host "Cluster stopped" -ForegroundColor Yellow
    }
    "restart" {
        & `$MyInvocation.MyCommand.Path -Action stop
        Start-Sleep -Seconds 5
        & `$MyInvocation.MyCommand.Path -Action start
    }
    "status" {
        Write-Host "Pulse cluster status:" -ForegroundColor Blue
        for (`$i = 1; `$i -le $ClusterSize; `$i++) {
            `$status = (Get-Service -Name "PulseWorker`$i").Status
            Write-Host "Node `$i: `$status"
        }
    }
    "logs" {
        `$logFile = "$BaseDir\nodes\node-`$Node\logs\worker.log"
        if (Test-Path `$logFile) {
            Get-Content `$logFile -Tail 50 -Wait
        } else {
            Write-Host "Log file not found: `$logFile" -ForegroundColor Red
        }
    }
    "health" {
        Write-Host "Checking cluster health..." -ForegroundColor Blue
        `$healthy = 0
        for (`$i = 1; `$i -le $ClusterSize; `$i++) {
            `$port = $WORKER_PORT_START + `$i - 1
            try {
                `$response = Invoke-RestMethod -Uri "http://localhost:`$port/health" -TimeoutSec 5
                Write-Host "Node `$i: healthy" -ForegroundColor Green
                `$healthy++
            } catch {
                Write-Host "Node `$i: unhealthy" -ForegroundColor Red
            }
        }
        Write-Host "Healthy nodes: `$healthy/$ClusterSize"
    }
    default {
        Write-Host "Usage: cluster-manager.ps1 -Action {start|stop|restart|status|logs|health} [-Node N]"
    }
}
"@
    
    Set-Content -Path "$BaseDir\scripts\cluster-manager.ps1" -Value $managerScript
    Write-Info "Created cluster management script"
}

function Set-Firewall {
    Write-Step "Configuring Windows Firewall rules..."
    
    try {
        # Allow cluster ports
        for ($i = 1; $i -le $ClusterSize; $i++) {
            $workerPort = $WORKER_PORT_START + $i - 1
            $p2pPort = $P2P_PORT_START + $i - 1
            
            New-NetFirewallRule -DisplayName "Pulse Worker $i API" -Direction Inbound -Port $workerPort -Protocol TCP -Action Allow -ErrorAction SilentlyContinue | Out-Null
            New-NetFirewallRule -DisplayName "Pulse Worker $i P2P" -Direction Inbound -Port $p2pPort -Protocol TCP -Action Allow -ErrorAction SilentlyContinue | Out-Null
        }
        
        # Allow management ports
        New-NetFirewallRule -DisplayName "Pulse Discovery" -Direction Inbound -Port 8000 -Protocol TCP -Action Allow -ErrorAction SilentlyContinue | Out-Null
        New-NetFirewallRule -DisplayName "Pulse Gossip" -Direction Inbound -Port 8001 -Protocol TCP -Action Allow -ErrorAction SilentlyContinue | Out-Null
        New-NetFirewallRule -DisplayName "Pulse API" -Direction Inbound -Port 8002 -Protocol TCP -Action Allow -ErrorAction SilentlyContinue | Out-Null
        
        Write-Info "Configured Windows Firewall rules"
    }
    catch {
        Write-Warning "Failed to configure firewall rules - manual configuration may be needed"
    }
}

function New-CliConfig {
    Write-Step "Creating CLI configuration..."
    
    $cliConfig = @"
[cluster]
endpoint = "http://localhost:8002"
timeout_seconds = 30

[output]
default_format = "table"
colors = true

[auth]
# For production use
# token = ""
# cert_file = ""
# key_file = ""
"@
    
    Set-Content -Path "$BaseDir\config\pulse-cli.toml" -Value $cliConfig
    
    # Add to PATH if not already there
    $envPath = [Environment]::GetEnvironmentVariable("PATH", "Machine")
    if ($envPath -notlike "*$BaseDir\bin*") {
        [Environment]::SetEnvironmentVariable("PATH", "$envPath;$BaseDir\bin", "Machine")
        Write-Info "Added Pulse CLI to system PATH"
    }
    
    Write-Info "CLI configuration created"
}

function New-MonitoringStack {
    Write-Step "Setting up monitoring with Docker Compose..."
    
    $dockerCompose = @"
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    container_name: pulse-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
    restart: unless-stopped

  grafana:
    image: grafana/grafana:latest
    container_name: pulse-grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=pulse123
    volumes:
      - grafana_data:/var/lib/grafana
      - ./monitoring/dashboards:/etc/grafana/provisioning/dashboards
      - ./monitoring/datasources:/etc/grafana/provisioning/datasources
    restart: unless-stopped

volumes:
  prometheus_data:
  grafana_data:
"@
    
    Set-Content -Path "$BaseDir\docker-compose.yml" -Value $dockerCompose
    
    # Create monitoring config
    New-Item -ItemType Directory -Force -Path "$BaseDir\monitoring\dashboards" | Out-Null
    New-Item -ItemType Directory -Force -Path "$BaseDir\monitoring\datasources" | Out-Null
    
    $prometheusConfig = @"
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'pulse-workers'
    static_configs:
"@
    
    for ($i = 1; $i -le $ClusterSize; $i++) {
        $workerPort = $WORKER_PORT_START + $i - 1
        $prometheusConfig += "`n      - targets: ['localhost:$workerPort']"
    }
    
    Set-Content -Path "$BaseDir\monitoring\prometheus.yml" -Value $prometheusConfig
    Write-Info "Monitoring stack configured"
}

function Test-Cluster {
    Write-Step "Running cluster validation tests..."
    
    Write-Info "Starting cluster services..."
    & "$BaseDir\scripts\cluster-manager.ps1" -Action start
    
    Write-Info "Waiting for cluster to initialize..."
    Start-Sleep -Seconds 15
    
    # Test CLI connectivity
    try {
        & "$BaseDir\bin\pulse-cli.exe" --config "$BaseDir\config\pulse-cli.toml" cluster | Out-Null
        Write-Info "✓ CLI connectivity test passed"
    }
    catch {
        Write-Warning "✗ CLI connectivity test failed"
    }
    
    # Test job submission
    $testJob = @"
name: Test Job
version: "1.0"

jobs:
  test:
    name: Simple Test
    runs-on: ["any"]
    steps:
      - name: Echo test
        run: echo "Hello from Pulse cluster!"
"@
    
    Set-Content -Path "$env:TEMP\test-job.yml" -Value $testJob
    
    try {
        & "$BaseDir\bin\pulse-cli.exe" --config "$BaseDir\config\pulse-cli.toml" submit "$env:TEMP\test-job.yml" | Out-Null
        Write-Info "✓ Job submission test passed"
    }
    catch {
        Write-Warning "✗ Job submission test failed"
    }
    
    Remove-Item "$env:TEMP\test-job.yml" -Force -ErrorAction SilentlyContinue
}

function Show-ClusterInfo {
    Write-Step "Cluster setup completed successfully!"
    
    Write-Host ""
    Write-Host "Pulse Cluster Information:" -ForegroundColor Green
    Write-Host "=========================="
    Write-Host "Cluster Name: $CLUSTER_NAME"
    Write-Host "Cluster Size: $ClusterSize nodes"
    Write-Host "Base Directory: $BaseDir"
    Write-Host ""
    Write-Host "Node Endpoints:"
    for ($i = 1; $i -le $ClusterSize; $i++) {
        $workerPort = $WORKER_PORT_START + $i - 1
        Write-Host "  Node $i: http://localhost:$workerPort"
    }
    Write-Host ""
    Write-Host "Management Commands:" -ForegroundColor Blue
    Write-Host "  Start cluster:  $BaseDir\scripts\cluster-manager.ps1 -Action start"
    Write-Host "  Stop cluster:   $BaseDir\scripts\cluster-manager.ps1 -Action stop"
    Write-Host "  Cluster status: $BaseDir\scripts\cluster-manager.ps1 -Action status"
    Write-Host "  View logs:      $BaseDir\scripts\cluster-manager.ps1 -Action logs -Node [N]"
    Write-Host "  Health check:   $BaseDir\scripts\cluster-manager.ps1 -Action health"
    Write-Host ""
    Write-Host "CLI Usage:" -ForegroundColor Blue
    Write-Host "  pulse-cli --help"
    Write-Host "  pulse-cli cluster"
    Write-Host "  pulse-cli submit workflow.yml"
    Write-Host "  pulse-cli list"
    Write-Host ""
    Write-Host "Monitoring:" -ForegroundColor Blue
    Write-Host "  Prometheus: http://localhost:9090"
    Write-Host "  Grafana:    http://localhost:3000 (admin/pulse123)"
    Write-Host ""
    Write-Host "To start monitoring: cd $BaseDir && docker-compose up -d" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "🎉 Pulse cluster setup complete!" -ForegroundColor Green
    Write-Host "Ready to execute distributed workflows." -ForegroundColor Blue
}

# Main execution
if ($Help) {
    Write-Host "Pulse Cluster Setup for Windows"
    Write-Host ""
    Write-Host "Usage: setup-cluster.ps1 [OPTIONS]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -ClusterSize N    Set cluster size (default: 3)"
    Write-Host "  -BaseDir PATH     Set installation directory (default: C:\pulse)"
    Write-Host "  -LogLevel LEVEL   Set log level (default: info)"
    Write-Host "  -Help             Show this help"
    exit 0
}

Write-Header
Test-Dependencies
New-ClusterDirectories
Get-PulseBinaries
New-ClusterConfig
New-WindowsServices
New-ClusterManagerScript
Set-Firewall
New-CliConfig
New-MonitoringStack
Test-Cluster
Show-ClusterInfo