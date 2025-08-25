#!/bin/bash

# Pulse Workflow Engine - Cluster Setup Script
# This script sets up a basic working Pulse cluster with worker nodes and storage replication

set -e

# Configuration
PULSE_VERSION="0.1.0"
CLUSTER_NAME="pulse-cluster"
CLUSTER_SIZE=3
WORKER_PORT_START=8080
P2P_PORT_START=9000
BASE_DIR="/opt/pulse"
LOG_LEVEL="info"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}"
    echo "============================================="
    echo "     Pulse Workflow Engine Setup"
    echo "============================================="
    echo -e "${NC}"
}

print_step() {
    echo -e "${GREEN}[STEP]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_dependencies() {
    print_step "Checking system dependencies..."
    
    # Check if running as root or with sudo
    if [[ $EUID -ne 0 ]]; then
        print_error "This script must be run as root or with sudo"
        exit 1
    fi
    
    # Check required commands
    local deps=("docker" "docker-compose" "curl" "jq" "tar" "systemctl")
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            print_error "Required dependency not found: $dep"
            exit 1
        fi
    done
    
    print_info "All dependencies satisfied"
}

create_directories() {
    print_step "Creating cluster directories..."
    
    # Create base directory structure
    mkdir -p "$BASE_DIR"/{bin,config,data,logs,scripts,artifacts}
    mkdir -p "$BASE_DIR"/nodes/{node-{1..$CLUSTER_SIZE}}/{data,logs,config}
    
    print_info "Created directory structure at $BASE_DIR"
}

download_binaries() {
    print_step "Downloading Pulse binaries..."
    
    # In production, these would be downloaded from releases
    # For now, we'll build them locally
    local project_root
    project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    
    print_info "Building Pulse binaries from source..."
    cd "$project_root"
    
    # Build the workspace
    cargo build --release --workspace
    
    # Copy binaries to cluster directory
    cp target/release/pulse-worker "$BASE_DIR/bin/"
    cp target/release/pulse-cli "$BASE_DIR/bin/"
    
    # Make binaries executable
    chmod +x "$BASE_DIR/bin"/*
    
    print_info "Binaries installed to $BASE_DIR/bin/"
}

generate_cluster_config() {
    print_step "Generating cluster configuration..."
    
    # Generate cluster-wide config
    cat > "$BASE_DIR/config/cluster.toml" << EOF
[cluster]
name = "$CLUSTER_NAME"
size = $CLUSTER_SIZE
replication_factor = 2
election_timeout_ms = 5000
heartbeat_interval_ms = 1000

[storage]
data_dir = "$BASE_DIR/data"
max_storage_size_mb = 10240
sync_interval_seconds = 30

[network]
discovery_port = 8000
gossip_port = 8001
api_port = 8002

[logging]
level = "$LOG_LEVEL"
log_dir = "$BASE_DIR/logs"
max_log_files = 10
max_log_size_mb = 100

[security]
tls_enabled = false
auth_enabled = false
# For production, enable TLS and authentication
EOF

    # Generate individual node configs
    for i in $(seq 1 $CLUSTER_SIZE); do
        local node_dir="$BASE_DIR/nodes/node-$i"
        local worker_port=$((WORKER_PORT_START + i - 1))
        local p2p_port=$((P2P_PORT_START + i - 1))
        
        cat > "$node_dir/config/node.toml" << EOF
[node]
id = "node-$i"
name = "pulse-worker-$i"
role = "worker"
worker_port = $worker_port
p2p_port = $p2p_port

[worker]
max_concurrent_tasks = 10
working_directory = "$node_dir/workspace"
artifacts_directory = "$node_dir/artifacts"
available_executors = ["shell", "docker", "checkout"]

[storage]
data_dir = "$node_dir/data"
replication_factor = 2

[network]
listen_address = "0.0.0.0:$worker_port"
p2p_listen_address = "0.0.0.0:$p2p_port"
bootstrap_nodes = [
EOF

        # Add bootstrap nodes (other nodes in cluster)
        for j in $(seq 1 $CLUSTER_SIZE); do
            if [ "$j" -ne "$i" ]; then
                local bootstrap_port=$((P2P_PORT_START + j - 1))
                echo "  \"/ip4/127.0.0.1/tcp/$bootstrap_port\"," >> "$node_dir/config/node.toml"
            fi
        done
        
        cat >> "$node_dir/config/node.toml" << EOF
]

[logging]
level = "$LOG_LEVEL"
log_file = "$node_dir/logs/worker.log"
EOF

        # Create workspace directory
        mkdir -p "$node_dir"/{workspace,artifacts}
    done
    
    print_info "Generated configuration files"
}

create_systemd_services() {
    print_step "Creating systemd services..."
    
    # Create service for each worker node
    for i in $(seq 1 $CLUSTER_SIZE); do
        local service_name="pulse-worker-$i"
        local node_dir="$BASE_DIR/nodes/node-$i"
        
        cat > "/etc/systemd/system/$service_name.service" << EOF
[Unit]
Description=Pulse Worker Node $i
After=network.target
Wants=network.target

[Service]
Type=simple
User=pulse
Group=pulse
ExecStart=$BASE_DIR/bin/pulse-worker --config $node_dir/config/node.toml
WorkingDirectory=$node_dir
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment=RUST_LOG=$LOG_LEVEL

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
EOF
    done
    
    # Create cluster management service
    cat > "/etc/systemd/system/pulse-cluster.service" << EOF
[Unit]
Description=Pulse Cluster Manager
After=network.target
Wants=network.target

[Service]
Type=oneshot
User=root
ExecStart=$BASE_DIR/scripts/cluster-manager.sh start
ExecStop=$BASE_DIR/scripts/cluster-manager.sh stop
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
    
    systemctl daemon-reload
    print_info "Created systemd services"
}

create_cluster_manager_script() {
    print_step "Creating cluster management script..."
    
    cat > "$BASE_DIR/scripts/cluster-manager.sh" << 'EOF'
#!/bin/bash

CLUSTER_SIZE=3
BASE_DIR="/opt/pulse"

case "$1" in
    start)
        echo "Starting Pulse cluster..."
        for i in $(seq 1 $CLUSTER_SIZE); do
            systemctl start pulse-worker-$i
            sleep 2  # Stagger startup
        done
        echo "Cluster started"
        ;;
    stop)
        echo "Stopping Pulse cluster..."
        for i in $(seq 1 $CLUSTER_SIZE); do
            systemctl stop pulse-worker-$i
        done
        echo "Cluster stopped"
        ;;
    restart)
        $0 stop
        sleep 5
        $0 start
        ;;
    status)
        echo "Pulse cluster status:"
        for i in $(seq 1 $CLUSTER_SIZE); do
            echo -n "Node $i: "
            systemctl is-active pulse-worker-$i
        done
        ;;
    logs)
        node=${2:-1}
        journalctl -u pulse-worker-$node -f
        ;;
    health)
        echo "Checking cluster health..."
        healthy=0
        for i in $(seq 1 $CLUSTER_SIZE); do
            port=$((8079 + i))
            if curl -s "http://localhost:$port/health" > /dev/null; then
                echo "Node $i: healthy"
                ((healthy++))
            else
                echo "Node $i: unhealthy"
            fi
        done
        echo "Healthy nodes: $healthy/$CLUSTER_SIZE"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs [node]|health}"
        exit 1
        ;;
esac
EOF
    
    chmod +x "$BASE_DIR/scripts/cluster-manager.sh"
    print_info "Created cluster management script"
}

create_user_and_permissions() {
    print_step "Setting up pulse user and permissions..."
    
    # Create pulse user if it doesn't exist
    if ! id "pulse" &>/dev/null; then
        useradd -r -s /bin/false -d "$BASE_DIR" pulse
        print_info "Created pulse user"
    fi
    
    # Set ownership and permissions
    chown -R pulse:pulse "$BASE_DIR"
    chmod -R 755 "$BASE_DIR"
    chmod 600 "$BASE_DIR"/config/*.toml
    chmod 600 "$BASE_DIR"/nodes/*/config/*.toml
    
    print_info "Set permissions for pulse user"
}

setup_firewall() {
    print_step "Configuring firewall rules..."
    
    # Check if ufw is available
    if command -v ufw &> /dev/null; then
        # Allow cluster ports
        for i in $(seq 1 $CLUSTER_SIZE); do
            local worker_port=$((WORKER_PORT_START + i - 1))
            local p2p_port=$((P2P_PORT_START + i - 1))
            
            ufw allow "$worker_port/tcp" comment "Pulse Worker $i API"
            ufw allow "$p2p_port/tcp" comment "Pulse Worker $i P2P"
        done
        
        # Allow discovery and management ports
        ufw allow 8000/tcp comment "Pulse Discovery"
        ufw allow 8001/tcp comment "Pulse Gossip"
        ufw allow 8002/tcp comment "Pulse API"
        
        print_info "Configured UFW firewall rules"
    else
        print_warning "UFW not found - manual firewall configuration may be needed"
    fi
}

create_cli_config() {
    print_step "Creating CLI configuration..."
    
    cat > "$BASE_DIR/config/pulse-cli.toml" << EOF
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
EOF

    # Create symlink for global CLI access
    ln -sf "$BASE_DIR/bin/pulse-cli" /usr/local/bin/pulse
    
    print_info "CLI configuration created and linked to /usr/local/bin/pulse"
}

install_docker_compose_monitoring() {
    print_step "Setting up monitoring with Docker Compose..."
    
    cat > "$BASE_DIR/docker-compose.yml" << EOF
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
EOF

    # Create monitoring config
    mkdir -p "$BASE_DIR/monitoring"/{dashboards,datasources}
    
    cat > "$BASE_DIR/monitoring/prometheus.yml" << EOF
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'pulse-workers'
    static_configs:
EOF

    for i in $(seq 1 $CLUSTER_SIZE); do
        local worker_port=$((WORKER_PORT_START + i - 1))
        echo "      - targets: ['localhost:$worker_port']" >> "$BASE_DIR/monitoring/prometheus.yml"
    done
    
    print_info "Monitoring stack configured"
}

run_cluster_tests() {
    print_step "Running cluster validation tests..."
    
    # Wait for cluster to be ready
    print_info "Waiting for cluster to initialize..."
    sleep 15
    
    # Test CLI connectivity
    if "$BASE_DIR/bin/pulse-cli" --config "$BASE_DIR/config/pulse-cli.toml" cluster > /dev/null; then
        print_info "✓ CLI connectivity test passed"
    else
        print_warning "✗ CLI connectivity test failed"
    fi
    
    # Test job submission
    cat > /tmp/test-job.yml << EOF
name: Test Job
version: "1.0"

jobs:
  test:
    name: Simple Test
    runs-on: ["any"]
    steps:
      - name: Echo test
        run: echo "Hello from Pulse cluster!"
EOF

    if "$BASE_DIR/bin/pulse-cli" --config "$BASE_DIR/config/pulse-cli.toml" submit /tmp/test-job.yml > /dev/null; then
        print_info "✓ Job submission test passed"
    else
        print_warning "✗ Job submission test failed"
    fi
    
    rm -f /tmp/test-job.yml
}

print_cluster_info() {
    print_step "Cluster setup completed successfully!"
    
    echo ""
    echo -e "${GREEN}Pulse Cluster Information:${NC}"
    echo "=========================="
    echo "Cluster Name: $CLUSTER_NAME"
    echo "Cluster Size: $CLUSTER_SIZE nodes"
    echo "Base Directory: $BASE_DIR"
    echo ""
    echo "Node Endpoints:"
    for i in $(seq 1 $CLUSTER_SIZE); do
        local worker_port=$((WORKER_PORT_START + i - 1))
        echo "  Node $i: http://localhost:$worker_port"
    done
    echo ""
    echo "Management Commands:"
    echo "  Start cluster:  sudo $BASE_DIR/scripts/cluster-manager.sh start"
    echo "  Stop cluster:   sudo $BASE_DIR/scripts/cluster-manager.sh stop"
    echo "  Cluster status: sudo $BASE_DIR/scripts/cluster-manager.sh status"
    echo "  View logs:      sudo $BASE_DIR/scripts/cluster-manager.sh logs [node]"
    echo "  Health check:   sudo $BASE_DIR/scripts/cluster-manager.sh health"
    echo ""
    echo "CLI Usage:"
    echo "  pulse --help"
    echo "  pulse cluster"
    echo "  pulse submit workflow.yml"
    echo "  pulse list"
    echo ""
    echo "Monitoring:"
    echo "  Prometheus: http://localhost:9090"
    echo "  Grafana:    http://localhost:3000 (admin/pulse123)"
    echo ""
    echo -e "${YELLOW}To start monitoring:${NC} cd $BASE_DIR && docker-compose up -d"
}

# Main execution
main() {
    print_header
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --cluster-size)
                CLUSTER_SIZE="$2"
                shift 2
                ;;
            --base-dir)
                BASE_DIR="$2"
                shift 2
                ;;
            --log-level)
                LOG_LEVEL="$2"
                shift 2
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo "Options:"
                echo "  --cluster-size N    Set cluster size (default: 3)"
                echo "  --base-dir PATH     Set installation directory (default: /opt/pulse)"
                echo "  --log-level LEVEL   Set log level (default: info)"
                echo "  --help              Show this help"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    check_dependencies
    create_directories
    download_binaries
    generate_cluster_config
    create_systemd_services
    create_cluster_manager_script
    create_user_and_permissions
    setup_firewall
    create_cli_config
    install_docker_compose_monitoring
    
    # Start the cluster
    print_step "Starting cluster services..."
    systemctl enable pulse-cluster
    systemctl start pulse-cluster
    
    run_cluster_tests
    print_cluster_info
    
    echo ""
    echo -e "${GREEN}🎉 Pulse cluster setup complete!${NC}"
    echo -e "${BLUE}Ready to execute distributed workflows.${NC}"
}

# Run main function
main "$@"