use async_trait::async_trait;
use pulse_core::{NodeInfo, NodeCapabilities, NodeStatus};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{interval, Instant};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    error::{P2PError, Result},
    messaging::{Message, MessageEnvelope},
    P2PNetworkService,
};

#[async_trait]
pub trait NodeDiscovery: Send + Sync {
    async fn start_discovery(&self) -> Result<()>;
    async fn stop_discovery(&self) -> Result<()>;
    async fn discover_peers(&self) -> Result<Vec<NodeInfo>>;
    async fn announce_node(&self, node_info: &NodeInfo) -> Result<()>;
    async fn get_discovered_nodes(&self) -> Result<Vec<NodeInfo>>;
}

pub struct DiscoveryService<N: P2PNetworkService> {
    network: N,
    node_id: Uuid,
    local_node_info: NodeInfo,
    discovered_nodes: RwLock<HashMap<Uuid, DiscoveredNode>>,
    discovery_interval: Duration,
    heartbeat_interval: Duration,
    node_timeout: Duration,
}

#[derive(Debug, Clone)]
struct DiscoveredNode {
    info: NodeInfo,
    last_seen: Instant,
    discovery_source: DiscoverySource,
}

#[derive(Debug, Clone)]
enum DiscoverySource {
    Multicast,
    PeerAnnouncement,
    Bootstrap,
}

impl<N: P2PNetworkService> DiscoveryService<N> {
    pub fn new(
        network: N,
        node_id: Uuid,
        local_node_info: NodeInfo,
        discovery_interval_secs: u64,
    ) -> Self {
        Self {
            network,
            node_id,
            local_node_info,
            discovered_nodes: RwLock::new(HashMap::new()),
            discovery_interval: Duration::from_secs(discovery_interval_secs),
            heartbeat_interval: Duration::from_secs(30),
            node_timeout: Duration::from_secs(120),
        }
    }

    pub async fn handle_peer_discovery(&self, envelope: MessageEnvelope) -> Result<Option<MessageEnvelope>> {
        if let Message::PeerDiscovery { node_id, capabilities } = envelope.payload {
            info!("Discovered new peer: {}", node_id);

            let node_info = NodeInfo {
                id: node_id,
                address: format!("peer:{}", node_id), // Simplified address
                status: NodeStatus::Online,
                capabilities,
                last_seen: chrono::Utc::now(),
                metadata: std::collections::HashMap::new(),
            };

            let discovered_node = DiscoveredNode {
                info: node_info.clone(),
                last_seen: Instant::now(),
                discovery_source: DiscoverySource::PeerAnnouncement,
            };

            let mut nodes = self.discovered_nodes.write().await;
            nodes.insert(node_id, discovered_node);

            debug!("Added peer {} to discovered nodes", node_id);

            // Send our info back if this wasn't a response to our announcement
            if envelope.correlation_id.is_none() {
                let response_message = Message::PeerDiscovery {
                    node_id: self.node_id,
                    capabilities: self.local_node_info.capabilities.clone(),
                };

                let response = MessageEnvelope::new(self.node_id, response_message)
                    .to_peer(node_id)
                    .as_response(envelope.id);

                return Ok(Some(response));
            }
        }

        Ok(None)
    }

    async fn cleanup_stale_nodes(&self) {
        let mut nodes = self.discovered_nodes.write().await;
        let now = Instant::now();

        let stale_nodes: Vec<Uuid> = nodes
            .iter()
            .filter_map(|(id, node)| {
                if now.duration_since(node.last_seen) > self.node_timeout {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in stale_nodes {
            if let Some(node) = nodes.remove(&node_id) {
                warn!("Removed stale node: {} (last seen: {:?} ago)", 
                      node_id, 
                      now.duration_since(node.last_seen));
            }
        }
    }

    pub async fn update_node_last_seen(&self, node_id: Uuid) {
        let mut nodes = self.discovered_nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.last_seen = Instant::now();
            node.info.last_seen = chrono::Utc::now();
        }
    }

    pub async fn remove_node(&self, node_id: &Uuid) -> Result<()> {
        let mut nodes = self.discovered_nodes.write().await;
        if nodes.remove(node_id).is_some() {
            info!("Removed node {} from discovery", node_id);
        }
        Ok(())
    }

    pub async fn get_node(&self, node_id: &Uuid) -> Option<NodeInfo> {
        let nodes = self.discovered_nodes.read().await;
        nodes.get(node_id).map(|node| node.info.clone())
    }

    pub async fn get_healthy_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.discovered_nodes.read().await;
        let now = Instant::now();

        nodes
            .values()
            .filter(|node| {
                now.duration_since(node.last_seen) < self.node_timeout &&
                node.info.status == NodeStatus::Online
            })
            .map(|node| node.info.clone())
            .collect()
    }
}

#[async_trait]
impl<N: P2PNetworkService> NodeDiscovery for DiscoveryService<N> {
    async fn start_discovery(&self) -> Result<()> {
        info!("Starting node discovery service");

        // Start periodic discovery announcements
        let network = &self.network;
        let node_id = self.node_id;
        let local_capabilities = self.local_node_info.capabilities.clone();
        let discovery_interval = self.discovery_interval;

        tokio::spawn({
            async move {
                let mut discovery_timer = interval(discovery_interval);
                
                loop {
                    discovery_timer.tick().await;
                    
                    let announcement = Message::PeerDiscovery {
                        node_id,
                        capabilities: local_capabilities.clone(),
                    };

                    let envelope = MessageEnvelope::new(node_id, announcement).broadcast();
                    
                    if let Err(e) = network.broadcast_message(envelope).await {
                        error!("Failed to broadcast discovery announcement: {}", e);
                    } else {
                        debug!("Sent discovery announcement");
                    }
                }
            }
        });

        // Start cleanup task for stale nodes
        let discovery_service = self as *const Self;
        let cleanup_interval = self.heartbeat_interval;

        tokio::spawn({
            async move {
                let mut cleanup_timer = interval(cleanup_interval);
                
                loop {
                    cleanup_timer.tick().await;
                    
                    // Safety: This is safe because we're only accessing immutable data
                    // and the cleanup method uses interior mutability with RwLock
                    unsafe {
                        (*discovery_service).cleanup_stale_nodes().await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn stop_discovery(&self) -> Result<()> {
        info!("Stopping node discovery service");
        // In a real implementation, we'd store task handles and cancel them here
        Ok(())
    }

    async fn discover_peers(&self) -> Result<Vec<NodeInfo>> {
        debug!("Performing peer discovery scan");
        
        let announcement = Message::PeerDiscovery {
            node_id: self.node_id,
            capabilities: self.local_node_info.capabilities.clone(),
        };

        let envelope = MessageEnvelope::new(self.node_id, announcement)
            .broadcast()
            .with_response_required();

        self.network.broadcast_message(envelope).await?;

        // Wait a bit for responses
        tokio::time::sleep(Duration::from_secs(2)).await;

        self.get_discovered_nodes().await
    }

    async fn announce_node(&self, node_info: &NodeInfo) -> Result<()> {
        let announcement = Message::PeerDiscovery {
            node_id: node_info.id,
            capabilities: node_info.capabilities.clone(),
        };

        let envelope = MessageEnvelope::new(self.node_id, announcement).broadcast();
        self.network.broadcast_message(envelope).await?;

        Ok(())
    }

    async fn get_discovered_nodes(&self) -> Result<Vec<NodeInfo>> {
        let nodes = self.discovered_nodes.read().await;
        Ok(nodes.values().map(|node| node.info.clone()).collect())
    }
}

pub struct BootstrapService {
    bootstrap_nodes: Vec<String>,
    node_id: Uuid,
}

impl BootstrapService {
    pub fn new(bootstrap_nodes: Vec<String>, node_id: Uuid) -> Self {
        Self {
            bootstrap_nodes,
            node_id,
        }
    }

    pub async fn bootstrap<N: P2PNetworkService>(&self, network: &N) -> Result<Vec<NodeInfo>> {
        info!("Starting bootstrap process with {} bootstrap nodes", self.bootstrap_nodes.len());
        
        let mut discovered_nodes = Vec::new();

        for bootstrap_addr in &self.bootstrap_nodes {
            match self.connect_to_bootstrap_node(network, bootstrap_addr).await {
                Ok(nodes) => {
                    discovered_nodes.extend(nodes);
                    info!("Successfully bootstrapped from: {}", bootstrap_addr);
                }
                Err(e) => {
                    warn!("Failed to bootstrap from {}: {}", bootstrap_addr, e);
                }
            }
        }

        if discovered_nodes.is_empty() {
            warn!("No nodes discovered during bootstrap process");
        } else {
            info!("Discovered {} nodes during bootstrap", discovered_nodes.len());
        }

        Ok(discovered_nodes)
    }

    async fn connect_to_bootstrap_node<N: P2PNetworkService>(
        &self,
        network: &N,
        bootstrap_addr: &str,
    ) -> Result<Vec<NodeInfo>> {
        debug!("Connecting to bootstrap node: {}", bootstrap_addr);

        // Connect to the bootstrap node
        network.connect_to_peer(bootstrap_addr.to_string()).await?;

        // Send discovery message
        let discovery_message = Message::PeerDiscovery {
            node_id: self.node_id,
            capabilities: self.get_default_capabilities(),
        };

        let envelope = MessageEnvelope::new(self.node_id, discovery_message)
            .broadcast()
            .with_response_required();

        network.broadcast_message(envelope).await?;

        // Wait for responses
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Return empty for now - in a real implementation, we'd collect responses
        Ok(Vec::new())
    }

    fn get_default_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            max_concurrent_tasks: 10,
            available_executors: vec!["shell".to_string(), "docker".to_string()],
            resource_limits: std::collections::HashMap::new(),
            labels: std::collections::HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::P2PService;
    use tokio::sync::mpsc;

    async fn create_test_discovery() -> DiscoveryService<P2PService> {
        let node_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let network = P2PService::new(tx, node_id);
        
        let capabilities = NodeCapabilities {
            max_concurrent_tasks: 10,
            available_executors: vec!["test".to_string()],
            resource_limits: HashMap::new(),
            labels: HashMap::new(),
        };

        let node_info = NodeInfo {
            id: node_id,
            address: "test:8080".to_string(),
            status: NodeStatus::Online,
            capabilities,
            last_seen: chrono::Utc::now(),
            metadata: HashMap::new(),
        };

        DiscoveryService::new(network, node_id, node_info, 60)
    }

    #[tokio::test]
    async fn test_discovery_service_creation() {
        let discovery = create_test_discovery().await;
        assert_eq!(discovery.get_discovered_nodes().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_peer_discovery_message_handling() {
        let discovery = create_test_discovery().await;
        let peer_id = Uuid::new_v4();
        
        let capabilities = NodeCapabilities {
            max_concurrent_tasks: 5,
            available_executors: vec!["docker".to_string()],
            resource_limits: HashMap::new(),
            labels: HashMap::new(),
        };

        let message = Message::PeerDiscovery {
            node_id: peer_id,
            capabilities,
        };

        let envelope = MessageEnvelope::new(peer_id, message);
        
        let response = discovery.handle_peer_discovery(envelope).await.unwrap();
        assert!(response.is_some());

        let discovered_nodes = discovery.get_discovered_nodes().await.unwrap();
        assert_eq!(discovered_nodes.len(), 1);
        assert_eq!(discovered_nodes[0].id, peer_id);
    }
}