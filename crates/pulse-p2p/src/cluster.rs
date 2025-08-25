use async_trait::async_trait;
use pulse_core::{
    ClusterEvent, ClusterManager, ClusterState, NodeInfo, NodeStatus,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    discovery::{DiscoveryService, NodeDiscovery},
    error::{P2PError, Result},
    messaging::{Message, MessageEnvelope},
    P2PNetworkService,
};

pub struct P2PClusterManager<N: P2PNetworkService> {
    network: Arc<N>,
    discovery: Arc<DiscoveryService<N>>,
    cluster_state: Arc<RwLock<ClusterState>>,
    node_id: Uuid,
    local_node_info: NodeInfo,
    leader_election_in_progress: Arc<RwLock<bool>>,
}

impl<N: P2PNetworkService + 'static> P2PClusterManager<N> {
    pub fn new(
        network: Arc<N>,
        discovery: Arc<DiscoveryService<N>>,
        node_id: Uuid,
        local_node_info: NodeInfo,
    ) -> Self {
        Self {
            network,
            discovery,
            cluster_state: Arc::new(RwLock::new(ClusterState::new())),
            node_id,
            local_node_info,
            leader_election_in_progress: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting P2P cluster manager for node: {}", self.node_id);

        // Add ourselves to the cluster state
        {
            let mut state = self.cluster_state.write().await;
            state.add_node(self.local_node_info.clone());
        }

        // Start discovery service
        self.discovery.start_discovery().await
            .map_err(|e| P2PError::DiscoveryError(e.to_string()))?;

        // Announce our presence
        let join_message = Message::JoinCluster {
            node_info: self.local_node_info.clone(),
        };

        let envelope = MessageEnvelope::new(self.node_id, join_message).broadcast();
        self.network.broadcast_message(envelope).await?;

        // Start periodic cluster maintenance
        self.start_cluster_maintenance().await;

        Ok(())
    }

    async fn start_cluster_maintenance(&self) {
        let cluster_state = self.cluster_state.clone();
        let network = self.network.clone();
        let node_id = self.node_id;
        let leader_election_in_progress = self.leader_election_in_progress.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                // Check if we need to elect a leader
                {
                    let state = cluster_state.read().await;
                    let election_in_progress = *leader_election_in_progress.read().await;
                    
                    if state.leader.is_none() && !election_in_progress {
                        drop(state);
                        if let Err(e) = Self::initiate_leader_election(
                            &network,
                            &cluster_state,
                            &leader_election_in_progress,
                            node_id,
                        ).await {
                            error!("Failed to initiate leader election: {}", e);
                        }
                    }
                }
                
                // Perform health checks and cleanup
                if let Err(e) = Self::perform_cluster_maintenance(&cluster_state, &network, node_id).await {
                    error!("Cluster maintenance failed: {}", e);
                }
            }
        });
    }

    async fn initiate_leader_election(
        network: &Arc<N>,
        cluster_state: &Arc<RwLock<ClusterState>>,
        leader_election_in_progress: &Arc<RwLock<bool>>,
        node_id: Uuid,
    ) -> Result<()> {
        {
            let mut election_flag = leader_election_in_progress.write().await;
            if *election_flag {
                return Ok(()); // Election already in progress
            }
            *election_flag = true;
        }

        info!("Initiating leader election");

        // Simple leader election: node with smallest UUID becomes leader
        let state = cluster_state.read().await;
        let healthy_nodes = state.get_healthy_nodes();
        
        if let Some(leader) = healthy_nodes.iter().min_by_key(|node| node.id) {
            drop(state);
            
            let event = ClusterEvent::LeaderElected {
                node_id: leader.id,
            };
            
            let message = Message::ClusterUpdate { event };
            let envelope = MessageEnvelope::new(node_id, message).broadcast();
            
            network.broadcast_message(envelope).await?;
            
            // Update our local state
            let mut state = cluster_state.write().await;
            state.set_leader(leader.id);
            
            info!("Elected new leader: {}", leader.id);
        }

        // Reset election flag
        {
            let mut election_flag = leader_election_in_progress.write().await;
            *election_flag = false;
        }

        Ok(())
    }

    async fn perform_cluster_maintenance(
        cluster_state: &Arc<RwLock<ClusterState>>,
        _network: &Arc<N>,
        _node_id: Uuid,
    ) -> Result<()> {
        let mut state = cluster_state.write().await;
        let now = chrono::Utc::now();
        
        // Find nodes that haven't been seen recently
        let stale_threshold = chrono::Duration::seconds(120);
        let mut stale_nodes = Vec::new();
        
        for (node_id, node) in &state.nodes {
            if now - node.last_seen > stale_threshold && node.status == NodeStatus::Online {
                stale_nodes.push(*node_id);
            }
        }

        // Mark stale nodes as offline
        for node_id in stale_nodes {
            if let Some(node) = state.nodes.get_mut(&node_id) {
                node.status = NodeStatus::Offline;
                warn!("Marked node {} as offline due to inactivity", node_id);
            }
        }

        Ok(())
    }

    pub async fn handle_join_cluster(&self, envelope: MessageEnvelope) -> Result<Option<MessageEnvelope>> {
        if let Message::JoinCluster { node_info } = envelope.payload {
            info!("Node {} is joining the cluster", node_info.id);

            // Add node to cluster state
            {
                let mut state = self.cluster_state.write().await;
                state.add_node(node_info.clone());
            }

            // Broadcast the join event
            let event = ClusterEvent::NodeJoined {
                node: node_info.clone(),
            };

            let cluster_update = Message::ClusterUpdate { event };
            let broadcast_envelope = MessageEnvelope::new(self.node_id, cluster_update).broadcast();
            
            self.network.broadcast_message(broadcast_envelope).await?;

            // Send current cluster state to the joining node
            let current_state = self.cluster_state.read().await;
            let state_nodes: Vec<_> = current_state.nodes.values().cloned().collect();
            drop(current_state);

            // Send each existing node as a separate join event to the new node
            for existing_node in state_nodes {
                if existing_node.id != node_info.id {
                    let node_event = ClusterEvent::NodeJoined {
                        node: existing_node,
                    };
                    
                    let node_update = Message::ClusterUpdate { event: node_event };
                    let node_envelope = MessageEnvelope::new(self.node_id, node_update)
                        .to_peer(node_info.id);
                    
                    self.network.send_message(node_info.id, node_envelope).await?;
                }
            }
        }

        Ok(None)
    }

    pub async fn handle_leave_cluster(&self, envelope: MessageEnvelope) -> Result<Option<MessageEnvelope>> {
        if let Message::LeaveCluster { node_id, reason } = envelope.payload {
            info!("Node {} is leaving the cluster: {}", node_id, reason);

            // Remove node from cluster state
            {
                let mut state = self.cluster_state.write().await;
                state.remove_node(&node_id);
            }

            // Broadcast the leave event
            let event = ClusterEvent::NodeLeft { node_id, reason };
            let cluster_update = Message::ClusterUpdate { event };
            let broadcast_envelope = MessageEnvelope::new(self.node_id, cluster_update).broadcast();
            
            self.network.broadcast_message(broadcast_envelope).await?;

            // Trigger leader election if the leader left
            let state = self.cluster_state.read().await;
            if state.leader == Some(node_id) {
                drop(state);
                self.elect_leader().await?;
            }
        }

        Ok(None)
    }

    pub async fn handle_cluster_update(&self, envelope: MessageEnvelope) -> Result<Option<MessageEnvelope>> {
        if let Message::ClusterUpdate { event } = envelope.payload {
            debug!("Processing cluster update event: {:?}", event);

            match event {
                ClusterEvent::NodeJoined { node } => {
                    let mut state = self.cluster_state.write().await;
                    state.add_node(node);
                }
                ClusterEvent::NodeLeft { node_id, .. } => {
                    let mut state = self.cluster_state.write().await;
                    state.remove_node(&node_id);
                }
                ClusterEvent::NodeUpdated { node } => {
                    let mut state = self.cluster_state.write().await;
                    state.update_node(node);
                }
                ClusterEvent::LeaderElected { node_id } => {
                    let mut state = self.cluster_state.write().await;
                    state.set_leader(node_id);
                    info!("Leader elected: {}", node_id);
                }
                _ => {
                    debug!("Unhandled cluster event: {:?}", event);
                }
            }
        }

        Ok(None)
    }

    pub async fn leave_cluster_gracefully(&self) -> Result<()> {
        info!("Leaving cluster gracefully");

        let leave_message = Message::LeaveCluster {
            node_id: self.node_id,
            reason: "Graceful shutdown".to_string(),
        };

        let envelope = MessageEnvelope::new(self.node_id, leave_message).broadcast();
        self.network.broadcast_message(envelope).await?;

        // Give some time for the message to propagate
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        Ok(())
    }

    pub async fn get_cluster_info(&self) -> ClusterState {
        self.cluster_state.read().await.clone()
    }
}

#[async_trait]
impl<N: P2PNetworkService + 'static> ClusterManager for P2PClusterManager<N> {
    async fn join_cluster(&self, node_info: &NodeInfo) -> pulse_core::Result<()> {
        let message = Message::JoinCluster {
            node_info: node_info.clone(),
        };

        let envelope = MessageEnvelope::new(self.node_id, message).broadcast();
        self.network.broadcast_message(envelope).await
            .map_err(|e| pulse_core::PulseError::ClusterError(e.to_string()))?;

        Ok(())
    }

    async fn leave_cluster(&self) -> pulse_core::Result<()> {
        self.leave_cluster_gracefully().await
            .map_err(|e| pulse_core::PulseError::ClusterError(e.to_string()))
    }

    async fn get_cluster_nodes(&self) -> pulse_core::Result<Vec<NodeInfo>> {
        let state = self.cluster_state.read().await;
        Ok(state.nodes.values().cloned().collect())
    }

    async fn get_node(&self, node_id: &Uuid) -> pulse_core::Result<Option<NodeInfo>> {
        let state = self.cluster_state.read().await;
        Ok(state.nodes.get(node_id).cloned())
    }

    async fn update_node_status(&self, node_id: &Uuid, status: NodeStatus) -> pulse_core::Result<()> {
        let mut state = self.cluster_state.write().await;
        if let Some(node) = state.nodes.get_mut(node_id) {
            node.status = status;
            node.last_seen = chrono::Utc::now();
            state.last_updated = chrono::Utc::now();
            state.cluster_version += 1;

            // Broadcast the update
            let event = ClusterEvent::NodeUpdated { node: node.clone() };
            let message = Message::ClusterUpdate { event };
            let envelope = MessageEnvelope::new(self.node_id, message).broadcast();
            
            drop(state); // Release the lock before awaiting
            
            self.network.broadcast_message(envelope).await
                .map_err(|e| pulse_core::PulseError::ClusterError(e.to_string()))?;
        }

        Ok(())
    }

    async fn is_leader(&self) -> pulse_core::Result<bool> {
        let state = self.cluster_state.read().await;
        Ok(state.leader == Some(self.node_id))
    }

    async fn get_leader(&self) -> pulse_core::Result<Option<Uuid>> {
        let state = self.cluster_state.read().await;
        Ok(state.leader)
    }

    async fn broadcast_event(&self, event: &ClusterEvent) -> pulse_core::Result<()> {
        let message = Message::ClusterUpdate { event: event.clone() };
        let envelope = MessageEnvelope::new(self.node_id, message).broadcast();
        
        self.network.broadcast_message(envelope).await
            .map_err(|e| pulse_core::PulseError::ClusterError(e.to_string()))?;

        Ok(())
    }

    async fn elect_leader(&self) -> pulse_core::Result<()> {
        Self::initiate_leader_election(
            &self.network,
            &self.cluster_state,
            &self.leader_election_in_progress,
            self.node_id,
        ).await
            .map_err(|e| pulse_core::PulseError::ClusterError(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::P2PService;
    use pulse_core::NodeCapabilities;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    async fn create_test_cluster_manager() -> P2PClusterManager<P2PService> {
        let node_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let network = Arc::new(P2PService::new(tx, node_id));
        
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
            capabilities: capabilities.clone(),
            last_seen: chrono::Utc::now(),
            metadata: HashMap::new(),
        };

        let discovery = Arc::new(DiscoveryService::new(
            P2PService::new(tx, node_id),
            node_id,
            node_info.clone(),
            60,
        ));

        P2PClusterManager::new(network, discovery, node_id, node_info)
    }

    #[tokio::test]
    async fn test_cluster_manager_creation() {
        let manager = create_test_cluster_manager().await;
        let cluster_info = manager.get_cluster_info().await;
        
        // Should contain our own node
        assert_eq!(cluster_info.nodes.len(), 1);
        assert!(cluster_info.nodes.contains_key(&manager.node_id));
    }

    #[tokio::test]
    async fn test_join_cluster_handling() {
        let manager = create_test_cluster_manager().await;
        let new_node_id = Uuid::new_v4();
        
        let capabilities = NodeCapabilities {
            max_concurrent_tasks: 5,
            available_executors: vec!["docker".to_string()],
            resource_limits: HashMap::new(),
            labels: HashMap::new(),
        };

        let new_node_info = NodeInfo {
            id: new_node_id,
            address: "test:9000".to_string(),
            status: NodeStatus::Online,
            capabilities,
            last_seen: chrono::Utc::now(),
            metadata: HashMap::new(),
        };

        let join_message = Message::JoinCluster {
            node_info: new_node_info,
        };

        let envelope = MessageEnvelope::new(new_node_id, join_message);
        
        let _response = manager.handle_join_cluster(envelope).await.unwrap();
        
        let cluster_info = manager.get_cluster_info().await;
        assert_eq!(cluster_info.nodes.len(), 2);
        assert!(cluster_info.nodes.contains_key(&new_node_id));
    }
}