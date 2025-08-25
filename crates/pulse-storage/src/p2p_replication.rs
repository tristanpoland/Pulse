use pulse_core::{ReplicatedStorage, ReplicationEvent, NodeStorageStatus};
use pulse_p2p::{MessageEnvelope, Message, P2PNetworkService};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::Result;
use crate::replication::{PeerInfo, PeerStatus, ReplicationStatus};

/// Production-ready replication manager with P2P integration
pub struct P2PReplicationManager<S: ReplicatedStorage> {
    storage: Arc<S>,
    p2p_service: Arc<dyn P2PNetworkService>,
    peers: Arc<RwLock<std::collections::HashMap<Uuid, PeerInfo>>>,
    sync_interval: Duration,
    node_id: Uuid,
    replication_factor: usize,
}

impl<S: ReplicatedStorage + 'static> P2PReplicationManager<S> {
    pub fn new(
        storage: Arc<S>,
        p2p_service: Arc<dyn P2PNetworkService>,
        node_id: Uuid,
        sync_interval_seconds: u64,
        replication_factor: usize,
    ) -> Self {
        Self {
            storage,
            p2p_service,
            peers: Arc::new(RwLock::new(std::collections::HashMap::new())),
            sync_interval: Duration::from_secs(sync_interval_seconds),
            node_id,
            replication_factor,
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting P2P replication manager for node: {}", self.node_id);
        
        let storage = self.storage.clone();
        let p2p_service = self.p2p_service.clone();
        let peers = self.peers.clone();
        let sync_interval = self.sync_interval;
        let node_id = self.node_id;

        // Start periodic sync task
        tokio::spawn(async move {
            let mut interval_timer = interval(sync_interval);
            
            loop {
                interval_timer.tick().await;
                
                if let Err(e) = Self::sync_with_peers_internal(
                    &storage, 
                    &p2p_service,
                    &peers, 
                    node_id
                ).await {
                    error!("Failed to sync with peers: {}", e);
                }
            }
        });

        Ok(())
    }

    pub async fn handle_replication_message(&self, envelope: MessageEnvelope) -> Result<()> {
        match envelope.payload {
            Message::ReplicateEvent { event } => {
                self.handle_incoming_replication_event(event, envelope.from).await
            }
            Message::SyncRequest { since } => {
                self.handle_sync_request(since, envelope.from).await
            }
            Message::SyncResponse { events } => {
                self.handle_sync_response(events, envelope.from).await
            }
            _ => {
                warn!("Unexpected message type for replication: {}", envelope.message_type);
                Ok(())
            }
        }
    }

    async fn handle_incoming_replication_event(
        &self,
        event: ReplicationEvent,
        from_peer: Uuid,
    ) -> Result<()> {
        debug!("Handling replication event {} from peer {}", event.id, from_peer);

        // Apply the event locally
        if let Err(e) = self.storage.replicate_event(&event).await {
            error!("Failed to apply replication event {}: {}", event.id, e);
            return Err(crate::error::StorageError::ReplicationError(e.to_string()));
        }

        // Update peer sync status
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.get_mut(&from_peer) {
            peer.last_sync = chrono::Utc::now();
        }

        info!("Successfully applied replication event {} from peer {}", event.id, from_peer);
        Ok(())
    }

    async fn handle_sync_request(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        requesting_peer: Uuid,
    ) -> Result<()> {
        debug!("Handling sync request from peer {} since {}", requesting_peer, since);

        // Get events since the requested timestamp
        let events = self.storage.get_pending_events(since).await
            .map_err(|e| crate::error::StorageError::ReplicationError(e.to_string()))?;

        // Filter events to only include our local events
        let local_node_status = self.storage.get_node_status().await
            .map_err(|e| crate::error::StorageError::ReplicationError(e.to_string()))?;
        
        let local_events: Vec<_> = events.into_iter()
            .filter(|event| event.node_id == local_node_status.node_id)
            .collect();

        debug!("Sending {} events to peer {} in sync response", local_events.len(), requesting_peer);

        // Send sync response
        let response_message = Message::SyncResponse {
            events: local_events,
        };

        let envelope = MessageEnvelope::new(self.node_id, response_message)
            .to_peer(requesting_peer);

        if let Err(e) = self.p2p_service.send_message(requesting_peer, envelope).await {
            error!("Failed to send sync response to peer {}: {}", requesting_peer, e);
            return Err(crate::error::StorageError::ReplicationError(e.to_string()));
        }

        Ok(())
    }

    async fn handle_sync_response(
        &self,
        events: Vec<ReplicationEvent>,
        from_peer: Uuid,
    ) -> Result<()> {
        debug!("Handling sync response with {} events from peer {}", events.len(), from_peer);

        let mut successful_applications = 0;
        for event in events {
            if let Err(e) = self.storage.replicate_event(&event).await {
                error!("Failed to apply event {} from sync response: {}", event.id, e);
            } else {
                successful_applications += 1;
                debug!("Applied event {} from sync response", event.id);
            }
        }

        // Update peer sync status
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.get_mut(&from_peer) {
            peer.last_sync = chrono::Utc::now();
            peer.status = PeerStatus::Online;
        }

        info!("Successfully applied {} events from sync response from peer {}", 
              successful_applications, from_peer);
        Ok(())
    }

    pub async fn replicate_event_to_peers(&self, event: &ReplicationEvent) -> Result<()> {
        let peers = self.peers.read().await;
        let online_peers: Vec<_> = peers
            .values()
            .filter(|peer| peer.status == PeerStatus::Online)
            .collect();

        debug!("Replicating event {} to {} online peers", event.id, online_peers.len());

        // Store event locally first
        if let Err(e) = self.storage.replicate_event(event).await {
            error!("Failed to store replication event locally: {}", e);
            return Err(crate::error::StorageError::ReplicationError(e.to_string()));
        }

        // Send to peers via P2P network
        let mut successful_replications = 1; // Count self
        for peer in online_peers.iter() {
            let message = Message::ReplicateEvent {
                event: event.clone(),
            };

            let envelope = MessageEnvelope::new(self.node_id, message)
                .to_peer(peer.node_id);

            match self.p2p_service.send_message(peer.node_id, envelope).await {
                Ok(()) => {
                    debug!("Successfully sent replication event {} to peer {}", 
                           event.id, peer.node_id);
                    successful_replications += 1;
                }
                Err(e) => {
                    error!("Failed to send replication event {} to peer {}: {}", 
                           event.id, peer.node_id, e);
                    
                    // Mark peer as potentially offline
                    drop(peers); // Release read lock
                    let mut peers_write = self.peers.write().await;
                    if let Some(peer_info) = peers_write.get_mut(&peer.node_id) {
                        peer_info.status = PeerStatus::Offline;
                    }
                }
            }
        }

        // Check if we have sufficient replications
        let required_replications = std::cmp::min(self.replication_factor, online_peers.len() + 1);
        if successful_replications >= required_replications {
            info!("Successfully replicated event {} to {} peers (required: {})", 
                  event.id, successful_replications, required_replications);
            Ok(())
        } else {
            error!("Failed to achieve required replication factor: {} out of {} required", 
                   successful_replications, required_replications);
            Err(crate::error::StorageError::ReplicationError(
                format!("Failed to achieve replication factor: {}/{}", 
                        successful_replications, required_replications)
            ))
        }
    }

    async fn sync_with_peers_internal(
        storage: &Arc<S>,
        p2p_service: &Arc<dyn P2PNetworkService>,
        peers: &Arc<RwLock<std::collections::HashMap<Uuid, PeerInfo>>>,
        node_id: Uuid,
    ) -> Result<()> {
        let peers_snapshot = peers.read().await.clone();
        let online_peers: Vec<_> = peers_snapshot
            .values()
            .filter(|peer| peer.status == PeerStatus::Online)
            .collect();

        if online_peers.is_empty() {
            debug!("No online peers to sync with");
            return Ok(());
        }

        debug!("Starting sync with {} online peers", online_peers.len());

        for peer in online_peers {
            // Send sync request to each peer
            let sync_message = Message::SyncRequest {
                since: peer.last_sync,
            };

            let envelope = MessageEnvelope::new(node_id, sync_message)
                .to_peer(peer.node_id);

            if let Err(e) = p2p_service.send_message(peer.node_id, envelope).await {
                error!("Failed to send sync request to peer {}: {}", peer.node_id, e);
                
                // Mark peer as offline
                let mut peers_write = peers.write().await;
                if let Some(peer_info) = peers_write.get_mut(&peer.node_id) {
                    peer_info.status = PeerStatus::Offline;
                }
            } else {
                debug!("Sent sync request to peer {}", peer.node_id);
            }
        }

        Ok(())
    }

    pub async fn add_peer(&self, peer_info: PeerInfo) -> Result<()> {
        info!("Adding replication peer: {}", peer_info.node_id);
        let mut peers = self.peers.write().await;
        peers.insert(peer_info.node_id, peer_info);
        Ok(())
    }

    pub async fn remove_peer(&self, peer_id: &Uuid) -> Result<()> {
        info!("Removing replication peer: {}", peer_id);
        let mut peers = self.peers.write().await;
        peers.remove(peer_id);
        Ok(())
    }

    pub async fn get_replication_status(&self) -> Result<ReplicationStatus> {
        let local_status = self.storage.get_node_status().await
            .map_err(|e| crate::error::StorageError::ReplicationError(e.to_string()))?;
        
        let peers = self.peers.read().await;
        let peer_statuses: Vec<_> = peers.values().cloned().collect();

        Ok(ReplicationStatus {
            node_id: self.node_id,
            local_status,
            peers: peer_statuses,
            replication_factor: peers.len() + 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::StorageConfig;
    use crate::sled_storage::SledStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_p2p_replication_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            node_id: Uuid::new_v4(),
            replication_factor: 3,
            sync_interval_seconds: 60,
            max_storage_size_mb: None,
            custom_options: std::collections::HashMap::new(),
        };

        let storage = Arc::new(SledStorage::new(config).await.unwrap());
        
        // Mock P2P service for testing
        struct MockP2PService;
        
        #[async_trait::async_trait]
        impl P2PNetworkService for MockP2PService {
            async fn broadcast_message(&self, _message: MessageEnvelope) -> pulse_p2p::Result<()> {
                Ok(())
            }
            
            async fn send_message(&self, _peer_id: Uuid, _message: MessageEnvelope) -> pulse_p2p::Result<()> {
                Ok(())
            }
            
            async fn get_connected_peers(&self) -> Vec<Uuid> {
                Vec::new()
            }
            
            async fn connect_to_peer(&self, _address: String) -> pulse_p2p::Result<()> {
                Ok(())
            }
        }

        let p2p_service = Arc::new(MockP2PService);
        let node_id = Uuid::new_v4();
        
        let _manager = P2PReplicationManager::new(
            storage,
            p2p_service,
            node_id,
            30,
            3,
        );

        // Test passes if manager is created successfully
    }
}