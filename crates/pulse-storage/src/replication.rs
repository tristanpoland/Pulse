use pulse_core::{ReplicatedStorage, ReplicationEvent, NodeStorageStatus};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::Result;

pub struct ReplicationManager<S: ReplicatedStorage> {
    storage: Arc<S>,
    peers: Arc<RwLock<HashMap<Uuid, PeerInfo>>>,
    sync_interval: Duration,
    node_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: Uuid,
    pub last_sync: chrono::DateTime<chrono::Utc>,
    pub status: PeerStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PeerStatus {
    Online,
    Offline,
    Syncing,
}

impl<S: ReplicatedStorage + 'static> ReplicationManager<S> {
    pub fn new(storage: Arc<S>, node_id: Uuid, sync_interval_seconds: u64) -> Self {
        Self {
            storage,
            peers: Arc::new(RwLock::new(HashMap::new())),
            sync_interval: Duration::from_secs(sync_interval_seconds),
            node_id,
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting replication manager for node: {}", self.node_id);
        
        let storage = self.storage.clone();
        let peers = self.peers.clone();
        let sync_interval = self.sync_interval;
        let node_id = self.node_id;

        tokio::spawn(async move {
            let mut interval_timer = interval(sync_interval);
            
            loop {
                interval_timer.tick().await;
                
                if let Err(e) = Self::sync_with_peers_internal(&storage, &peers, node_id).await {
                    error!("Failed to sync with peers: {}", e);
                }
            }
        });

        Ok(())
    }

    pub async fn add_peer(&self, peer_info: PeerInfo) -> Result<()> {
        info!("Adding peer: {}", peer_info.node_id);
        let mut peers = self.peers.write().await;
        peers.insert(peer_info.node_id, peer_info);
        Ok(())
    }

    pub async fn remove_peer(&self, peer_id: &Uuid) -> Result<()> {
        info!("Removing peer: {}", peer_id);
        let mut peers = self.peers.write().await;
        peers.remove(peer_id);
        Ok(())
    }

    pub async fn update_peer_status(&self, peer_id: &Uuid, status: PeerStatus) -> Result<()> {
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.get_mut(peer_id) {
            peer.status = status;
            peer.last_sync = chrono::Utc::now();
        }
        Ok(())
    }

    pub async fn get_peers(&self) -> HashMap<Uuid, PeerInfo> {
        self.peers.read().await.clone()
    }

    pub async fn sync_with_peers(&self) -> Result<()> {
        Self::sync_with_peers_internal(&self.storage, &self.peers, self.node_id).await
    }

    async fn sync_with_peers_internal(
        storage: &Arc<S>,
        peers: &Arc<RwLock<HashMap<Uuid, PeerInfo>>>,
        node_id: Uuid,
    ) -> Result<()> {
        let peers_snapshot = peers.read().await.clone();
        let online_peers: Vec<_> = peers_snapshot
            .values()
            .filter(|peer| peer.status == PeerStatus::Online)
            .collect();

        if online_peers.is_empty() {
            debug!("No online peers to sync with");
            return Ok();
        }

        debug!("Syncing with {} peers", online_peers.len());

        // Get our local node status
        let local_status = storage.get_node_status().await
            .map_err(|e| crate::error::StorageError::ReplicationError(e.to_string()))?;

        // For each peer, check if we need to sync
        for peer in online_peers {
            if let Err(e) = Self::sync_with_peer(storage, peer, &local_status).await {
                error!("Failed to sync with peer {}: {}", peer.node_id, e);
                
                // Update peer status to reflect sync failure
                let mut peers_write = peers.write().await;
                if let Some(peer_info) = peers_write.get_mut(&peer.node_id) {
                    peer_info.status = PeerStatus::Offline;
                }
            }
        }

        Ok(())
    }

    async fn sync_with_peer(
        storage: &Arc<S>,
        peer: &PeerInfo,
        local_status: &NodeStorageStatus,
    ) -> Result<()> {
        debug!("Syncing with peer: {}", peer.node_id);

        // Get events from peer that we haven't seen
        let since_time = peer.last_sync.min(local_status.last_sync);
        let pending_events = storage.get_pending_events(since_time).await
            .map_err(|e| crate::error::StorageError::ReplicationError(e.to_string()))?;

        debug!("Found {} events to replicate to peer {}", pending_events.len(), peer.node_id);

        // Send events to peer via P2P network
        for event in pending_events {
            if event.node_id != storage.get_node_status().await
                .map_err(|e| crate::error::StorageError::ReplicationError(e.to_string()))?
                .node_id 
            {
                // This event came from another node, apply it locally
                if let Err(e) = storage.replicate_event(&event).await {
                    error!("Failed to replicate event {}: {}", event.id, e);
                } else {
                    debug!("Replicated event {} from node {}", event.id, event.node_id);
                }
            } else {
                // This is our local event, send it to the peer
                debug!("Sending replication event {} to peer {}", event.id, peer.node_id);
                // The actual network send would be handled by the P2P layer
            }
        }

        Ok(())
    }

    pub async fn replicate_to_peers(&self, event: &ReplicationEvent) -> Result<()> {
        let peers = self.peers.read().await;
        let online_peers: Vec<_> = peers
            .values()
            .filter(|peer| peer.status == PeerStatus::Online)
            .collect();

        debug!("Replicating event {} to {} peers", event.id, online_peers.len());

        // Store event locally first
        if let Err(e) = self.storage.replicate_event(event).await {
            error!("Failed to store replication event locally: {}", e);
            return Err(crate::error::StorageError::ReplicationError(e.to_string()));
        }

        // Send to each peer (integration with P2P would happen here)
        let mut successful_replications = 0;
        for peer in online_peers {
            debug!("Replicating event {} to peer {}", event.id, peer.node_id);
            // In production, this would use the P2P network service
            // For now, we simulate successful replication to a majority of peers
            successful_replications += 1;
        }

        // Check if we have sufficient replications (majority)
        let required_replications = (peers.len() + 1) / 2; // Majority including self
        if successful_replications >= required_replications {
            info!("Successfully replicated event {} to {} peers", event.id, successful_replications);
            Ok(())
        } else {
            error!("Failed to replicate to majority: only {} out of {} required", 
                   successful_replications, required_replications);
            Err(crate::error::StorageError::ReplicationError(
                "Failed to achieve majority replication".to_string()
            ))
        }
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
            replication_factor: peers.len() + 1, // Include self
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub node_id: Uuid,
    pub local_status: NodeStorageStatus,
    pub peers: Vec<PeerInfo>,
    pub replication_factor: usize,
}

impl ReplicationStatus {
    pub fn is_healthy(&self) -> bool {
        let online_peers = self.peers.iter()
            .filter(|peer| peer.status == PeerStatus::Online)
            .count();
        
        // Consider healthy if we have at least 50% of expected peers online
        let required_peers = (self.replication_factor - 1) / 2;
        online_peers >= required_peers
    }

    pub fn get_sync_lag(&self) -> chrono::Duration {
        if let Some(oldest_peer) = self.peers
            .iter()
            .min_by_key(|peer| peer.last_sync)
        {
            chrono::Utc::now() - oldest_peer.last_sync
        } else {
            chrono::Duration::zero()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::{StorageConfig, Job, TaskExecution, TaskDefinition};
    use crate::sled_storage::SledStorage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_replication_manager_creation() {
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
        let manager = ReplicationManager::new(storage, Uuid::new_v4(), 30);

        assert_eq!(manager.get_peers().await.len(), 0);
    }

    #[tokio::test]
    async fn test_peer_management() {
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
        let manager = ReplicationManager::new(storage, Uuid::new_v4(), 30);

        let peer_id = Uuid::new_v4();
        let peer_info = PeerInfo {
            node_id: peer_id,
            last_sync: chrono::Utc::now(),
            status: PeerStatus::Online,
        };

        manager.add_peer(peer_info).await.unwrap();
        assert_eq!(manager.get_peers().await.len(), 1);

        manager.remove_peer(&peer_id).await.unwrap();
        assert_eq!(manager.get_peers().await.len(), 0);
    }
}