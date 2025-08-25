use async_trait::async_trait;
use futures::stream::StreamExt;
use libp2p::{
    gossipsub, identify, kad,
    mdns,
    noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    error::{P2PError, Result},
    messaging::{MessageEnvelope, MessageRouter},
};

#[derive(NetworkBehaviour)]
pub struct PulseBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub identify: identify::Behaviour,
}

pub struct P2PNetwork {
    swarm: Swarm<PulseBehaviour>,
    node_id: Uuid,
    message_router: MessageRouter,
    message_sender: mpsc::UnboundedSender<MessageEnvelope>,
    message_receiver: mpsc::UnboundedReceiver<MessageEnvelope>,
    peer_id: PeerId,
    topics: std::collections::HashSet<gossipsub::IdentTopic>,
}

impl P2PNetwork {
    pub async fn new(node_id: Uuid, listen_addr: Multiaddr) -> Result<Self> {
        info!("Initializing P2P network for node: {}", node_id);

        // Create a random PeerId
        let local_key = libp2p::identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        
        info!("Local peer ID: {}", local_peer_id);

        // Set up the swarm
        let swarm = SwarmBuilder::with_existing_identity(local_key)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(|e| P2PError::NetworkError(format!("Failed to configure transport: {}", e)))?
            .with_behaviour(|key| {
                // Create a Gossipsub topic for the cluster
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10))
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .build()
                    .map_err(|e| P2PError::ProtocolError(format!("Gossipsub config error: {}", e)))?;

                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )
                .map_err(|e| P2PError::ProtocolError(format!("Gossipsub init error: {}", e)))?;

                // Create mDNS for local discovery
                let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
                    .map_err(|e| P2PError::DiscoveryError(format!("mDNS init error: {}", e)))?;

                // Create Kademlia for DHT
                let store = kad::store::MemoryStore::new(local_peer_id);
                let kademlia = kad::Behaviour::new(local_peer_id, store);

                // Create Identify protocol
                let identify = identify::Behaviour::new(identify::Config::new(
                    "/pulse/1.0.0".to_string(),
                    key.public(),
                ));

                Ok(PulseBehaviour {
                    gossipsub,
                    mdns,
                    kademlia,
                    identify,
                })
            })
            .map_err(|e| P2PError::NetworkError(format!("Failed to create behaviour: {}", e)))?
            .build();

        // Create message channel
        let (message_sender, message_receiver) = mpsc::unbounded_channel();

        let mut network = Self {
            swarm,
            node_id,
            message_router: MessageRouter::new(),
            message_sender,
            message_receiver,
            peer_id: local_peer_id,
            topics: std::collections::HashSet::new(),
        };

        // Start listening on the specified address
        network.start_listening(listen_addr).await?;

        // Subscribe to default topics
        network.subscribe_to_topic("pulse.cluster").await?;
        network.subscribe_to_topic("pulse.jobs").await?;
        network.subscribe_to_topic("pulse.storage").await?;

        Ok(network)
    }

    pub async fn start_listening(&mut self, addr: Multiaddr) -> Result<()> {
        self.swarm
            .listen_on(addr)
            .map_err(|e| P2PError::NetworkError(format!("Failed to listen: {}", e)))?;
        
        info!("Started listening on address");
        Ok(())
    }

    pub async fn subscribe_to_topic(&mut self, topic_name: &str) -> Result<()> {
        let topic = gossipsub::IdentTopic::new(topic_name);
        
        self.swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic)
            .map_err(|e| P2PError::ProtocolError(format!("Failed to subscribe to topic: {}", e)))?;
        
        self.topics.insert(topic);
        info!("Subscribed to topic: {}", topic_name);
        Ok(())
    }

    pub async fn publish_message(&mut self, topic: &str, message: &MessageEnvelope) -> Result<()> {
        let topic = gossipsub::IdentTopic::new(topic);
        let serialized = serde_json::to_vec(message)
            .map_err(|e| P2PError::SerializationError(format!("Failed to serialize message: {}", e)))?;

        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic, serialized)
            .map_err(|e| P2PError::NetworkError(format!("Failed to publish message: {}", e)))?;

        debug!("Published message of type: {} to topic: {}", message.message_type, topic);
        Ok(())
    }

    pub async fn send_direct_message(&mut self, peer_id: PeerId, message: MessageEnvelope) -> Result<()> {
        // For direct messages, we'll use a dedicated topic
        let direct_topic = format!("pulse.direct.{}", peer_id);
        self.publish_message(&direct_topic, &message).await
    }

    pub async fn dial_peer(&mut self, addr: Multiaddr) -> Result<()> {
        self.swarm
            .dial(addr)
            .map_err(|e| P2PError::ConnectionError(format!("Failed to dial peer: {}", e)))?;
        
        Ok(())
    }

    pub fn get_connected_peers(&self) -> Vec<PeerId> {
        self.swarm.connected_peers().cloned().collect()
    }

    pub fn get_node_id(&self) -> Uuid {
        self.node_id
    }

    pub fn get_peer_id(&self) -> PeerId {
        self.peer_id
    }

    pub fn register_message_handler<F, Fut>(&mut self, message_type: String, handler: F)
    where
        F: Fn(MessageEnvelope) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Option<MessageEnvelope>>> + Send + 'static,
    {
        self.message_router.register_handler(message_type, handler);
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting P2P network event loop for node: {}", self.node_id);

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await?;
                }
                
                message = self.message_receiver.recv() => {
                    if let Some(envelope) = message {
                        self.handle_outgoing_message(envelope).await?;
                    }
                }
            }
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<PulseBehaviourEvent>) -> Result<()> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address);
            }
            
            SwarmEvent::Behaviour(PulseBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (peer_id, multiaddr) in list {
                    info!("mDNS discovered peer: {} at {}", peer_id, multiaddr);
                    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);
                }
            }
            
            SwarmEvent::Behaviour(PulseBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                for (peer_id, multiaddr) in list {
                    debug!("mDNS expired peer: {} at {}", peer_id, multiaddr);
                }
            }
            
            SwarmEvent::Behaviour(PulseBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                message,
                ..
            })) => {
                self.handle_gossipsub_message(message).await?;
            }
            
            SwarmEvent::Behaviour(PulseBehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                ..
            })) => {
                debug!("Identified peer: {} with info: {:?}", peer_id, info);
                
                // Add peer to Kademlia
                for addr in &info.listen_addrs {
                    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
                }
            }
            
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connection established with peer: {}", peer_id);
            }
            
            SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                info!("Connection closed with peer: {} due to: {:?}", peer_id, cause);
            }
            
            SwarmEvent::IncomingConnection { connection_id, .. } => {
                debug!("Incoming connection: {:?}", connection_id);
            }
            
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                warn!("Outgoing connection error to {:?}: {}", peer_id, error);
            }
            
            _ => {}
        }
        
        Ok(())
    }

    async fn handle_gossipsub_message(&mut self, message: gossipsub::Message) -> Result<()> {
        let envelope: MessageEnvelope = serde_json::from_slice(&message.data)
            .map_err(|e| P2PError::SerializationError(format!("Failed to deserialize message: {}", e)))?;

        debug!("Received message of type: {} from peer", envelope.message_type);

        // Route the message through our message router
        if let Some(response) = self.message_router.route_message(envelope).await? {
            // If the handler returned a response, send it back
            self.handle_outgoing_message(response).await?;
        }

        Ok(())
    }

    async fn handle_outgoing_message(&mut self, envelope: MessageEnvelope) -> Result<()> {
        if let Some(target_peer) = envelope.to {
            // Convert UUID to PeerId (this is a simplified approach)
            let peer_id = self.uuid_to_peer_id(target_peer);
            self.send_direct_message(peer_id, envelope).await?;
        } else {
            // Broadcast message - determine appropriate topic based on message type
            let topic = self.get_topic_for_message(&envelope.message_type);
            self.publish_message(&topic, &envelope).await?;
        }
        Ok(())
    }

    fn get_topic_for_message(&self, message_type: &str) -> String {
        match message_type {
            "join_cluster" | "leave_cluster" | "cluster_update" | "peer_discovery" => "pulse.cluster".to_string(),
            "submit_job" | "job_status_update" | "assign_task" | "task_status_update" | "task_completed" => "pulse.jobs".to_string(),
            "replicate_event" | "sync_request" | "sync_response" => "pulse.storage".to_string(),
            _ => "pulse.cluster".to_string(), // Default to cluster topic
        }
    }

    fn uuid_to_peer_id(&self, uuid: Uuid) -> PeerId {
        // This is a simplified conversion - in a real implementation,
        // you'd want to maintain a mapping between UUIDs and PeerIds
        let mut hasher = DefaultHasher::new();
        uuid.hash(&mut hasher);
        let hash = hasher.finish();
        
        // Generate a deterministic PeerId from the hash
        let key_bytes = hash.to_be_bytes();
        let mut extended_bytes = [0u8; 32];
        extended_bytes[..8].copy_from_slice(&key_bytes);
        
        let keypair = libp2p::identity::ed25519::Keypair::from(libp2p::identity::ed25519::SecretKey::from_bytes(extended_bytes).unwrap());
        PeerId::from(libp2p::identity::Keypair::Ed25519(keypair).public())
    }

    pub fn get_message_sender(&self) -> mpsc::UnboundedSender<MessageEnvelope> {
        self.message_sender.clone()
    }
}

#[async_trait]
pub trait P2PNetworkService: Send + Sync {
    async fn broadcast_message(&self, message: MessageEnvelope) -> Result<()>;
    async fn send_message(&self, peer_id: Uuid, message: MessageEnvelope) -> Result<()>;
    async fn get_connected_peers(&self) -> Vec<Uuid>;
    async fn connect_to_peer(&self, address: String) -> Result<()>;
}

pub struct P2PService {
    message_sender: mpsc::UnboundedSender<MessageEnvelope>,
    node_id: Uuid,
}

impl P2PService {
    pub fn new(message_sender: mpsc::UnboundedSender<MessageEnvelope>, node_id: Uuid) -> Self {
        Self {
            message_sender,
            node_id,
        }
    }
}

#[async_trait]
impl P2PNetworkService for P2PService {
    async fn broadcast_message(&self, message: MessageEnvelope) -> Result<()> {
        self.message_sender
            .send(message)
            .map_err(|e| P2PError::NetworkError(format!("Failed to send message: {}", e)))?;
        Ok(())
    }

    async fn send_message(&self, peer_id: Uuid, message: MessageEnvelope) -> Result<()> {
        let targeted_message = MessageEnvelope {
            to: Some(peer_id),
            ..message
        };
        
        self.message_sender
            .send(targeted_message)
            .map_err(|e| P2PError::NetworkError(format!("Failed to send message: {}", e)))?;
        Ok(())
    }

    async fn get_connected_peers(&self) -> Vec<Uuid> {
        // This would be implemented by querying the actual network state
        // For now, return empty as this requires integration with the swarm
        Vec::new()
    }

    async fn connect_to_peer(&self, _address: String) -> Result<()> {
        // This would be implemented by sending a dial command to the swarm
        // For now, just return Ok as this requires integration with the swarm
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_p2p_network_creation() {
        let node_id = Uuid::new_v4();
        let listen_addr: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
        
        let result = P2PNetwork::new(node_id, listen_addr).await;
        assert!(result.is_ok());
        
        let network = result.unwrap();
        assert_eq!(network.get_node_id(), node_id);
        assert_eq!(network.get_connected_peers().len(), 0);
    }
}