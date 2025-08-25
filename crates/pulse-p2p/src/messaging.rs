use serde::{Deserialize, Serialize};
use uuid::Uuid;
use pulse_core::{Job, TaskExecution, ReplicationEvent, ClusterEvent};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    // Cluster management messages
    JoinCluster {
        node_info: pulse_core::NodeInfo,
    },
    LeaveCluster {
        node_id: Uuid,
        reason: String,
    },
    ClusterUpdate {
        event: ClusterEvent,
    },
    
    // Job management messages
    SubmitJob {
        job: Job,
    },
    JobStatusUpdate {
        job_id: Uuid,
        status: pulse_core::JobStatus,
    },
    
    // Task execution messages
    AssignTask {
        execution: TaskExecution,
    },
    TaskStatusUpdate {
        execution: TaskExecution,
    },
    TaskCompleted {
        execution: TaskExecution,
    },
    
    // Storage replication messages
    ReplicateEvent {
        event: ReplicationEvent,
    },
    SyncRequest {
        since: chrono::DateTime<chrono::Utc>,
    },
    SyncResponse {
        events: Vec<ReplicationEvent>,
    },
    
    // Discovery messages
    PeerDiscovery {
        node_id: Uuid,
        capabilities: pulse_core::NodeCapabilities,
    },
    
    // Health check messages
    Ping {
        timestamp: chrono::DateTime<chrono::Utc>,
    },
    Pong {
        timestamp: chrono::DateTime<chrono::Utc>,
        original_timestamp: chrono::DateTime<chrono::Utc>,
    },
    
    // Error messages
    Error {
        message: String,
        code: Option<u32>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub id: Uuid,
    pub from: Uuid,
    pub to: Option<Uuid>, // None for broadcast messages
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub message_type: String,
    pub payload: Message,
    pub requires_response: bool,
    pub correlation_id: Option<Uuid>, // For request/response pairs
}

impl MessageEnvelope {
    pub fn new(from: Uuid, payload: Message) -> Self {
        Self {
            id: Uuid::new_v4(),
            from,
            to: None,
            timestamp: chrono::Utc::now(),
            message_type: Self::get_message_type(&payload),
            payload,
            requires_response: false,
            correlation_id: None,
        }
    }

    pub fn to_peer(mut self, to: Uuid) -> Self {
        self.to = Some(to);
        self
    }

    pub fn broadcast(self) -> Self {
        // to remains None for broadcast
        self
    }

    pub fn with_response_required(mut self) -> Self {
        self.requires_response = true;
        self
    }

    pub fn as_response(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    fn get_message_type(message: &Message) -> String {
        match message {
            Message::JoinCluster { .. } => "join_cluster".to_string(),
            Message::LeaveCluster { .. } => "leave_cluster".to_string(),
            Message::ClusterUpdate { .. } => "cluster_update".to_string(),
            Message::SubmitJob { .. } => "submit_job".to_string(),
            Message::JobStatusUpdate { .. } => "job_status_update".to_string(),
            Message::AssignTask { .. } => "assign_task".to_string(),
            Message::TaskStatusUpdate { .. } => "task_status_update".to_string(),
            Message::TaskCompleted { .. } => "task_completed".to_string(),
            Message::ReplicateEvent { .. } => "replicate_event".to_string(),
            Message::SyncRequest { .. } => "sync_request".to_string(),
            Message::SyncResponse { .. } => "sync_response".to_string(),
            Message::PeerDiscovery { .. } => "peer_discovery".to_string(),
            Message::Ping { .. } => "ping".to_string(),
            Message::Pong { .. } => "pong".to_string(),
            Message::Error { .. } => "error".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageHandler<F> {
    pub message_type: String,
    pub handler: F,
}

pub trait MessageProcessor {
    fn process_message(&self, envelope: MessageEnvelope) -> impl std::future::Future<Output = crate::Result<Option<MessageEnvelope>>> + Send;
}

#[derive(Debug)]
pub struct MessageRouter {
    handlers: std::collections::HashMap<String, Box<dyn Fn(MessageEnvelope) -> futures::future::BoxFuture<'static, crate::Result<Option<MessageEnvelope>>> + Send + Sync>>,
}

impl MessageRouter {
    pub fn new() -> Self {
        Self {
            handlers: std::collections::HashMap::new(),
        }
    }

    pub fn register_handler<F, Fut>(&mut self, message_type: String, handler: F)
    where
        F: Fn(MessageEnvelope) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = crate::Result<Option<MessageEnvelope>>> + Send + 'static,
    {
        let boxed_handler = Box::new(move |envelope: MessageEnvelope| {
            Box::pin(handler(envelope)) as futures::future::BoxFuture<'static, crate::Result<Option<MessageEnvelope>>>
        });
        self.handlers.insert(message_type, boxed_handler);
    }

    pub async fn route_message(&self, envelope: MessageEnvelope) -> crate::Result<Option<MessageEnvelope>> {
        if let Some(handler) = self.handlers.get(&envelope.message_type) {
            handler(envelope).await
        } else {
            tracing::warn!("No handler found for message type: {}", envelope.message_type);
            Ok(None)
        }
    }
}

impl Default for MessageRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::NodeCapabilities;
    use std::collections::HashMap;

    #[test]
    fn test_message_envelope_creation() {
        let node_id = Uuid::new_v4();
        let capabilities = NodeCapabilities {
            max_concurrent_tasks: 10,
            available_executors: vec!["shell".to_string()],
            resource_limits: HashMap::new(),
            labels: HashMap::new(),
        };

        let message = Message::PeerDiscovery {
            node_id,
            capabilities,
        };

        let envelope = MessageEnvelope::new(node_id, message);
        
        assert_eq!(envelope.from, node_id);
        assert_eq!(envelope.message_type, "peer_discovery");
        assert_eq!(envelope.to, None);
        assert!(!envelope.requires_response);
    }

    #[test]
    fn test_message_envelope_to_peer() {
        let from_node = Uuid::new_v4();
        let to_node = Uuid::new_v4();
        
        let message = Message::Ping {
            timestamp: chrono::Utc::now(),
        };

        let envelope = MessageEnvelope::new(from_node, message).to_peer(to_node);
        
        assert_eq!(envelope.from, from_node);
        assert_eq!(envelope.to, Some(to_node));
        assert_eq!(envelope.message_type, "ping");
    }

    #[tokio::test]
    async fn test_message_router() {
        let mut router = MessageRouter::new();
        let response_message = Message::Pong {
            timestamp: chrono::Utc::now(),
            original_timestamp: chrono::Utc::now(),
        };
        
        router.register_handler("ping".to_string(), move |envelope| {
            let response = MessageEnvelope::new(envelope.to.unwrap_or(envelope.from), response_message.clone());
            async move { Ok(Some(response)) }
        });

        let ping_message = Message::Ping {
            timestamp: chrono::Utc::now(),
        };
        
        let node_id = Uuid::new_v4();
        let envelope = MessageEnvelope::new(node_id, ping_message).to_peer(node_id);
        
        let response = router.route_message(envelope).await.unwrap();
        assert!(response.is_some());
        
        let response_envelope = response.unwrap();
        match response_envelope.payload {
            Message::Pong { .. } => {},
            _ => panic!("Expected Pong message"),
        }
    }
}