use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Online,
    Offline,
    Draining,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: Uuid,
    pub address: String,
    pub status: NodeStatus,
    pub capabilities: NodeCapabilities,
    pub last_seen: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub max_concurrent_tasks: usize,
    pub available_executors: Vec<String>,
    pub resource_limits: HashMap<String, u64>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterEvent {
    NodeJoined {
        node: NodeInfo,
    },
    NodeLeft {
        node_id: Uuid,
        reason: String,
    },
    NodeUpdated {
        node: NodeInfo,
    },
    TaskAssigned {
        task_id: Uuid,
        node_id: Uuid,
    },
    TaskCompleted {
        task_id: Uuid,
        node_id: Uuid,
    },
    LeaderElected {
        node_id: Uuid,
    },
}

#[async_trait]
pub trait ClusterManager: Send + Sync {
    async fn join_cluster(&self, node_info: &NodeInfo) -> Result<()>;
    async fn leave_cluster(&self) -> Result<()>;
    async fn get_cluster_nodes(&self) -> Result<Vec<NodeInfo>>;
    async fn get_node(&self, node_id: &Uuid) -> Result<Option<NodeInfo>>;
    async fn update_node_status(&self, node_id: &Uuid, status: NodeStatus) -> Result<()>;
    async fn is_leader(&self) -> Result<bool>;
    async fn get_leader(&self) -> Result<Option<Uuid>>;
    async fn broadcast_event(&self, event: &ClusterEvent) -> Result<()>;
    async fn elect_leader(&self) -> Result<()>;
}

#[async_trait]
pub trait NodeScheduler: Send + Sync {
    async fn select_node_for_task(
        &self,
        task_requirements: &TaskRequirements,
        available_nodes: &[NodeInfo],
    ) -> Result<Option<Uuid>>;
    
    async fn can_schedule_task(
        &self,
        node: &NodeInfo,
        task_requirements: &TaskRequirements,
    ) -> Result<bool>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequirements {
    pub required_executors: Vec<String>,
    pub resource_requirements: HashMap<String, u64>,
    pub node_selector: Option<HashMap<String, String>>,
    pub affinity_rules: Vec<AffinityRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AffinityRule {
    PreferNode { node_id: Uuid },
    AvoidNode { node_id: Uuid },
    RequireLabel { key: String, value: String },
    PreferLabel { key: String, value: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub nodes: HashMap<Uuid, NodeInfo>,
    pub leader: Option<Uuid>,
    pub last_updated: DateTime<Utc>,
    pub cluster_version: u64,
}

impl ClusterState {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            leader: None,
            last_updated: Utc::now(),
            cluster_version: 0,
        }
    }

    pub fn add_node(&mut self, node: NodeInfo) {
        self.nodes.insert(node.id, node);
        self.last_updated = Utc::now();
        self.cluster_version += 1;
    }

    pub fn remove_node(&mut self, node_id: &Uuid) {
        self.nodes.remove(node_id);
        if self.leader == Some(*node_id) {
            self.leader = None;
        }
        self.last_updated = Utc::now();
        self.cluster_version += 1;
    }

    pub fn update_node(&mut self, node: NodeInfo) {
        self.nodes.insert(node.id, node);
        self.last_updated = Utc::now();
        self.cluster_version += 1;
    }

    pub fn set_leader(&mut self, node_id: Uuid) {
        self.leader = Some(node_id);
        self.last_updated = Utc::now();
        self.cluster_version += 1;
    }

    pub fn get_healthy_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|node| node.status == NodeStatus::Online)
            .collect()
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}