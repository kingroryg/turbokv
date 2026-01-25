//! # Configuration Management
//!
//! Handles all configuration for HanshiroDB components.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub index: IndexConfig,
    pub ingestion: IngestionConfig,
    pub api: ApiConfig,
    pub security: SecurityConfig,
    pub monitoring: MonitoringConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            index: IndexConfig::default(),
            ingestion: IngestionConfig::default(),
            api: ApiConfig::default(),
            security: SecurityConfig::default(),
            monitoring: MonitoringConfig::default(),
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub max_wal_size: u64,
    pub memtable_size: u64,
    pub sstable_size: u64,
    pub compaction_threshold: u32,
    pub compression: CompressionConfig,
    pub sync_writes: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            wal_dir: PathBuf::from("./data/wal"),
            max_wal_size: 1024 * 1024 * 1024, // 1GB
            memtable_size: 256 * 1024 * 1024,  // 256MB
            sstable_size: 512 * 1024 * 1024,   // 512MB
            compaction_threshold: 4,
            compression: CompressionConfig::default(),
            sync_writes: true,
        }
    }
}

/// Compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub level: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Zstd,
            level: 3,
        }
    }
}

/// Compression algorithms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Lz4,
    Zstd,
    Snappy,
}

/// Index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub vector: VectorIndexConfig,
    pub metadata: MetadataIndexConfig,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            vector: VectorIndexConfig::default(),
            metadata: MetadataIndexConfig::default(),
        }
    }
}

/// Vector index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    pub algorithm: VectorIndexAlgorithm,
    pub dimension: u16,
    pub max_neighbors: u32,
    pub ef_construction: u32,
    pub ef_search: u32,
    pub disk_ann: DiskAnnConfig,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            algorithm: VectorIndexAlgorithm::DiskAnn,
            dimension: 768,
            max_neighbors: 32,
            ef_construction: 200,
            ef_search: 100,
            disk_ann: DiskAnnConfig::default(),
        }
    }
}

/// Vector index algorithms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VectorIndexAlgorithm {
    Hnsw,
    DiskAnn,
    Flat,
}

/// DiskANN specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskAnnConfig {
    pub graph_degree: u32,
    pub beam_width: u32,
    pub alpha: f32,
    pub num_pq_chunks: u32,
    pub nodes_per_sector: u32,
}

impl Default for DiskAnnConfig {
    fn default() -> Self {
        Self {
            graph_degree: 64,
            beam_width: 128,
            alpha: 1.2,
            num_pq_chunks: 256,
            nodes_per_sector: 4096,
        }
    }
}

/// Metadata index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataIndexConfig {
    pub max_values_per_key: u32,
    pub cache_size: u64,
}

impl Default for MetadataIndexConfig {
    fn default() -> Self {
        Self {
            max_values_per_key: 10000,
            cache_size: 128 * 1024 * 1024, // 128MB
        }
    }
}

/// Ingestion configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionConfig {
    pub batch_size: u32,
    pub batch_timeout: Duration,
    pub max_event_size: u64,
    pub vectorization: VectorizationConfig,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_timeout: Duration::from_millis(100),
            max_event_size: 10 * 1024 * 1024, // 10MB
            vectorization: VectorizationConfig::default(),
        }
    }
}

/// Vectorization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorizationConfig {
    pub model_name: String,
    pub model_path: PathBuf,
    pub batch_size: u32,
    pub cache_size: u32,
}

impl Default for VectorizationConfig {
    fn default() -> Self {
        Self {
            model_name: "all-MiniLM-L6-v2".to_string(),
            model_path: PathBuf::from("./models"),
            batch_size: 64,
            cache_size: 10000,
        }
    }
}

/// API configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub grpc_port: u16,
    pub http_port: u16,
    pub max_request_size: u64,
    pub timeout: Duration,
    pub rate_limit: RateLimitConfig,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            grpc_port: 9090,
            http_port: 8080,
            max_request_size: 100 * 1024 * 1024, // 100MB
            timeout: Duration::from_secs(30),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub enabled: bool,
    pub requests_per_second: u32,
    pub burst_size: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            requests_per_second: 1000,
            burst_size: 2000,
        }
    }
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub tls: TlsConfig,
    pub auth: AuthConfig,
    pub encryption: EncryptionConfig,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            tls: TlsConfig::default(),
            auth: AuthConfig::default(),
            encryption: EncryptionConfig::default(),
        }
    }
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: Option<PathBuf>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cert_path: PathBuf::from("./certs/server.crt"),
            key_path: PathBuf::from("./certs/server.key"),
            ca_path: None,
        }
    }
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub enabled: bool,
    pub method: AuthMethod,
    pub token_expiry: Duration,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            method: AuthMethod::ApiKey,
            token_expiry: Duration::from_secs(3600),
        }
    }
}

/// Authentication methods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    ApiKey,
    Jwt,
    Mtls,
}

/// Encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub at_rest: bool,
    pub algorithm: EncryptionAlgorithm,
    pub key_rotation_days: u32,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            at_rest: true,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_rotation_days: 90,
        }
    }
}

/// Encryption algorithms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    Aes256Gcm,
    ChaCha20Poly1305,
}

/// Monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
    pub tracing: TracingConfig,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
            tracing: TracingConfig::default(),
        }
    }
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub interval: Duration,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "0.0.0.0:9091".to_string(),
            interval: Duration::from_secs(10),
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: LogLevel,
    pub format: LogFormat,
    pub file: Option<PathBuf>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Json,
            file: None,
        }
    }
}

/// Log levels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Log formats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogFormat {
    Text,
    Json,
}

/// Tracing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub sample_rate: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "localhost:4317".to_string(),
            sample_rate: 0.1,
        }
    }
}