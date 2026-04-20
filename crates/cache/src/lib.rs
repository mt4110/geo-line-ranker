use anyhow::{Context, Result};
use redis::{aio::MultiplexedConnection, Client};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};

const RECOMMENDATION_PREFIX: &str = "geo-line-ranker:recommendations";

#[derive(Debug, Clone)]
pub struct RecommendationCache {
    redis_url: Option<String>,
    ttl_secs: u64,
}

impl RecommendationCache {
    pub fn new(redis_url: Option<String>, ttl_secs: u64) -> Self {
        Self {
            redis_url,
            ttl_secs,
        }
    }

    pub fn enabled(&self) -> bool {
        self.redis_url.is_some()
    }

    pub fn build_key<T: Serialize>(
        &self,
        profile_version: &str,
        algorithm_version: &str,
        candidate_mode: &str,
        candidate_limit: usize,
        request: &T,
    ) -> Result<String> {
        let serialized =
            serde_json::to_vec(request).context("failed to serialize cache request")?;
        let mut digest = Sha256::new();
        digest.update(&serialized);
        let request_hash = format!("{:x}", digest.finalize());
        Ok(format!(
            "{RECOMMENDATION_PREFIX}:{profile_version}:{algorithm_version}:{candidate_mode}:{candidate_limit}:{request_hash}"
        ))
    }

    pub async fn get_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let Some(mut connection) = self.connection().await? else {
            return Ok(None);
        };
        let cached: Option<String> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut connection)
            .await?;
        cached
            .map(|raw| serde_json::from_str(&raw).context("failed to deserialize cached payload"))
            .transpose()
    }

    pub async fn set_json<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let Some(mut connection) = self.connection().await? else {
            return Ok(());
        };
        let serialized =
            serde_json::to_string(value).context("failed to serialize cached payload")?;
        let _: () = redis::cmd("SETEX")
            .arg(key)
            .arg(self.ttl_secs)
            .arg(serialized)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    pub async fn invalidate_recommendations(&self) -> Result<usize> {
        self.delete_matching(&format!("{RECOMMENDATION_PREFIX}:*"))
            .await
    }

    pub async fn status(&self) -> String {
        match self.ping().await {
            Ok(status) => status,
            Err(error) => format!("degraded ({error})"),
        }
    }

    async fn ping(&self) -> Result<String> {
        let Some(mut connection) = self.connection().await? else {
            return Ok("disabled".to_string());
        };
        let response: String = redis::cmd("PING").query_async(&mut connection).await?;
        Ok(if response == "PONG" {
            "reachable".to_string()
        } else {
            response
        })
    }

    async fn delete_matching(&self, pattern: &str) -> Result<usize> {
        let Some(mut connection) = self.connection().await? else {
            return Ok(0);
        };
        let mut deleted = 0_usize;
        let mut cursor = 0_u64;

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut connection)
                .await?;
            if !keys.is_empty() {
                let removed: usize = redis::cmd("DEL")
                    .arg(&keys)
                    .query_async(&mut connection)
                    .await?;
                deleted += removed;
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(deleted)
    }

    async fn connection(&self) -> Result<Option<MultiplexedConnection>> {
        let Some(redis_url) = self.redis_url.as_deref() else {
            return Ok(None);
        };
        let client = Client::open(redis_url).context("failed to create redis client")?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .context("failed to connect to redis")?;
        Ok(Some(connection))
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::RecommendationCache;

    #[derive(Serialize)]
    struct Request<'a> {
        target_station_id: &'a str,
        placement: &'a str,
        limit: usize,
    }

    #[test]
    fn cache_key_contains_profile_and_algorithm_versions() {
        let cache = RecommendationCache::new(Some("redis://127.0.0.1:6379".to_string()), 60);
        let key = cache
            .build_key(
                "profile-123",
                "algo-456",
                "sql_only",
                256,
                &Request {
                    target_station_id: "st_tamachi",
                    placement: "home",
                    limit: 3,
                },
            )
            .expect("cache key");

        assert!(key.contains("profile-123"));
        assert!(key.contains("algo-456"));
        assert!(key.contains("sql_only"));
    }

    #[test]
    fn placement_changes_cache_key_hash() {
        let cache = RecommendationCache::new(Some("redis://127.0.0.1:6379".to_string()), 60);
        let home_key = cache
            .build_key(
                "profile-123",
                "algo-456",
                "sql_only",
                256,
                &Request {
                    target_station_id: "st_tamachi",
                    placement: "home",
                    limit: 3,
                },
            )
            .expect("home cache key");
        let search_key = cache
            .build_key(
                "profile-123",
                "algo-456",
                "sql_only",
                256,
                &Request {
                    target_station_id: "st_tamachi",
                    placement: "search",
                    limit: 3,
                },
            )
            .expect("search cache key");

        assert_ne!(home_key, search_key);
    }

    #[test]
    fn candidate_limit_changes_cache_key() {
        let cache = RecommendationCache::new(Some("redis://127.0.0.1:6379".to_string()), 60);
        let small_limit_key = cache
            .build_key(
                "profile-123",
                "algo-456",
                "sql_only",
                128,
                &Request {
                    target_station_id: "st_tamachi",
                    placement: "search",
                    limit: 3,
                },
            )
            .expect("small limit cache key");
        let large_limit_key = cache
            .build_key(
                "profile-123",
                "algo-456",
                "sql_only",
                256,
                &Request {
                    target_station_id: "st_tamachi",
                    placement: "search",
                    limit: 3,
                },
            )
            .expect("large limit cache key");

        assert_ne!(small_limit_key, large_limit_key);
    }
}
