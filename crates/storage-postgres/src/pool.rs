use anyhow::{Context, Result};
use config::parse_postgres_pool_max_size;
use deadpool_postgres::{
    Client, Config as PgPoolConfig, ManagerConfig, Pool, PoolConfig, RecyclingMethod, Runtime,
};
use tokio::sync::OnceCell;
use tokio_postgres::NoTls;

pub(crate) fn build_pool_config(database_url: String, max_size: usize) -> PgPoolConfig {
    let mut config = PgPoolConfig::new();
    config.url = Some(database_url);
    config.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    config.pool = Some(PoolConfig::new(max_size));
    config
}

pub(crate) fn load_postgres_pool_max_size() -> usize {
    let configured = std::env::var("POSTGRES_POOL_MAX_SIZE").ok();
    parse_postgres_pool_max_size(configured.as_deref())
}

pub(crate) async fn connect(pool: &OnceCell<Pool>, pool_config: PgPoolConfig) -> Result<Client> {
    let pool = pool
        .get_or_try_init(move || async move {
            pool_config
                .create_pool(Some(Runtime::Tokio1), NoTls)
                .with_context(|| "failed to create PostgreSQL connection pool")
        })
        .await?;

    pool.get()
        .await
        .with_context(|| "failed to get PostgreSQL connection from pool")
}

#[cfg(test)]
mod tests {
    use config::{parse_postgres_pool_max_size, DEFAULT_POSTGRES_POOL_MAX_SIZE};

    #[test]
    fn parse_postgres_pool_max_size_uses_default_when_unset() {
        assert_eq!(
            parse_postgres_pool_max_size(None),
            DEFAULT_POSTGRES_POOL_MAX_SIZE
        );
    }

    #[test]
    fn parse_postgres_pool_max_size_accepts_positive_values() {
        assert_eq!(parse_postgres_pool_max_size(Some("32")), 32);
        assert_eq!(parse_postgres_pool_max_size(Some(" 8 ")), 8);
    }

    #[test]
    fn parse_postgres_pool_max_size_rejects_invalid_values() {
        assert_eq!(
            parse_postgres_pool_max_size(Some("0")),
            DEFAULT_POSTGRES_POOL_MAX_SIZE
        );
        assert_eq!(
            parse_postgres_pool_max_size(Some("invalid")),
            DEFAULT_POSTGRES_POOL_MAX_SIZE
        );
    }
}
