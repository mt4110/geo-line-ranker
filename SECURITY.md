# Security

Please report security issues privately before opening a public issue.

## Crawl policy

- allowlist-only crawl manifests are the supported model
- default Docker Compose files are for local development only and must not be exposed directly to the public internet
- each manifest must declare robots.txt, terms URL, user-agent, and rate limit
- raw fetched HTML stays under `.storage/raw/` and should not be committed
- crawler failures must not become a required path for API or worker correctness
- parser failures should be reported through crawl audit tables instead of being dropped silently

## Privacy boundary

- recommendation and context traces must not store raw address, name, email, phone number, precise device GPS, or unredacted external account payloads
- user identifiers in context traces should be hashed or otherwise minimized by default
- tracking timestamps must be RFC3339 when supplied by clients
