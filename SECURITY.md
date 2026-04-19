# Security

Please report security issues privately before opening a public issue.

## Crawl policy

- allowlist-only crawl manifests are the supported model
- each manifest must declare robots.txt, terms URL, user-agent, and rate limit
- raw fetched HTML stays under `.storage/raw/` and should not be committed
- crawler failures must not become a required path for API or worker correctness
- parser failures should be reported through crawl audit tables instead of being dropped silently
