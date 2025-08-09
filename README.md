### Description
Basic queue stream distributed system using redis and tokio

### Requirements
- Docker (to install redis)
- Redis
- Rust - MSRV 1.85

### Setup
.env file - replace REDIS_URL with your connection info
```
REDIS_URL="redis://:test123@localhost:6379/"
```

```
> $ docker compose up -d
> $ cargo run
```