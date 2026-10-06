# Modified Messages API

Fork of [eclipse-aerios/iota-messages-api](https://github.com/eclipse-aerios/iota-messages-api) (Apache-2.0).
After writing a block to Hornet it forwards `{block_id, tag, message}` to the auditor.

```bash
docker build -t iota_api_audit messages_api
docker rm -f iota_api
docker run -d --name iota_api --network iota-net -p 5555:5555 \
  --add-host=host.docker.internal:host-gateway \
  -e AUDITOR_URL=http://host.docker.internal:8000/api/ingest iota_api_audit
```
