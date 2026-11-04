# NileMart Azure Container Apps Streaming Starter

Educational cloud-first Kafka-compatible Event Hubs producer and consumer. **Events are simulated and NOT read from/written to the four Azure SQL databases.**

- `producer.py`: cycles across four Event Hubs, emitting one synthetic event every 5 seconds by default.
- `consumer.py`: reads all four hubs using a Kafka consumer group and prints partition/offset before committing. In this first lab it does **not** write to Fabric; container logs are the output.
- `Dockerfile`: shared image, default command runs producer. A second Container App can override the command to `python consumer.py`.

## Setup

1. Create an Event Hubs **Standard** namespace and four Event Hubs: `commerce-events`, `supply-events`, `people-events`, `finance-events`.
2. Create a namespace-level SAS policy with Send and Listen, not Manage. Never commit or share its connection string.
3. Upload this ZIP to Azure Cloud Shell (Bash), run `unzip nilemart-cloud-streaming.zip -d nilemart-cloud-streaming && cd nilemart-cloud-streaming`.
4. Sign in with `az account show` (Cloud Shell), choose subscription and create RG, then deploy with `az containerapp up --name nilemart-stream-producer --resource-group rg-nilemart-streaming-dev --location <region> --source . --ingress disabled` (check your Azure CLI's up parameters; omit `--ingress` if disabled isn't a supported value). This creates billable resources (registry, environment and logging). Verify what is created.
5. Add the connection string as a Container Apps secret via CLI; reference it via `secretref:eh-connection` in `EVENTHUB_CONNECTION_STRING`. Set `EVENTHUB_NAMESPACE` environment variable. Configure `minReplicas=1`, `maxReplicas=1`, and ingress off for background operation. Without `minReplicas=1` a non-ingress app can scale to zero.
6. Monitor `az containerapp logs show --name nilemart-stream-producer --resource-group rg-nilemart-streaming-dev --follow` and Event Hubs incoming messages. Never print credentials.
7. Stop the app with `az containerapp update --name nilemart-stream-producer --resource-group rg-nilemart-streaming-dev --min-replicas 0 --max-replicas 1` (with no ingress or scaling trigger it can scale to zero), or delete all lab resources if no longer needed.

## Important

- Event Hubs Standard TU, Container Apps compute, registry and Log Analytics can incur charges independently.
- A producer receiving a delivery callback with error is not successful delivery.
- For production replace direct synthetic events with verified Azure SQL commits + CDC/outbox connector, design idempotent processing, monitoring, and access control.
