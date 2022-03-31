# Kafka

The server may be configured to send notifications on events to Kafka - see the `deploy.cfg` file
for information. The events and their respective JSON message formats are:

## New sample or sample version

```json
{
    "event_type": "NEW_SAMPLE",
    "sample_id": "<sample ID>",
    "sample_ver": "<sample version>"
 }
```

## Sample ACL change

```json
{
    "event_type": "ACL_CHANGE",
    "sample_id": "<sample ID>"
 }
```

## New data link

```json
{
    "event_type": "NEW_LINK",
    "link_id": "<link ID>"
 }
```

## Expired data link

```json
{
    "event_type": "EXPIRED_LINK",
    "link_id": "<link ID>"
 }
```
