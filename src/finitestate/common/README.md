# Snapshot manager

The snapshot manager is a service that acts as a way to version data that's associated with a given entity.
Example: A device's network observations at a given point in time.
One version of the device's snapshot could show it communicating over HTTP and DNS with little usable data, but a newer snapshot version associated with the device could show that it's now only communicating over HTTP with substantially more data captured under HTTP.

## Goals

- Temporal lookup. Data is versioned to allow for user to see changes over time. (ex. Device network observations version 3)
- Scalable storage + queries with little to no management overhead

## Components

The technical nitty-gritty associated with implementing the Snapshot manager.

### Data Storage

How snapshots live in the cloud

- AWS S3
  - Stores the actual data for a given snapshot version
- AWS Glue tables
  - Writes a trigger to SNS/SQS
- AWS S3 + DynamoDb
  - DyanmoDB used as a lookup table for a given object in S3.

### Data Processing

Services that are called to create new snapshots in DynamoDB + S3.

- AWS Lambda (Example: Lambda is triggered whenever we get new network protocol observations for a given device) that can write to metadata to DynamoDB and the payload to S3
- AWS Glue: Glue jobs execute to generate S3 output and metadata in DynamoDB

### Messaging (triggered by services above)

- AWS SNS/SQS
