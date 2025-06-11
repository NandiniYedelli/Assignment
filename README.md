# File Processor Cloud Run Service

This service processes file uploads from Cloud Storage and publishes metadata to Pub/Sub.

## Functionality
- Triggered by Cloud Storage object finalized events
- Extracts file name, size, and format
- Publishes information to Pub/Sub topic

## Environment Variables
- `my-assig-nan`: Name of the Pub/Sub topic to publish to
- `my-sub`: GCP project ID (automatically set by Cloud Run)
