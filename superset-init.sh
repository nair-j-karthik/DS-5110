#!/bin/bash

# Upgrade the metadata database
superset db upgrade

# Create an admin user for Superset
superset fab create-admin \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com

# Initialize Superset (create default roles and permissions)
superset init
