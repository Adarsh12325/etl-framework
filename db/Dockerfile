FROM postgres:15-alpine

# Copy seed SQL files into the image
COPY seeds/01_schema.sql /seeds/01_schema.sql
COPY seeds/02_seed_data.sql /seeds/02_seed_data.sql

# Copy our custom init script into docker-entrypoint-initdb.d
# This directory IS executed on first run by the official image entrypoint.
# We also copy it to /always-init/ and run it from the healthcheck on
# subsequent starts via the POSTGRES_INITSCRIPTS trick below.
COPY init-db.sh /docker-entrypoint-initdb.d/init-db.sh
RUN chmod +x /docker-entrypoint-initdb.d/init-db.sh