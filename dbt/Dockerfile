FROM python:3.9-slim

# Install dbt with postgres adapter
RUN pip install --no-cache-dir \
    dbt-core==1.7.4 \
    dbt-postgres==1.7.4

# Set working directory
WORKDIR /usr/app/dbt

# Create dbt user
RUN useradd -m -s /bin/bash dbtuser
USER dbtuser

# Set environment
ENV DBT_PROFILES_DIR=/usr/app/dbt

ENTRYPOINT ["dbt"]
