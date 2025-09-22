FROM astrocrpublic.azurecr.io/runtime:3.0-10
ENV SODA_PROJECT_DIR=/usr/local/airflow/soda \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# Ensure updated Soda configs are available in the container
COPY soda/ /usr/local/airflow/soda/
