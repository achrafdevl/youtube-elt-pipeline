source .venv/Scripts/activate

 docker run --rm -it -p 8080:8080 \
   -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
   -e TARGET_CHANNEL_ID="UC..." \
   -e YOUTUBE_API_KEY="AIza..." \
   -e JSON_OUTPUT_PATH="/usr/local/airflow/data/raw" \
   -v "$PWD":/usr/local/airflow \
    --password admin --firstname A --lastname D --role Admin --email a@b.c && airflow webserver & airflow scheduler"


astro dev start