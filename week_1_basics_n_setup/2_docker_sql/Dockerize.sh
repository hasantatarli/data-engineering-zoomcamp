'''Docker Network Oluşturma '''
docker network create pg-network
9d567df66816cd5801077b7f6416eed9821276629b5ca2e7e6945f8f409c82e2

 ''' PostgreSQL Docker Run with NetworkName'''
winpty docker run -it \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="ny_taxi" \
-v /C/Users/TTL/git/data-engineering-zoomcamp/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
-p 5432:5432 \
--network=pg-network \
--name pg-database \
postgres:13

'''PGADMIN Docker Run with Network Name'''
winpty docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
--network=pg-network \
--name pgadmin-2 \
dpage/pgadmin4

''' Data Ingestion with Manually in Docker'''
URL="http://192.168.184.1:8000/yellow_tripdata_2021-01.csv"
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}

''' Data Ingestion with Manually in Docker'''
URL="http://192.168.184.1:8000/yellow_tripdata_2021-01.csv"
python ingest_data_2.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}    

''' Data Ingestion With Dockerize  '''
URL="http://192.168.184.1:8000/yellow_tripdata_2021-01.csv"
winpty docker run -it \
  --network=2_docker_sql_default \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}


''' Data Ingestion 2 with Manually in Docker'''
URL="http://192.168.184.1:8000/taxi+_zone_lookup.csv"
python ingest_data_2.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=zones \
    --url=${URL}  