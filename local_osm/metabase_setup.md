# Setting up Metabase to work with the Thai VPN
## Configuring the VPN
First, create a docker network that the VPN and Metabase will share:
```console
docker network create --subnet=172.20.0.0/16 fortinet
```

Then, start the VPN with the necessary credentials:
```console
docker run -it -d --restart unless-stopped --privileged --net fortinet --ip 172.20.0.2 \
  -e VPNADDR=<vpn address> \
  -e VPNUSER=<vpn user> \
  -e VPNPASS=<vpn password> \
  -e VPNTIMEOUT=60 \
  --name vpn auchandirect/forticlient
```

From there, the requests need to be routed through the VPN:
```console
ip route add 192.168.0.0/16 via 172.20.0.2
```

## Starting a PostgreSQL docker container
A PostgreSQL docker container needs to be set up as the backend for Metabase to store information about visualizations and dashboards. Note that Metabase can be set up without an external database and instead rely on an internal or external H2 database file, however Metabase recommends using a more stable database in production environments:

```console
docker run -d -p 5432:5432 --restart unless-stopped \
  -e POSTGRES_PASSWORD=<desired admin password> \
  --name metabase-postgres postgres
```

## Running Metabase
Finally, the Metabase container can be started:
```console
docker run -d -p 3000:3000 --restart unless-stopped --net fortinet \
  -e "MB_DB_TYPE=postgres" \
  -e "MB_DB_DBNAME=metabase" \
  -e "MB_DB_PORT=5432" \
  -e "MB_DB_USER=<username>" \
  -e "MB_DB_PASS=<password>" \
  -e "MB_DB_HOST=<database host>" \
  --name metabase metabase/metabase
```


# Miscellaneous
Below are a few commands that may come in handy if Metabase is already set up or if you want to set it up slightly differently than the process outlined above.

## Old commmand for running Metabase using an external H2 database file
```console
docker run -d -p 3000:3000 --restart unless-stopped --net fortinet \
  -v ~/metabase-data:/metabase-data \
  -e "MB_DB_FILE=/metabase-data/metabase.db" \
  --name metabase metabase/metabase
```
## Migrating Metabase H2 database to PostgreSQL
I wasn't able to get the migration script detailed [here](https://www.metabase.com/docs/latest/operations-guide/running-metabase-on-docker.html) to work due to issues linking the H2 file in the new docker container. Instead I started a new detached Metabase container as follows:
```console
docker run --rm -d --name metabase-migration \
    -v ~/metabase-data:/metabase-data \
    -e "MB_DB_FILE=/metabase-data/metabase.db" \
    -e "MB_DB_TYPE=postgres" \
    -e "MB_DB_DBNAME=metabase" \
    -e "MB_DB_PORT=5432" \
    -e "MB_DB_USER=<username>" \
    -e "MB_DB_PASS=<password>" \
    -e "MB_DB_HOST=<database host>" \
    metabase/metabase
```

*Note that running this attached can help to debug if there are issues connecting to the PostgreSQL container*

From there, I started a bash shell for the container:
```console
docker exec -it metabase-migration bash
```

Once there, I searched to make sure that the "metabase-data" folder I attached was visible and that I could find the .db file necessary to run the migration. I ran the migration from within the container with the following - note that <code>path/to/metabase.db</code> should point to the location of the <code>metabase.db.mv.db</code> file - in my case, this was <code>/metabase-data/metabase.db/metabase.db/metabase.db</code>:
```console
java -jar /app/metabase.jar load-from-h2 /path/to/metabase.db
```

## Updating Metabase versions
The following commands can be used to upgrade an already running Metabase instance on Docker. The Metabase documentation for this process can be found here: https://www.metabase.com/docs/latest/operations-guide/upgrading-metabase.html#upgrading-the-docker-image

First, pull the latest Metabase docker image:
```console
docker pull metabase/metabase:latest
```

Stop the running instance (named 'metabase' here):
```console
docker stop metabase
```

Remove the container:
```console
docker rm metabase
```

Recreate the container:
```console
docker run -d -p 3000:3000 --restart unless-stopped --net fortinet \
  -e "MB_DB_TYPE=postgres" \
  -e "MB_DB_DBNAME=metabase" \
  -e "MB_DB_PORT=5432" \
  -e "MB_DB_USER=<username>" \
  -e "MB_DB_PASS=<password>" \
  -e "MB_DB_HOST=<database host>" \
  --name metabase metabase/metabase
```