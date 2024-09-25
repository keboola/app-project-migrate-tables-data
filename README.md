# Project Migration - Tables Data

[![GitHub Actions](https://github.com/keboola/app-project-migrate-tables-data/actions/workflows/push.yml/badge.svg)](https://github.com/keboola/app-project-migrate-tables-data/actions/workflows/push.yml)

This application is used to migrate Storage data from one project to another. It is designed to be used in the Keboola Connection environment.

## Usage
The application migrates tables data from one project to another. If a bucket does not exist, it will be created. Similarly, if a table does not exist, it will be created.

### SAPI mode
In SAPI mode, the application migrates data using the Storage API of Keboola Connection.

### Database mode
In Database mode, the application migrates data directly between databases. This mode is significantly faster. Please note the following conditions:
- Replication from the source database must be allowed.
- Both Snowflake accounts must be in the same organization.
- The Snowflake user must have the `ACCOUNTADMIN` role.

For allowing replication, run the following SQL in source Snowflake account:
```sql
ALTER DATABASE {{SOURCE_DATABASE_NAME}} ENABLE REPLICATION TO ACCOUNTS {{DESTINATION_ACCOUNT_REGION}}.{{DESTINATION_ACCOUNT_NAME}};
```

If these conditions are not fulfilled, please use SAPI mode.

## Configuration
The configuration `config.json` contains following properties:

- `parameters` - object (required): Configuration of the application
    - `mode` - string (optional): `sapi`(default value) or `database`
    - `dryRun` - boolean (optional): If set to `true`, the application will only simulate the migration.
    - `sourceKbcUrl` - string (required): URL of the source project
    - `sourceKbcToken` - string (required): Storage API token of the source project
    - `tables` - array (optional): List of tables to migrate. If not set, all tables will be migrated.
    - `db` - object (optional in `database` mode and extends `db` object in `image_parameters`):
        - `host` - string (required): Snowflake host
        - `username` - string (required): Snowflake username
        - `#password` - string (required): Snowflake password
        - `warehouse` - string (required): Snowflake warehouse
- `image_parameters` - object (require): Includes from Developer Portal
    - `db` - object (required in `database` mode):
        - `host` - string (required): Snowflake host
        - `username` - string (required): Snowflake username
        - `#password` - string (required): Snowflake password
        - `warehouse` - string (required): Snowflake warehouse

## Example of Configurations

### DRY run

```json
{
  "parameters": {
    "mode": "sapi",
    "dryRun": true,
    "sourceKbcUrl": "https://connection.keboola.com/",
    "#sourceKbcToken": "SOURCE_KBC_TOKEN"
  }
}
```

### SAPI mode

```json
{
  "parameters": {
    "mode": "sapi",
    "sourceKbcUrl": "https://connection.keboola.com/",
    "#sourceKbcToken": "SOURCE_KBC_TOKEN"
  }
}
```

### Database mode

```json
{
  "parameters": {
    "mode": "database",
    "sourceKbcUrl": "https://connection.keboola.com/",
    "#sourceKbcToken": "SOURCE_KBC_TOKEN"
  }
}
```

### Database mode with your own Snowflake

```json
{
  "parameters": {
    "mode": "database",
    "sourceKbcUrl": "https://connection.keboola.com/",
    "#sourceKbcToken": "SOURCE_KBC_TOKEN",
    "db": {
      "host": "SNOWFLAKE_HOST",
      "username": "SNOWFLAKE_USERNAME",
      "#password": "SNOWFLAKE_PASSWORD",
      "warehouse": "SNOWFLAKE_WAREHOUSE"
    }
  }
}
```

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/app-project-migrate-tables-data
cd app-project-migrate-tables-data
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Save .env file with following content:

```
SOURCE_CLIENT_URL=
SOURCE_CLIENT_TOKEN=
DESTINATION_CLIENT_URL=
DESTINATION_CLIENT_TOKEN=
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```