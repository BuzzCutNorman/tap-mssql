# tap-mssql

`tap-mssql` is a Singer tap for mssql. !!! Warning !!! work in progress. It works ok 😐 for full loads. You can setup a batch_config and generate batch messages.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

### Whats New 🛳️🎉
**2023-02-08 Higher Defined(HD) JSON Schema types:**  This is just my interpretation of how to convert some MS SQL data types into JSON Schema definitions.  You can give it a try by setting `hd_jsonschema_types` to `True` in your config.json or meltano.yml

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.
-->
## Installation

### Prerequisites
You will need to install the SQL Server Native Driver or ODBC Driver for SQL Server if you plan  to use the `driver_type` of `pyodbc`. These drivers are not needed when opting to use `pymssql`.

[Installing Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/windows/system-requirements-installation-and-driver-files?view=sql-server-ver16#installing-microsoft-odbc-driver-for-sql-server)
<!--
Install from PyPi:

```bash
pipx install tap-mssql
```
-->
### Install from GitHub:

```bash
pipx install git+https://github.com/BuzzCutNorman/tap-mssql.git
```

### Meltano CLI

You can find this tap at [Meltano Hub](https://hub.meltano.com).  Which makes installation a snap.

Add the tap-stackoverflow-sampledata extractor to your project using meltano add :
```bash
meltano add extractor tap-mssql --variant buzzcutnorman
```

## Configuration

The simplest way to configure tap-mssql is to use the Meltano interactive configuration.

```bash
meltano config tap-mssql set --interactive
```

You can quickly set configuration options 1 - 7 this way: 
1. **dialect:** The Dialect of SQLAlchemy
2. **driver_type:** The Python Driver you will be using to connect to the SQL server
3. **host:** The FQDN of the Host serving out the SQL Instance
4. **port:** The port on which SQL awaiting connection
5. **user:** The User Account who has been granted access to the SQL Server
6. **password:** The Password for the User account
7. **database:** The Default database for this connection

**WARNING:** Do not attempt setting any other configuration options via interactive.  Doing so has lead to incomplete configurations that fail when the tap is run.

Options 8 - 15 can be setup via `meltano config tap-mssql set`.  Examples for the most commonly needed configurations options are given below.

When using `pyodbc` `sqlalchemy_url_query.driver` passes SQLAlchemny the installed ODBC driver. 
```bash
meltano config target-mssql set sqlalchemy_url_query.driver "ODBC Driver 18 for SQL Server"
```

When using `pyodbc` `sqlalchemy_url_query.TrustServerCertificate` let SQLAlchemy know whether to trust server signed certificates when connecting to SQL Server.
```bash
meltano config target-mssql set sqlalchemy_url_query.TrustServerCertificate yes
```

The `pyodbc` driver has added support for a “fast executemany” mode of execution which greatly reduces round trips.  You can trun the option on or off by setting `sqlalchemy_eng_params.fast_executemany` to `"True"` or `"False"`
```bash
meltano config target-mssql set sqlalchemy_eng_params.fast_executemany "True"
```
### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-mssql --about --format=markdown
```
-->


| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| dialect             | False    | None    | The Dialect of SQLAlchamey |
| driver_type         | False    | None    | The Python Driver you will be using to connect to the SQL server (either "pyodbc" or "pymssql") |
| host                | False    | None    | The FQDN of the Host serving out the SQL Instance |
| port                | False    | None    | The port on which SQL awaiting connection |
| user                | False    | None    | The User Account who has been granted access to the SQL Server |
| password            | False    | None    | The Password for the User account |
| database            | False    | None    | The Default database for this connection |
| sqlalchemy_eng_params| False    | None    | SQLAlchemy Engine Paramaters: fast_executemany, future |
| sqlalchemy_url_query| False    | None    | SQLAlchemy URL Query options: driver, TrustServerCertificate |
| batch_config        | False    | None    | Optional Batch Message configuration |
| start_date          | False    | None    | The earliest record date to sync |
| hd_jsonschema_types | False    |       0 | Turn on Higher Defined(HD) JSON Schema types to assist Targets |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-mssql --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.
<!--
### Source Authentication and Authorization


Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-mssql` by itself or in a pipeline using [Meltano](https://meltano.com/).

<!--
### Executing the Tap Directly

```bash
tap-mssql --version
tap-mssql --help
tap-mssql --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_mssql/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-mssql` CLI interface directly using `poetry run`:

```bash
poetry run tap-mssql --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->
<!--
Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-mssql
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-mssql --version
# OR run a test `elt` pipeline:
meltano elt tap-mssql target-jsonl
```
-->
### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
