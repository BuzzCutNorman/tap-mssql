# tap-mssql

`tap-mssql` is a Singer tap for mssql. !!! Warning !!! work in progress. It works ok 😐 for full loads.
<br>It works maybe 🤷‍♀️🤷‍♂️ for Incremntal loads.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

### Whats New 🛳️🎉
**2024-08-20 msgspec:**  I have been working on getting a quicker JSON encoder in place for a while and thanks to Edger at Arch I am able too.  The library I switched to is [msgspec]( https://jcristharif.com/msgspec/).  It is lightweight and fast.  Big Thank You 🙏 to Jim Crist-Harif for writing and maintaining `msgspec`.  I also removed `pedulum` and am using phython datetime at the moment.    

**2024-08-12 Pendulum Dependency Fix:** Arch had been saying it was removing `pendulum` as a dependency for month. I being a procrastinator didn't change tap-mssql  to use another library for dealing with dates, because I have plenty of time. Singer-SDK 0.39.1 removed `pendulum` as they said they would and well I didn't 😅. Luckly Anna Nylander swooped in to save the day by adding `pendulum` as a dependancy for tap-mssql. 🎉🥳🎉

**2024-08-05 Upgraded to Meltano Singer-SDK 0.39.0**

**2024-05-07 Upgraded to Meltano Singer-SDK 0.36.1**

**2024-01-31 Upgraded to Meltano Singer-SDK 0.34.1:** Happy New Year!!!🎉.  My goal was to start using tags and releases by 2024 and was pretty close.  You can now lock on a release number if you want. 

**2023-10-16 Upgraded to Meltano Singer-SDK 0.32.0:** SQLAlchemy 2.x is main stream in this version so I took advantage of that and bumped from `1.4.x` to `2.x`.  SDK 0.32.0 also has a built-in feature to set streams to be resumeable when running incremental extracts (Thanks 🙏 to Pat from Meltano). The issue with Windows wheels for `pymssql` was resolved so I bumped it back up to `2.2.8`. The `BIT` data type is now converted to the json schema type of `bool`.  MS SQL has the data type `TIMESTAMP` which is used to track row versions and is definitely not a `datetime`.  `TIMESTAMP` and `ROWVERSION` are now converted to a `string`.  In the `hd_jsonschema_types` the `minimum` and `maximum` values used to define `NUMERIC` or `DECIMAL` precision and scale values were being rounded.  This caused an issue with the translation on the target side.  I leveraged scientific notation to resolve this.

**2023-08-30 MultiSubnetFailover:** A big thanks to @wesseljt and his colleagues for finding when using `pyodbc` you may need to pass `MultiSubnetFailover: yes` when connecting to an SQL Server AG VNN.  `MultiSubnetFailover` has been added to the settings.  This also prompted me to add a Troubleshooting section to the readme.

**2023-05-03 Incremental Replication:**  Equipped with the Singer-SDK documentation on how to implement Incremental Replication and davert0 issue filled with great details I headed off on a coding adventure.  There were twists, turns, and backtracking but in the end you can now setup Incremental Replication and it might work.  If you are using Meltano here is the documentation to follow to setup [Key-based Incremental Replication ](https://docs.meltano.com/guide/integration#key-based-incremental-replication) and manage [Incremental Replication State](https://docs.meltano.com/guide/integration#incremental-replication-state). Skim over the documentation and head off on you own Incremental Replication adventure.

**2023-04-26 New HD JSON Schema Types:**  XML, IMAGE, BINARY, and VARBINARY types have been give definitions.  This is Thanks🙏 to Singer-SDK 0.24.0 which allows for JSON Schema `contentMediaType` and `contentEncoding`.  Currently all binary data types are Base64 encoded before being sent to a tartget. The buzzcutnorman `target-mssql` and `target-postgres` are both able to translate them back into SQL data types. 

**2023-02-22 Batch Message Can Handle More Data Types:** You use batch message when a table contains DECIMAL or NUMERIC columns. Plus better batch message support for TIME, DATE, and DATETIME columns.

**2023-02-08 Higher Defined(HD) JSON Schema types:**  This is my interpretation of how to define MS SQL data types using the JSON Schema.  You can give it a try by setting `hd_jsonschema_types` to `True` in your `config.json` or `meltano.yml`.  The buzzcutnorman `target-mssql` and `target-postgres` are both able to translate them back into SQL data types.

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
meltano config tap-mssql set sqlalchemy_url_query.driver "ODBC Driver 18 for SQL Server"
```

When using `pyodbc` `sqlalchemy_url_query.TrustServerCertificate` let SQLAlchemy know whether to trust server signed certificates when connecting to SQL Server.  Using this will correct the connection error of 
```bash
meltano config tap-mssql set sqlalchemy_url_query.TrustServerCertificate yes
```

The `pyodbc` driver has added support for a “fast executemany” mode of execution which greatly reduces round trips.  You can trun the option on or off by setting `sqlalchemy_eng_params.fast_executemany` to `"True"` or `"False"`
```bash
meltano config tap-mssql set sqlalchemy_eng_params.fast_executemany "True"
```
### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-mssql --about --format=markdown
```
-->


| Setting              | Required | Default | Description |
|:---------------------|:--------:|:-------:|:------------|
| dialect              | True     | mssql   | The Dialect of SQLAlchamey |
| driver_type          | True     | pymssql | The Python Driver you will be using to connect to the SQL server |
| host                 | True     | None    | The FQDN of the Host serving out the SQL Instance |
| port                 | False    | None    | The port on which SQL awaiting connection |
| user                 | True     | None    | The User Account who has been granted access to the SQL Server |
| password             | True     | None    | The Password for the User account |
| database             | True     | None    | The Default database for this connection |
| sqlalchemy_eng_params| False    | None    | SQLAlchemy Engine Paramaters: fast_executemany, future |
| sqlalchemy_url_query | False    | None    | SQLAlchemy URL Query options: driver, TrustServerCertificate |
| batch_config         | False    | None    | Optional Batch Message configuration |
| start_date           | False    | None    | The earliest record date to sync |
| hd_jsonschema_types  | False    | False | Turn on Higher Defined(HD) JSON Schema types to assist Targets |
| stream_maps          | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config    | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled   | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |

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

## Troubleshooting

### Pyodbc Connection Errors

#### The certificate chain was issued by an authority that is not trusted:
```
sqlalchemy.exc.OperationalError: (pyodbc.OperationalError) ('08001', '[08001] [Microsoft][ODBC Driver 18 for SQL Server]SSL Provider: The certificate chain was issued by an authority that is not trusted.\r\n (-2146893019) (SQLDriverConnect); [08001] [Microsoft][ODBC Driver 18 for SQL Server]Client unable to establish connection (-2146893019)')
```
If the SQL Server you are connecting too is utlizing a self signed certificate you will get this error.  You can tell pyodbc to trust the self signed certificate with the following configuration command.

```bash
meltano config tap-mssql set sqlalchemy_url_query.TrustServerCertificate yes
```

#### Login timeout expired:
```
sqlalchemy.exc.OperationalError: (pyodbc.OperationalError) ('HYT00', '[HYT00] [Microsoft][ODBC Driver 17 for SQL Server]Login timeout expired (0) (SQLDriverConnect)')
```
Users have reported running into this issue when pointing to a Avaibility Group (AG) Virutal Network Name (VNN) when the group is split across multiple subnets. You will need to let pyodbc know this by adding `MultiSubnetFailover: yes`. This can ben done by running the following configuration command.

```bash
meltano config tap-mssql set sqlalchemy_url_query.MultiSubnetFailover yes
```

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
