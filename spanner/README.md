# Spanner dialect for SQLAlchemy.

This is an implementation of Python's DB-API and SqlAlchemy + Alembic driver 
for Google Cloud Spanner database. It is a Proof-Of-Concept implementation, it takes a 
number of shortcuts and has a number of limitations.

# Running

You should be able to run tests and the Airflow with Spanner database 
with those prerequisites:

* Switch to a brand new Python 3.6 virtualenv. If you are familiar with 
  `virtualenvwrapper` it's easiest to use `mkvirtualenv -p python3.6 <VENVNAME>`
  then you should be able to switch to the virtualenv easily via `workon <VENVMNAME>`

* Add `spanner` directory to `PYTHONPATH` environment variable. If you run your
  tests from within IDE such as IntelliJ, you can simply mark `spanner` directory 
  as "Sources Dir"

* Install airflow dependencies. In the root of `incubator-airflow` project run:
```bash
 pip install -e .[devel_all,gcp_api]
```  
Note that depending on the OS (MacOS, Linux) - you might need to have several extra
dependencies needed - usually you can install then via apt-get (Linux) or brew (MacOS).

Usually the following command will do the job: `brew install sqlite mysql postgresql`

* Create a spanner instance and database. You can do it via 
[Create instance](https://console.cloud.google.com/spanner/instances/new) Google Console
and then create a database using "Create Database" button.

Point to the database in 
[airflow_spanner.cfg](airflow_spanner.cfg). 
You should replace project_id, instance_id, database_id in the URL:

```ini
sql_alchemy_conn = spanner:///?project_id=polidea-airflow&instance_id=test-instance-jarek&database_id=airflow
```

* Install spanner db_api dependencies and the db_api driver itself locally 
in your local virtualenv. In the [spanner](.) directory run:

```bash
pip install -e .
```

* Set `GOOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your
  service account json key (absolute path). The Service Account should have
  Spanner Admin Role assigned
  
* Set `AIRFLOW_CONFIG` environment variable to point to your 
[airflow_spanner.cfg](airflow_spanner.cfg) (absolute path)


Copy [spanner_airflow_plugin.py](spanner_airflow_plugin.py) to 
`${AIRFLOW_HOME}/airflow/plugins`. If you have no AIRFLOW_HOME variable defined then
do it in ${HOME}/airflow/plugins

# Testing

As a starting point, you can run test `spanner.test.test_spanner:SpannerInitDBTest`. It 
will guide on what environemtn variables should be set and will 
copy the plugin file automatically.

If you run it with an empty Spanner database - it should create an Aifrlow Database in 
your Spanner instance.
