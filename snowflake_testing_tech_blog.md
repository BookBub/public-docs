# Testing against Snowflake

At Bookbub, we use and love Snowflake as our data warehouse tool. But one problem we encountered early on was around writing automated tests for our jobs that interact with Snowflake. We have many data transformations with tricky logic details that need to be exercised, as well as jobs where data flows through s3 and are pulled into our data warehouse via Snowflake stages. How can we test these effectively?

## Postgres as a stand-in for Snowflake

In other SQL database systems, like Postgres, you can spin up an empty local database, seed it with whatever fake data you need, and run jobs against it for testing. But Snowflake doesn't have the concept of a local database.

At first, we thought we might be able to use a local Postgres instance as a stand-in for Snowflake. But this presented many problems. Postgres and Snowflake SQL have dialectic differences. We initially tried maintaining a series of regexes for converting Snowflake SQL into Postgres SQL - for example, anytime our Snowflake query contained JSON parsing using `json:key` syntax, in the test environment it would have to be replaced within the query string with `json ->> key` syntax for Postgres compatibility.

```python
def translate_to_postgres(self, sql):
    sql = sql.replace('CURRENT_DATE()', 'CURRENT_DATE')

    # replace DATEADD(day, -1, some_date) with some_date + INTERVAL '-1 days'
    sql = re.sub(r"DATEADD\((.*), (.*), (.*)\)", r"\3 + INTERVAL '\2 \1'", sql, flags=re.IGNORECASE)

    # replace `REGEXP_SUBSTR(field, 'pattern')` with `SUBSTRING(field, 'pattern')
    sql = sql.replace('REGEXP_SUBSTR', 'SUBSTRING')

    # replace `CHARINDEX('&', 'string with & symbol')` with `position('&' in 'string with & symbol')`
    sql = re.sub(r"CHARINDEX\(([^,]+),", r"position(\1 in ", sql, flags=re.IGNORECASE)

    # replace TRY_TO_NUMBER(exp) with CAST(exp) AS INTEGER)
    sql = re.sub(r"TRY_TO_NUMBER\((.*)\)", r"CAST(\1 AS INTEGER)", sql, flags=re.IGNORECASE)

    # replace `TO_NUMBER(expression)` with `CAST(expression AS INTEGER)`
    sql = re.sub(r"TO_NUMBER\((.*)\)", r"CAST(\1 AS INTEGER)", sql, flags=re.IGNORECASE)

    # replace json:key with json -> 'key' for the specific case of data:properties
    sql = sql.replace('data:properties', "data -> 'properties'")

    # replace json:key with json ->> 'key'
    # some tricky business here avoiding matching times (e.g. 00:00)
    sql = re.sub(r"([\s(])([A-Za-z_]+):([A-Za-z_]+)",  r"\1(\2 ->> '\3')", sql)

    # replace lateral flatten() with json_each()
    sql = re.sub(r"lateral flatten\((.*) => (.*)\)", r"json_each(\2)", sql)

    # replace ::string with ::text
    sql = sql.replace('::string', '::text')

    # replace USER-DEFINED with text
    sql = sql.replace('USER-DEFINED', 'text')

    # replace variant with json
    sql = sql.replace('variant', 'json')

    # replace TIMESTAMP_NTZ with timestamp without time zone
    sql = sql.replace('TIMESTAMP_NTZ', 'timestamp without time zone')

    # replace DATE_TRUNC(value with DATE_TRUNC('VALUE'
    sql = re.sub(r"DATE_TRUNC\((.*),", r"DATE_TRUNC('\1',", sql, flags=re.IGNORECASE)

    return sql
```
Exhibit A: This was starting to get a little unwieldy to maintain...

We found ourselves needing to add more and more to our translation function, and at the same time, bugs were slipping past us into production - because we weren't testing the actual query that ran in production, we were testing a cobbled-together translation of that query, against a foreign database system. The tests weren't serving their purpose of exercising the actual production code. It just wasn't working.

In addition to that, there are Snowflake features that other database systems just don't have, like stages. We couldn't test any of our jobs that make use of Snowflake stages at all.

So we started to think about actually hitting Snowflake in the testing environment. From there, two concerns quickly came into focus: keeping the testing environment safely cordoned off from production, and speed/performance concerns.

## Environment isolation

The safest thing to do would be to have separate Snowflake accounts for different environments. But, this would be expensive. We decided to create our test databases within the same account that holds our production data, and just set permissions carefully to make sure the wires can't get crossed.

We created a Snowflake user and role specifically for tests. The test role has the ability to create new databases within the account that it then has full ownership over, but has no access to production databases. The secrets that are provided to the test environment only contain the test user credentials. This is enough to ensure there is no way for the tests to accidentally touch higher environments' data.

We set up similar safeguards in s3 for when we're testing stages. We set up a test IAM user, and a separate test version of every bucket we need to test against. The test IAM user only has access to the test buckets.

A different but related issue is that of concurrency. Any number of instances of the test suite can be running against the same account at the same time - various branches running in CI, as well as human users running tests from their machines. All of these must run against separate databases with no chance of crossing wires. And we must also isolate test data flowing through s3!

We handle this by assigning a random UUID to each test suite run. The test suite run will create a database with that UUID in the name and use it for all its testing. We also use the UUID to create a directory within the test s3 buckets, and all s3 data from the test run will go into that directory. Test databases and directories are cleaned up at the end of the test run.

Here's some of the code that supports this:

This function sets the session UUID (or gets it if it's already been set) using environment variables.

```python
def session_id():
    if environment() in ['test', 'development']:
        env_variable = 'SESSION_UUID'
        session_identifier = os.environ.get(env_variable)
        if not session_identifier:
            session_identifier = str(uuid.uuid4())
            os.environ[env_variable] = session_identifier
        return session_identifier
    else:
        return ''
```

This class manages our connection to Snowflake. The `app_settings` variable holds environment-specific secrets, so it will ensure we use the limited test Snowflake/IAM users in the testing environment.

```python
# Use a single connection in the testing environment as a performance optimization
@add_metaclass(Singleton if environment() == 'test' else type)
class WarehouseHook(metaclass=Singleton):

    def __init__(self, query_tag, *args, **kwargs):
        super(WarehouseHook, self).__init__(*args, **kwargs)

        self.snowflake_credentials = app_settings.snowflake_credentials
        self.aws_credentials = app_settings.aws_credentials
        self.db_name = self.snowflake_credentials['database_name'] + session_id()
        self.setup_connection(query_tag)

    def setup_connection(self, query_tag):
        queries = []
        if query_tag:
            queries.append(f"ALTER SESSION SET QUERY_TAG = '{query_tag}'")

        queries.append(f'CREATE DATABASE IF NOT EXISTS "{self.db_name}"')
        queries.append(f'USE DATABASE "{self.db_name}"')
        self.run_queries_in_transaction(queries)

    def create_stage(self, stage_name, bucket_name, file_format):
        sql = """
        CREATE OR REPLACE STAGE {stage_name}
                url='s3://{bucket}/'
            credentials=(aws_key_id='{key}' aws_secret_key='{secret}')
            file_format = {file_format};
        """.format(
            bucket=self.s3_bucket_for_environment(bucket_name),
            key=self.aws_credentials['key'],
            secret=self.aws_credentials['secret'],
            file_format=file_format
        )
        self.__execute(sql)

    @staticmethod
    def s3_bucket_for_environment(bucket_name):
        suffix = f'-{environment()}'
        env_bucket_path = bucket_name + suffix

        if environment() in ['development', 'test']:
            env_bucket_path += ('/' + session_id())

        return env_bucket_path
```

This code is in `conftest.py`, a special file used by pytest for defining setup and teardown hooks to run at the beginning and end of test sessions.

```python
@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown(request):
    test_db_hook = WarehouseHook()

    ### This block of code will make more sense after the discussion of performance optimization

    if not use_single_test_mode():
        collect_all_objects_used_by_tests()
        commands = build_ddl_commands()
        if len(commands) > 0:
            create_stored_procedure(commands, test_db_hook)
            call_stored_procedure(test_db_hook)
        create_stages(test_db_hook, __all_stages_used_by_tests)
        create_custom_functions(test_db_hook, __all_custom_functions_used_by_tests)

    elif reuse_test_db():
        logging.warning("Reusing test database {} for speed. ".format(test_db_hook.db_name) +
                        "Tables will not be reinstantiated. " +
                        "If you need to make a schema change, please use `export REUSE_TEST_DB=false`.")

        tables = test_db_hook.run_query('SHOW TABLES IN DATABASE "{}"'.format(test_db_hook.db_name)).fetchall()
        for table in tables:
            __existing_tables.add('{}.{}'.format(table['schema_name'], table['name']).lower())
    
    ###

    def drop_database_and_cleanup_s3():
        for bucket in __all_s3_buckets_used_by_tests:
            delete_from_s3(bucket, session_id())

        if not reuse_test_db():
            test_db_hook.run_query('DROP DATABASE "{}"'.format(test_db_hook.db_name))

        test_db_hook.close_snowflake_connection(override_testing_skip=True)

    request.addfinalizer(drop_database_and_cleanup_s3)
```

## Performance

By far the most difficult aspect of using Snowflake for testing has been performance. When we first switched from using Postgres as a stand-in for Snowflake, we were dismayed to find that a naive implementation of setting up an empty database and seeding it with the tables and other objects we'd need for testing took *minutes.*

There are multiple reasons for this. First, when you use a local Postgres database for testing, you don't have to send any commands over a network connection - the database server you're talking to is on your own machine. But Snowflake databases are in the cloud, which adds I/O time.

Secondly, this is *specifically* the kind of thing that Snowflake is not optimized to be good at. Snowflake's superpower is running small numbers of complex analytics queries against large data in a reasonable amount of time. In the testing use case, the behavior is exactly opposite of this - we're running huge numbers of tiny queries, to instantiate tables, fill them with small sets of test data, and then run our test queries against those small datasets. We're using Snowflake *wrong.*

As a side-note, this is the main reason why we at Bookbub are super excited about Snowflake's upcoming feature, Unistore, which promises web-scale query runtimes for transactional use cases. This isn't what they had in mind when they added this feature, but we think it will help our test suite's performance *a lot.*

But alas, we currently live in a world without Unistore and have had to find creative ways to optimize our test suite. Let's discuss some of the specific performance bottlenecks and how we dealt with them.

### Instantiating empty tables

In our Snowflake data warehouse, we have thousands of tables. When we used Postgres for testing, we were easily able to instantiate empty versions of all of these tables when spinning up our test database - Postgres can run thousands of tiny `create table` statements in seconds. But when we first tried naively running our thousands of `create table` statements in Snowflake, it took about 9 minutes 😬

The first optimization we made was only instantiating the tables we need. Our repo's test suite doesn't touch every one of those thousands of tables. We also wanted to optimize for the use case of an engineer who is actively developing a feature and repeatedly running a single test module - which may touch only one or two tables and doesn't need the overhead of instantiating everything.

The way we accomplished this was by tagging test modules with metadata about which tables they require. At the top of each test module, we define an array variable called `tables_used_by_tests`. When you run a single test module, there is a setup hook that iterates over that array and instantiates all those tables. Other Snowflake objects like stages and file formats are also tagged and created in this way.

```python
def create_tables(test_db_hook, table_names):
    schemas = set()
    tables = []
    for table in table_names:
        if not reuse_test_db() or table not in __existing_tables:
            [schema_name, table_name] = table.split('.')
            schemas.add(schema_name)
            tables.append((schema_name, table_name))

    for schema in schemas:
        test_db_hook.create_schema(schema)
    # current_schema changes to newly created schema after a create schema statement; switch back to public
    test_db_hook.run_query('USE SCHEMA PUBLIC')

    for schema, table in tables:
        # Note: we maintain a set of JSON files in our repo that describes the schema (column names and types)
        # of each table, which supports this function. There's a whole bunch of code behind how we keep these
        # files up to date with reality, but that's outside the scope of this blog topic.
        test_db_hook.create_table_if_not_exists(schema, table)
        __existing_tables.add(f'{schema}.{table}')
```

However, this is a bit naive for the use case of running the entire test suite during CI, since there's a lot of overlap between the sets of tables used by various modules. So for this use case, we wrote some code that runs just once at the beginning of the entire test suite and introspects all of the test modules to get the full set of tables used by all tests. Then we can just iterate over that list and create them all at once.

```python
def collect_all_objects_used_by_tests():
    if __all_tables_used_by_tests and __all_stages_used_by_tests:
        return
    else:
        modules = []
        package = sys.modules['test']
        for _, name, is_package in pkgutil.walk_packages(path=package.__path__, prefix='test.'):
            if not is_package:
                modules.append(name)
        for module_name in modules:
            module = importlib.import_module(module_name)
            if hasattr(module, 'tables_used_by_tests'):
                __all_tables_used_by_tests.update(module.tables_used_by_tests)
            if hasattr(module, 'stages_used_by_tests'):
                __all_stages_used_by_tests.update(module.stages_used_by_tests)
            if hasattr(module, 'custom_functions_used_by_tests'):
                __all_custom_functions_used_by_tests.update(module.custom_functions_used_by_tests)
            if hasattr(module, 's3_buckets_used_by_tests'):
                __all_s3_buckets_used_by_tests.update(module.s3_buckets_used_by_tests)
```

These two use cases - running the whole suite during CI, and running just one module while iterating - are distinct and require different optimization strategies. So we added the concept of a "test run mode" of either "single" or "full" and use a different strategy depending on which one we're in. We use environment variables to make the mode default to "full" in CI and "single" for developers.

One breakthrough that hugely helped with the efficiency of "full" test mode was the use of a stored procedure to instantiate all the tables. Instead of making dozens of separate network calls out to Snowflake to queue up queries to instantiate our tables, we make just two - one to define a stored procedure that contains all the `create table` statements, and one to call that procedure. This saves a lot on the I/O time.

```python
def create_stored_procedure(commands, db_hook):
    proc = """
        create or replace procedure CREATE_ALL_TABLES()
        returns string
        language javascript
        as
        $$
        var sql_commands = [
            {commands}
        ];
        try {{
            for(command of sql_commands){{
              snowflake.execute (
                  {{sqlText: command}}
                  );
            }}
            return "Succeeded.";   // Return a success/error indicator.
        }}
        catch (err)  {{
            return "Failed: " + err;   // Return a success/error indicator.
        }}
        $$
        ;
    """.format(commands="'" + "',\n'".join(commands) + "'")
    db_hook.run_query(proc)

def call_stored_procedure(db_hook):
    result = db_hook.run_query('call CREATE_ALL_TABLES()')
    result = result.fetchone()['CREATE_ALL_TABLES']
    if result != 'Succeeded.':
        raise RuntimeError(result)
    # current_schema changes to newly created schema after a create schema statement; switch back to public
    db_hook.run_query('USE SCHEMA PUBLIC')
```

### Between-test cleaning

We must also deal with cleaning up tables between tests. In some database systems, this can be accomplished very quickly and easily by smart use of transactions. You can wrap each test run in a transaction and roll it back at the end to quickly throw out all the modifications to database objects made by your test. This will only work if your database system supports nested transactions - otherwise, if you commit a transaction at any point during the course of your test, you won't be able to roll it back at the end. Snowflake unfortunately does not support nesting of transactions. Snowflake will also auto-commit your open transaction any time you run a DDL query, which happens frequently within the code we're testing. So we can't do this.

Instead, we must use a teardown hook that runs after every test to iterate over tables that have data and truncate them. We could make use of `tables_used_by_tests` for this too, but we've actually found it's a bit faster and less fraught to just ask Snowflake to tell us which tables are "dirty":

```python
def truncate_non_empty_tables():
    test_db_hook = WarehouseHook()
    non_empty_tables = test_db_hook.run_query('''
        select TABLE_SCHEMA, TABLE_NAME
        from INFORMATION_SCHEMA.TABLES
        where ROW_COUNT > 0
    ''').fetchall()

    for table_row in non_empty_tables:
        if environment() in ['test', 'development']:
            test_db_hook.execute('TRUNCATE TABLE {}.{}'.format(table_row['TABLE_SCHEMA'], table_row['TABLE_NAME']))
        else:
            TypeError("You're trying to truncate a production Snowflake table. You probably don't actually want to do that.")
```

### Other optimizations

Another optimization that proved very helpful was making the WarehouseHook, the class that handles connecting to Snowflake, a singleton in the testing environment. This prevents us from having to redundantly connect and run the setup code that specifies which database, warehouse, and role to use.

A further optimization that applies to "single" test mode only is the option to keep the test database at the end of the run and reuse it on the next run. This can save time because you won't have to redo the initial setup of instantiating tables. But this feature is opt-in, because it can cause issues if you need to adjust the schema of a table during the course of iteration by adding or dropping columns, and we found that this was confusing for developers.

Despite all the work we put into novel optimizations here, our full test suite would still not be performant enough to be usable if not for parallelism. Any good CI tool should allow you to split up your tests into groups that run in parallel, and do so strategically so that each group of tests runs in roughly the same amount of time. All told, our full test suite currently runs in 36 parallel containers and takes about 7 minutes to run. Each partition is further optimized to only instantiate the objects it needs to run its "slice" of the tests.

To avoid overwhelming the Snowflake warehouse with all this parallelism, we have 6 different XS-sized Snowflake warehouses allocated for CI, each of which supports 6 of the 36 partitions. Running our test suite is a significant source of Snowflake spend for us, but it's well worth it.

## Developer UX

To improve the experience of writing tests, we've also developed lots of helpers for seeding tables with data, and for making assertions about the results of a transform. Developers in our repo are able to describe their seed data and assertions in JSON form using our helpers.

Here's what a typical basic test might look like:

```python
def test_build_materialized_view():
    insert_seed_data({
        'public.users': [
            {'id': 1, 'name': 'Rachel Wigell'},
            {'id': 2, 'name': 'Tessa Nijssen'},
            {'id': 3, 'name': 'Rob Lewis'}
        ],
        'public.audiobooks': [
            {'id': 100, 'title': 'Project Hail Mary'},
            {'id': 200, 'title': 'The Hobbit'}
        ],
        'public.user_audiobooks': [
            {'user_id': 1, 'audiobook_id': 100},
            {'user_id': 2, 'audiobook_id': 200},
            {'user_id': 3, 'audiobook_id': 100},
        ]
    })

    # We use Airflow, so for us this function is calling a task from a DAG that runs a data transform
    # but this could be a call to any function that mutates data
    run_task('denormalize_data', 'user_audiobooks')

    assert_table_has_data('public.user_audiobooks_denormalized', [
        {'user_id': 1, 'name': 'Rachel Wigell', 'audiobook_id': 100, 'title': 'Project Hail Mary'},
        {'user_id': 2, 'name': 'Tessa Nijssen', 'audiobook_id': 200, 'title': 'The Hobbit'},
        {'user_id': 3, 'name': 'Rob Lewis', 'audiobook_id': 100, 'title': 'Project Hail Mary'},
    ])
```

And here's the code behind it:

```python
def insert_seed_data(seed_data)
    # convert into an insert query and run
    snowflake_hook = WarehouseHook()
    for table_name, data_list in seed_data.items():
        if '.' in table_name:
            namespace, table_name = table_name.split('.')
        else:
            namespace = 'public'

        columns_list = []
        values_list = []
        for row in data_list:
            columns_list += row.keys()
        columns_list = list(set(columns_list))

        for row in data_list:
            values_list.append(",".join([snowflake_hook.convert_to_string(row.get(column)) for column in columns_list]))

        query = '''
                insert into {namespace}.{table} ({columns}) SELECT {values}
            '''.format(namespace=namespace,
                       table=table_name,
                       columns=','.join(columns_list),
                       values=' union all select '.join(values_list))

        snowflake_hook.run_query(query)

def convert_to_string(self, value, inner=False):
    if value is None:
        return 'null'

    elif type(value) == list:
        # recursively format the elements within the array
        formatted_list = ','.join([self.convert_to_string(x, inner=inner) for x in value])
        if inner:
            return '[{}]'.format(formatted_list)
        else:
            return "array_construct({})".format(formatted_list)

    elif type(value) == dict:
        # recursively format the elements within the dict
        formatted_dict = ', '.join([
            '{}:{}'.format(self.convert_to_string(k, inner=True), self.convert_to_string(v, inner=True)) for k, v in value.items()
        ])
        formatted_dict = '{{ {} }}'.format(formatted_dict)
        if inner:
            return formatted_dict
        else:
            return 'parse_json($${}$$)'.format(formatted_dict)

    elif type(value) in (int, bool):
        return str(value)

    elif type(value) == RawSnowflakeValue:
        if inner:
            raise snowflake.connector.errors.ProgrammingError("Cannot use RawSnowflakeValues inside of JSON data")
        else:
            return value.raw_value

    else:
        # Escaping steps copied from snowflake-connector-python project:
        # https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/converter.py#L641
        value = str(value)
        value = value.replace("\\", "\\\\")
        value = value.replace("\n", "\\n")
        value = value.replace("\r", "\\r")
        value = value.replace("\047", "\134\047") # single quotes
        return f"'{value}'"

def assert_table_has_data(transformed_table_name, expected_transformed_data):
    # get the actual data from the table
    # only select the columns included in the expected data; ignore all others
    columns = set()
    for row in expected_transformed_data:
        columns.update(row.keys())
    column_string = ', '.join(columns) if columns else '*'

    snowflake_hook = WarehouseHook()
    actual_transformed_data = snowflake_hook.run_query('''
        select {columns} from {table}
    '''.format(
        columns=column_string,
        table=transformed_table_name
    )).fetchall()

    # disregard casing in column names by casting both to lowercase
    actual_results = lower_column_names(actual_transformed_data)
    expected_results = lower_column_names(expected_transformed_data)

    test_class = unittest.TestCase()
    test_class.maxDiff = None
    test_class.assertCountEqual(actual_results, expected_results)
    
```

## Feedback

As far as we've been able to determine, what we're doing with Snowflake in the testing environment is somewhat unique, which leads us to wonder how other companies that use Snowflake as a key part of their data stack are handling testing. What do you do? Have you found a better or less convoluted strategy than what we've described here? We'd love to hear from you.
