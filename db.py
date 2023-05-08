import psycopg2 as pg
import pandas as pd
import psycopg2.extras as extras
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_batch, execute_values
from aws.aws_secrets import SecretService
import warnings
import os



class DbConn:

    db_library = {
        'dev': 'mdc_dataops',
        'production': 'mdc_db',
        'mdc_db': 'mdc_db',
        'mdc_dataops': 'mdc_dataops',
        'gds_db': 'mdcDataViz'
    }

    """
    A class for connecting to a PostgreSQL database and executing queries. This class provides a set of methods for interacting with a PostgreSQL database, 
    including executing SQL queries, creating new tables, and reading data from tables as pandas dataframes. The class uses the `psycopg2` library to connect
      to the database.

    Attributes:
        None

    Methods:
        __init__: Initializes a new `DbConn` object, either by reading database credentials from an AWS SSM parameter store or from environment variables when 
        running locally.
        db_query: Executes an SQL query on the database and returns the result set.
        db_close: Closes the database connection and cursor.
        execute_values: Inserts data from a pandas dataframe into a specified table using the psycopg2 `execute_values` method.
        bulk_insert: Inserts data into a specified table using the psycopg2 `execute_values` method and a provided SQL query.
        create_table_from_df: Creates a new table in the database with the specified name and schema, based on the columns in a provided pandas dataframe.
        pandas_read: Executes an SQL query on the database and returns the result set as a pandas dataframe.
        get_col_names: Returns a list of column names for a specified table in the database.
        get_col_datatypes: Returns a dictionary of column names and data types for a specified table in the database.
        ...

    Usage:
        To use the `DbConn` class, first create a new instance by calling the constructor and passing in the required credentials or parameters. Then, 
        call the desired methods to interact with the database. For example:

        Connecting to a database locally using environment variables
        db_conn = DbConn(db, local=True)

        Connecting to a database locally using credentials passed directly to the class
        db_conn = DbConn(db, local=True, creds=creds)

        Connecting to a database via AWS or Serverless function
        db_conn = DbConn(db, creds_name=creds_name)

 
        ```
        db = DbConn(creds_name='my-db-creds')
        results = db.db_query('SELECT * FROM my_table')
        df = db.pandas_read('SELECT * FROM my_table')
        db.create_table_from_df('new_table', df)
        ```

        This code creates a new `DbConn` object with credentials stored in the AWS SSM parameter store under the name 'my-db-creds', executes an SQL query on 
        the database, reads the result set as a pandas dataframe, and creates a new table in the database based on the columns in the dataframe.
    """

    warnings.filterwarnings('ignore')

    def __init__(self, db, local=False, creds=None, creds_name=None):

        
        self.db = self.db_library.get(db)

        # For a local connection using env variables
        if local is True and not creds:
            self.host = os.environ.get('pHOST')
            self.user = os.environ.get('pUSERNAME')
            self.pword = os.environ.get('pPASSWORD')
            self.db = os.environ.get('pDATABASE')

        # For a local connection with creds passed directly to class
        elif local is True and creds:
            self.user = creds['username']
            self.pword = creds['password']
            self.host = creds['host']
            self.db = creds['dbname']

        # For serverless/aws connection
        else:
            if not creds_name:
                raise 'ERROR: Must pass credential name if not using a local connection'
            ss = SecretService('ssm')
            creds = ss.get_paramstore_db_creds(creds_name)

        #Attempt Connection
        try:
            self.con = pg.connect(database=self.db, user=self.user, password=self.pword, host=self.host)
            self.cursor = self.con.cursor()
            self.con.autocommit = True

        except Exception as e:
            print(f'ERROR: {e}')


    def db_query(self, sql, results=True):
        """Executes sql query on db and returns result set"""
        self.cursor.execute(sql)
        if results:
            return self.cursor.fetchall()
        return 0

    def db_close(self):
        """Closes cursor and connection to db"""
        self.cursor.close()
        self.con.close()

    def execute_values(self, df, table):
        """Insert df rows into table"""

        # Creating a list of tuples from the dataframe values
        tpls = [tuple(x) for x in df.to_numpy()]

        # dataframe columns with Comma-separated
        cols = ','.join(list(df.columns))

        # SQL query to execute
        sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = self.con.cursor()
        try:
            extras.execute_values(cursor, sql, tpls)
            print("Data inserted using execute_values() successfully..")
            return 0
        except (Exception, pg.DatabaseError) as err:
            print('ERROR:', err)
            return err


    def bulk_insert(self, sql, values):
        try:
            self.con = pg.connect(database=self.db, user=self.user, password=self.pword, host=self.host)
            self.cursor = self.con.cursor()
            self.con.autocommit = True
        except Exception as e:
            print(f'ERROR: {e}')
        cur = self.con.cursor()
        try:
            execute_values(cur, sql, values, page_size=10000)
        except pg.DatabaseError as ex:
            # print('Execute Values Issue: {}\n QUERY EXECUTED: {}'.format(ex, sql))
            print('Error: {}'.format(ex))



    def create_table_from_df(self, table_name, df):
        """
        Creates a new table with the specified name using the columns in the given dataframe.

        Args:
            table_name (str): The name of the new table.
            df (pandas.DataFrame): The dataframe to use for creating the table.

        Raises:
            ValueError: If the table already exists in the database.
        """
        # Write to the raw schema exclusively
        schema = 'raw'
        table_name = schema+'.'+table_name
 

        # Generate the SQL statement for creating the table
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, "

        for i, col in enumerate(df.columns):
            if i > 0:
                create_table_sql += ", "
            create_table_sql += f"{col} TEXT"
        create_table_sql += ")"

        # Execute the SQL statement to create the table
        print(create_table_sql)
        self.db_query(create_table_sql, results=False)
        self.con.commit()

        self.execute_values(df, table_name)
        self.con.commit()


    def pandas_read(self, sql: str) -> pd.DataFrame:
        """
        Query database with SQL and return result set as pandas dataframe.
        Search terms: sql_to_df, sql_to_dataframe, query_to_df, query_to_dataframe
        """
        try:
            if self.con is not None:
                data = pd.read_sql(sql, self.con)
                return data
        except pg.DatabaseError as e:
            print(e)


    def get_col_names(self, tbl_name):
        """
        Return list of column names for specified table.

        tbl_name should always be schema-qualified (eg, 'raw.some_table' and not 'some_table'.
        Differs from get_column() func in that this func returns a list, whereas get_column() returns a string.
        """
        assert '.' in tbl_name, "{} must be schema-qualified (eg, some_schema.some_table)".format('tbl_name')
        schema_name, tbl_name = tbl_name.split('.')

        # declare an empty list for the column names
        columns = []

        # declare cursor objects from the connection

        # con = self.connect(Database.mdc_db.value)
        col_cursor = self.con.cursor()

        # concatenate string for query to get column names
        # col_names_str = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{tbl_name}' AND table_schema = '{schema_name}'"
        col_names_str = SQL.sql_column.value.format(tbl_name, schema_name)

        try:
            # pass SQL statement to sql.SQL() method
            sql_object = sql.SQL(col_names_str)

            # execute the SQL string to get list with col names in a tuple
            col_cursor.execute(sql_object)

            # get the tuple element from the liast
            col_names = (col_cursor.fetchall())

            # iterate list of tuples and grab first element
            for tup in col_names:
                # append the col name string to the list
                columns += [tup[0]]

            # close the cursor object to prevent memory leaks
            col_cursor.close()

        except Exception as err:
            print("get_col_names ERROR:", err)

        return columns


    def get_col_datatypes(self, schema, table):
        """
        Query the information_schema.columns table and return a dict with column names as the keys and datatypes as the
        values.

        Returns dict like this:
        {
            "column1": "numeric",
            "column2": "text"
        }
        """

        sql = f"""SELECT column_name,
                        CASE 
                            WHEN data_type = 'ARRAY' 
                            THEN replace(udt_name, '_', '') || '[]' 
                            ELSE data_type 
                        END AS data_type
    				FROM information_schema.columns 
    				WHERE table_name = '{table}' 
    				AND table_schema = '{schema}'
    			"""
        col_datatypes_df = self.pandas_read(sql)

        col_datatypes_dict = col_datatypes_df.set_index('column_name').to_dict()
        return col_datatypes_dict['data_type']


    @staticmethod
    def make_conditional_upsert_delete_sql(src_tbl, tgt_tbl, match_cols, val_cols, join_conditions):
        """
        Constructs a DELETE SQL statement that deletes only records that meet specified conditions.

        Assumes that the columns in match_cols and val_cols are present in each table.
        """

        assert '.' in src_tbl, AssertionMsg.schema_qualified.value.format('src_tbl')
        assert '.' in tgt_tbl, AssertionMsg.schema_qualified.value.format('tgt_tbl')
        col_intersection = list(set(match_cols) & set(val_cols))
        assert len(col_intersection) == 0, AssertionMsg.lists_cant_intersect.value.format('match_cols', 'val_cols',
                                                                                          col_intersection)

        sql = f"""
    		DELETE FROM {tgt_tbl} TGT
    		USING {src_tbl} SRC
    		WHERE
    		"""

        # for each col in match_cols, construct a WHERE condition that says the matching_cols must be equal
        and_conditions = []
        for match_col in match_cols:
            and_conditions.append(
                f"{ETLGlobalVars.src_tbl_alias.value}.{match_col} = {ETLGlobalVars.tgt_tbl_alias.value}.{match_col}")
        and_conditions_sql = '\n AND '.join(and_conditions)
        sql += ' ' + and_conditions_sql

        # construct additional AND-separated WHERE condition for each condition in join_conditions
        for condition in join_conditions:
            sql += f'\nAND {condition}'
        sql += '\n;'

        return sql.replace('\t', '')


    @staticmethod
    def make_generic_upsert_delete_sql(src_tbl, tgt_tbl, match_cols, val_cols):
        """
        Constructs a DELETE SQL statement that deletes only records that are different between two tables.

        Assumes that the columns in match_cols and val_cols are present in each table. All columns are casted to text
        datatype for sake of comparison.
        """

        assert '.' in src_tbl, AssertionMsg.schema_qualified.value.format('src_tbl')
        assert '.' in tgt_tbl, AssertionMsg.schema_qualified.value.format('tgt_tbl')
        col_intersection = list(set(match_cols) & set(val_cols))
        assert len(col_intersection) == 0, AssertionMsg.lists_cant_intersect.value.format('match_cols', 'val_cols',
                                                                                          col_intersection)
        # val_cols = list(set(val_cols).symmetric_difference(match_cols))  # remove match cols from val cols

        sql = f"""
    		DELETE FROM {tgt_tbl} TGT
    		USING {src_tbl} SRC
    		WHERE
    		"""

        # for each col in match_cols, construct a WHERE condition that says the matching_cols must be equal
        and_conditions = []
        for match_col in match_cols:
            and_conditions.append(
                f"{ETLGlobalVars.src_tbl_alias.value}.{match_col} = {ETLGlobalVars.tgt_tbl_alias.value}.{match_col}")
        and_conditions_sql = '\n AND '.join(and_conditions)
        sql += ' ' + and_conditions_sql + '\n AND ('

        # for each col in val_cols, construct OR-separated WHERE conditions to see if any values are different
        or_conditions = []
        for val_col in val_cols:
            or_conditions.append(
                f"TRIM(COALESCE({ETLGlobalVars.src_tbl_alias.value}.{val_col}::text, '')) <> TRIM(COALESCE({ETLGlobalVars.tgt_tbl_alias.value}.{val_col}::text, ''))")
        or_conditions_sql = '\n OR '.join(or_conditions)
        sql += ' ' + or_conditions_sql + '\n);'

        return sql.replace('\t', '')


    def make_generic_upsert_insert_sql(self, src_tbl, tgt_tbl, match_cols, val_cols, cast_to_tgt=False):
        """
        Constructs a SQL statement to INSERT records from src_tbl to tgt_tbl.

        Assumes that columns in cols are present in both tables. If cast_to_tgt is True, then the values in the src_tbl
        will be cast to the datatypes of the corresponding columns in the tgt_tbl in the INSERT statement.
        """

        assert '.' in src_tbl, AssertionMsg.schema_qualified.value.format('src_tbl')
        assert '.' in tgt_tbl, AssertionMsg.schema_qualified.value.format('tgt_tbl')

        and_conditions = []
        for match_col in match_cols:
            and_conditions.append(
                f"{ETLGlobalVars.src_tbl_alias.value}.{match_col} = {ETLGlobalVars.tgt_tbl_alias.value}.{match_col}")
        and_conditions_sql = '\n AND '.join(and_conditions)

        # change sql to cast src_tbl cols to datatype of corresponding tgt_tbl cols
        if cast_to_tgt:
            schema = tgt_tbl.split('.')[0]
            table = tgt_tbl.split('.')[1]
            dtypes_dict = self.get_col_datatypes(schema, table)
            src_val_cols = [f'{ETLGlobalVars.src_tbl_alias.value}.' + s + '::' + dtypes_dict[s] for s in val_cols]
        else:
            src_val_cols = [f'{ETLGlobalVars.src_tbl_alias.value}.' + s for s in val_cols]

        sql = f"""
    			INSERT INTO {tgt_tbl} ({', '.join(val_cols)})
    			SELECT DISTINCT {', '.join(src_val_cols)}
    			FROM {src_tbl} SRC
    			WHERE NOT EXISTS (
    				SELECT 1 FROM {tgt_tbl} TGT
    				WHERE {and_conditions_sql}
    				);
    		"""

        # cast to numeric before casting to integer so that Postgres can translate strings like '1.0' into 1
        sql = sql.replace('::integer', '::numeric::integer')
        return sql.replace('\t', '')


    def do_generic_upsert(self, src_tbl, tgt_tbl, src_rm_cols, match_cols, cast_to_tgt=False, non_val_cols=None,
                          return_sql=False):
        """
        Perform an upsert from src_tbl to tgt_tbl, assuming that column names match between the src_tbl and tgt_tbl.

        This function performs a basic ETL going from the src_tbl table to the tgt_tbl. The ETL
        involves deleting the records from the tgt_tbl table only if one or more values are different from its
        counterpart record in the src_tbl table. Counterpart records are determined using the match_cols columns.
        Following the deletion, the records in the src_tbl table are inserted into the tgt_tbl table if the match_cols
        values don't already exist in the tgt_tbl table. The ETL process is wrapped in a single transaction so that if
        one of the steps fails then the process will be rolled back automatically, including the deletion step.

        If cast_to_tgt is True, then the values in the src_tbl will be cast to the datatypes of the corresponding
        columns in the tgt_tbl in the INSERT statement.

        non_val_cols should be a list of columns that should not be used for the DELETE statement, but will be included
        in the INSERT statement. Examples include batch_id; you should expect batch_id to be different between source
        and target and ir should therefore not be used to test for differences in the data between source and target,
        but you still want to include it in the INSERT statement.
        """

        # get src_tbl columns
        src_tbl_cols = self.get_col_names(src_tbl)
        if non_val_cols is None:
            non_val_cols = []
        if src_rm_cols is None:
            src_rm_cols = []
        if match_cols is None:
            match_cols = []
        for col in src_rm_cols + match_cols + non_val_cols:
            try:
                src_tbl_cols.remove(col)
            except ValueError:
                pass

        # construct DELETE sql
        del_sql = self.make_generic_upsert_delete_sql(src_tbl=src_tbl,
                                                      tgt_tbl=tgt_tbl,
                                                      match_cols=match_cols,
                                                      val_cols=src_tbl_cols)
        # construct INSERT sql
        ins_sql = self.make_generic_upsert_insert_sql(src_tbl=src_tbl,
                                                      tgt_tbl=tgt_tbl,
                                                      match_cols=match_cols,
                                                      val_cols=match_cols + src_tbl_cols + non_val_cols,
                                                      cast_to_tgt=cast_to_tgt)

        # construct transaction sql
        txn_sql = SQL.sql_transaction.value.format(del_sql + '\n' + ins_sql)

        # print(del_sql)
        # print(ins_sql)
        # execute SQL to transfer records from src to tgt
        if return_sql:
            return txn_sql
        print(f'Transferring records from {src_tbl} to {tgt_tbl}')
        try:
            self.db_query(txn_sql, results=False)
            print('Success')
            return 0
        except Exception as e:
            print(f'ERROR(!): Failed to transfer records from {src_tbl} to {tgt_tbl}: {e}\nSQL: {txn_sql}')
            return 1


    def do_truncate_insert(self, src_tbl, tgt_tbl, src_rm_cols, cast_to_tgt=False):
        """
        Truncate tgt_tbl and transfer all rows from src_tbl to tgt_tbl, assuming that all columns match.
        """

        # get src_tbl columns
        src_tbl_cols = self.get_col_names(src_tbl)
        if src_rm_cols is None:
            src_rm_cols = []

        # remove unwanted columns as specified by src_rm_cols param
        for col in src_rm_cols:
            try:
                src_tbl_cols.remove(col)
            except ValueError:
                pass

        # construct TRUNCATE sql
        trunc_sql = f"TRUNCATE TABLE {tgt_tbl} RESTART IDENTITY;"

        # construct INSERT sql
        # change sql to cast src_tbl cols to datatype of corresponding tgt_tbl cols
        if cast_to_tgt:
            schema = tgt_tbl.split('.')[0]
            table = tgt_tbl.split('.')[1]
            dtypes_dict = self.get_col_datatypes(schema, table)
            src_cols_casted = [f'{ETLGlobalVars.src_tbl_alias.value}.' + s + '::' + dtypes_dict[s] for s in src_tbl_cols]
            ins_sql = f"""INSERT INTO {tgt_tbl} ({', '.join(src_tbl_cols)})
                            SELECT DISTINCT {', '.join(src_cols_casted)}
                            FROM {src_tbl} SRC;
                            """
        else:
            ins_sql = f"""INSERT INTO {tgt_tbl} ({', '.join(src_tbl_cols)})
                    SELECT DISTINCT {', '.join(src_tbl_cols)}
                    FROM {src_tbl} SRC;
                    """

        # construct transaction sql
        txn_sql = SQL.sql_transaction.value.format(trunc_sql + '\n' + ins_sql)

        # execute SQL to transfer records from src to tgt
        print(f'Transferring records from {src_tbl} to {tgt_tbl}')
        try:
            self.db_query(txn_sql, results=False)
            print('Success')
            return 0
        except Exception as e:
            print(f'ERROR(!): Failed to transfer records from {src_tbl} to {tgt_tbl}: {e}\nSQL: {txn_sql}')
            return 1


    def do_conditional_upsert(self, src_tbl, tgt_tbl, src_rm_cols, match_cols, join_conditions=None, cast_to_tgt=False,
                              non_val_cols=None, return_sql=False):
        """
        Transfer data from src_tbl to tgt_tbl based on some condition, assuming that all columns match.

        `join_conditions`: List of WHERE conditions to be used in the WHERE clause of the DELETE statement.

        non_val_cols should be a list of columns that should not be used for the DELETE statement, but will be included
        in the INSERT statement.

        Example usage:
        join_conditions = ['source_table.updated_at::timestamp > target_table.updated_at::timestamp']
        upsert_res = self.con.do_conditional_upsert(src_tbl=stg_tbl,
                                                    tgt_tbl=dwh_tbl,
                                                    src_rm_cols=rm_cols,
                                                    match_cols=['uuid'],
                                                    join_conditions=join_conditions,
                                                    cast_to_tgt=True,
                                                    non_val_cols=['batch_id'])
        """

        assert join_conditions is not None, "Must pass at least one join condition to join_conditions param"

        # get src_tbl columns
        src_tbl_cols = self.get_col_names(src_tbl)

        if non_val_cols is None:
            non_val_cols = []
        if src_rm_cols is None:
            src_rm_cols = []
        if match_cols is None:
            match_cols = []

        # remove unwanted columns as specified by src_rm_cols param
        for col in src_rm_cols + match_cols + non_val_cols:
            try:
                src_tbl_cols.remove(col)
            except ValueError:
                pass

        # construct DELETE sql statement
        del_sql = self.make_conditional_upsert_delete_sql(src_tbl=src_tbl,
                                                          tgt_tbl=tgt_tbl,
                                                          match_cols=match_cols,
                                                          val_cols=src_tbl_cols,
                                                          join_conditions=join_conditions)

        # construct INSERT sql statement
        ins_sql = self.make_generic_upsert_insert_sql(src_tbl=src_tbl,
                                                      tgt_tbl=tgt_tbl,
                                                      match_cols=match_cols,
                                                      val_cols=match_cols + src_tbl_cols + non_val_cols,
                                                      cast_to_tgt=cast_to_tgt)

        # construct transaction sql
        txn_sql = SQL.sql_transaction.value.format(del_sql + '\n' + ins_sql)

        if return_sql:
            return txn_sql

        # execute SQL to transfer records from src to tgt
        print(f'Transferring records from {src_tbl} to {tgt_tbl}')
        try:
            self.db_query(txn_sql, results=False)
            print('Success')
            return 0
        except Exception as e:
            print(f'ERROR(!): Failed to transfer records from {src_tbl} to {tgt_tbl}: {e}\nSQL: {txn_sql}')
            return 1


    def read_single_value_query(self, sql):
        """Execute passed sql query and return the value at the first column and first row of the result set as a
        string
        """
        return str(self.pandas_read(sql).iloc[0, 0])
