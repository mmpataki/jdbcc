# jdbcc

Simple, stupid jdbc client which has the following features
- Not driver specific
- Recording sessions in HTML feature
	- records SQL, query run time, results

### Usage sample

```bash
$ java -cp ".:ojdbc8.jar" JDBCClient --help
jdbcc version (1.1)
Usage : java JDBCClient <options>
Options:
   switch                              reqd multiple help
   -c, --url <url>                       *           jdbc url
   -u, --user <user>                     *           username
   -p, --password <password>             *           password
   -d, --driver <driver>                             JDBC Driver class name
   -t, --transformer <transformers>             *    Column transformers, specify as columnName=transformerClassName
   -l, --limit <resultPrintLimit>                    Number of records to print from result set
   -r, --record                                      Enables recording to HTML file
   -x, --debug                                       Enables debug logging
   -h, --help                                        Prints help
   -i, --input <inputFile>                           Input file (Can have SQL or shell commands)
   --props <propsFile>                               Config props file
   --nolinenum                                       Disables line number printing in shell
   --printProps                                      Prints sample props file
```

### Connecting to a DB and querying
Note the username password are prompted
```
$ java -cp ".:ojdbc8.jar" JDBCClient -c "jdbc:informatica:oracle://db.com:1521;ServiceName=orcl"
jdbcc version (1.1)
Enter user (username): mpatakia
Enter password (password):
shell started...
connecting to db: 3811ms
connection status : [successful]
Type help; for shell help
jdbcc> select table_name from user_tables;
sql execution: 1547ms (status: successful)
TABLE_NAME (varchar2) |
----------------------+
APP_NOTIFICATIONS     |
APP_NOTIFICATION_DATA |
AUTH_TOK              |
BLOBBY                |
CLOBBY                |
COMMENTS              |
EMPLOYEE              |
EVENTS                |
GROUPS                |
GROUPS_USERS          |

10 rows (limited), time: 2ms
```

### Shell help
```
jdbcc> help;
java code execution: 574ms (status: successful)
This is a SQL and JDBC API shell. There are two modes in the shell.
shell and SQL. The inputs which start with ! run the input in shell
mode and sql mode otherwise. JDBC APIs can be executed on the shell
variables (listed at the end).

Here are some examples for JDBC API execution
  !md.getSchemas(null, "MPATAKI%");
  !md.getTables(null, "MPATAKI%", null, null);


All the inputs should end with ; (semicolon)

Methods: (use !help("method"); to get help on methods)
   connect         : Connect to a database
   close           : Close the connection
   help            : Print this help
   executeSql      : Execute SQL

Shell variables: (use !variable; to print it)
   conn            : The connection variable
   conf            : The configuration of this program
   rs              : Result set from the last sql execution
   stmt            : Statement object
   md              : Database metadata object
```

### Using JDBC APIs 
```
jdbcc> !md.getSchemas(null, "MPATAKI%");
java code execution: 1109ms (status: successful)
TABLE_SCHEM ()   |TABLE_CATALOG () |
-----------------+-----------------+
MPATAKIA         |null             |
MPATAKIB         |null             |
MPATAKIC         |null             |

3 rows (all), time: 5ms
```

### Modifying shell config
```
jdbcc> !conf;
java code execution: 750ms (status: successful)
transformers = {BLOB=JDBCClient$BClobToString@1810399e, CLOB=JDBCClient$BClobToString@32d992b2}
resultPrintLimit = 10
record = false
debug = false
noLineNumbers = false

jdbcc> !conf.debug = true;
java code execution: 602ms (status: successful)
true

jdbcc> !conf;
transformers = {BLOB=JDBCClient$BClobToString@1810399e, CLOB=JDBCClient$BClobToString@32d992b2}
resultPrintLimit = 10
record = false
debug = true
noLineNumbers = false
```
