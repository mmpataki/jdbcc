There are two tools documented here.
1. JDBCClient
2. KUtil

1. JDBCClient
=============
Simple, stupid jdbc client which has the following features
    * Not driver specific
    * Recording sessions in HTML feature
	* records SQL, query run time, results

how to use:
1. Compile the program if not done.
	# unzip jdbcc.zip
	# cd jdbcc
	# export INFA_HOME=/path/to/infa/home
        # ./compile.sh
2. Run the program.
	# java -cp ".:/path/to/your/jdbc/jars" JDBCClient \
		-d <driver-name>			  \
		-c <connect-string>			  \
		-u <user-name>				  \
		-p <password>				  \
3. To turn on recording. You will get the output in output-file.
	# java ... JDBCClient ... -r <output-file> ...





2. KUtil
========
Program to run the other programs in kerberos context. Note that
KUtil honors the conf properties loaded into it and will not run the
Child utility/program in kerberos context. It depends on the
following property
        hadoop.security.authentication

how to use:
1. Compile the program if not done.
        # unzip jdbcc.zip
        # cd jdbcc
        # export INFA_HOME=/path/to/infa/home
        # ./compile.sh
2. Know the main class name of the program you want to run. In this
   example I assume it as "JDBCClient". We are using this JDBCClient
   to connect to a secure Hive DB
3. Let the KUtil run you program in Hadoop Kerberos context
        # java -cp . KUtil                      \
                -u <kerberos-principal-name>    \
                -k </path/to/keytab/file>       \
                JDBCClient                      \
                <args needed for JDBCClient>

Other options available
        -cf </path/to/conf/file>   : Load hadoop configuration files
        -cv <key=value>            : Load config from key value pair

eg.
1. Load config file present in /tmp/abc.xml. (Absolute path)
        # java -cp . KUtil -cf /tmp/abc.xml ...
2. Use the config "foo.bar.baz=tmp"
        # java -cp . KUtil -cv foo.bar.baz=tmp ...
