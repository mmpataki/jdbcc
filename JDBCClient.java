import java.io.*;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.stream.Collectors;

public class JDBCClient {

    @Retention(RetentionPolicy.RUNTIME)
    public @interface Exposed {
        String value();
    }

    public interface Transformer {
        public String transform(Object o);
    }

    public static class Configuration {
        public boolean printResults = true;
        public boolean record;
        public boolean debug;

        @Override
        public String toString() {
            return Arrays.stream(getClass().getFields()).map(f -> {
                try {
                    return String.format("%s = %s", f.getName(), f.get(this));
                } catch (IllegalAccessException e) {
                    return "";
                }
            }).collect(Collectors.joining("\n"));
        }
    }

    public static abstract class ScriptCallable {

        // The exposed variables
        @Exposed("The connection variable")
        public Connection conn = null;

        @Exposed("The configuration of this program")
        public Configuration conf = null;

        @Exposed("Result set from the last sql execution")
        public ResultSet rs = null;

        @Exposed("Statement object")
        public Statement stmt;

        @Exposed("Database metadata object")
        public DatabaseMetaData md;


        // Exposed methods in shell
        @Exposed("Connect to a database")
        public Connection connect(@Exposed("jdbcUrl") String jdbcUrl, @Exposed("user") String user, @Exposed("password") String password) throws Exception {
            return client.connect(jdbcUrl, user, password);
        }

        @Exposed("Close the connection")
        public boolean close() throws Exception {
            return client.close();
        }

        @Exposed("Execute SQL")
        public boolean executeSql(@Exposed("sqlQuery") String sql) throws Exception {
            return client.executeSql(sql);
        }

        @Exposed("Print this help")
        public String help(String... on) {
            if (on.length == 0) {
                String methods = Arrays.stream(getClass().getMethods()).filter(m -> m.isAnnotationPresent(Exposed.class)).map(m -> String.format("   %-15s : %s", m.getName(), m.getAnnotation(Exposed.class).value())).collect(Collectors.joining("\n"));
                String fields = Arrays.stream(getClass().getFields()).filter(f -> f.isAnnotationPresent(Exposed.class)).map(f -> String.format("   %-15s : %s", f.getName(), f.getAnnotation(Exposed.class).value())).collect(Collectors.joining("\n"));
                return "" +
                        "This is a SQL and JDBC API shell. There are two modes in the shell.\n" +
                        "shell and SQL. The inputs which start with ! run the input in shell\n" +
                        "mode and sql mode otherwise. JDBC APIs can be executed on the shell\n" +
                        "variables (listed at the end).\n" +
                        "\n" +
                        "Here are some examples\n" +
                        "  !md.getSchemas(null, \"MPATAKI%\");\n" +
                        "  !md.getTables(null, \"MPATAKI%\", null);\n" +
                        "\n\n" +
                        "All the inputs should end with ; (semi-colon)\n\n" +
                        "Methods: (use !help(\"method\"); to get help on methods)\n" +
                        methods +
                        "\n\nShell variables: (use !variable; to print it) \n" +
                        fields +
                        "\n\n";
            } else {
                Optional<Method> opt = Arrays.stream(getClass().getMethods()).filter(m -> m.getName().equals(on[0])).findFirst();
                if (!opt.isPresent()) {
                    return ("Shell API " + on[0] + " is not available");
                }
                Method method = opt.get();
                if (!method.isAnnotationPresent(Exposed.class)) {
                    log(on[0] + " is not exposed");
                }
                return String.format(

                        "// %s\n" +
                                "%s(%s)\n",
                        method.getAnnotation(Exposed.class).value(),
                        method.getName(),
                        Arrays.stream(method.getParameterAnnotations()).map(a -> ((Exposed) a[0]).value()).collect(Collectors.joining(", "))
                );
            }
        }

        public abstract Object call() throws Exception;


        // crap, private
        private JDBCClient client;

        public void init(JDBCClient cli) throws SQLException {
            client = cli;
            conn = cli.conn;
            conf = cli.conf;
            rs = cli.rs;
            stmt = cli.stmt;
            md = conn.getMetaData();
        }
    }


    boolean runTimeHook = false;
    private Connection conn = null;
    private Configuration conf = new Configuration();
    private ResultSet rs;
    private Statement stmt;

    public Connection connect(String jdbcUrl, String user, String password) throws Exception {
        pstart();
        conn = DriverManager.getConnection(jdbcUrl, user, password);

        $("<pre>")
                .$("JDBC connection string : ").$(jdbcUrl).$("\n")
                .$("Driver : ").$(jdbcUrl).$("\n")
                .$("User : ").$(user).$("\n")
                .$("Connecting to DB took : ").$(pend("Connecting to DB")).$("\n")
                .$("</pre>");

        log("connection status : [" + ((conn != null) ? "successful" : "failed") + "]");

        if (!runTimeHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(this::close));
            runTimeHook = true;
        }

        return conn;
    }

    public boolean close() {
        log("\nclosing the connection");
        try {
            conn.close();
            if (fw != null)
                fw.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean executeSql(String query) throws Exception {
        return stmt.execute(query);
    }

    static Object[][] registeredArgs = new Object[][]{
            {"-d", "driver class name", true, null},
            {"-c", "JDBC connection string", true},
            {"-u", "username", true},
            {"-p", "password", true},
            {"-r", "true|false", false},
            {"-t", "[column_name=transformerClassName;]+", false}
    };


    String url;
    String user;
    String driver;
    String record;
    String password;
    static long start, end;
    String txPlugins;

    private FileWriter fw;
    private Scanner sc;
    HashMap<String, String> argMap = new HashMap<>();
    static HashMap<String, Transformer> transformers = new HashMap<>();

    public static void main(String args[]) throws Exception {
        if (args.length > 0 && args[0].equals("-help")) {
            boolean required;
            System.err.println("Usage : java JDBCClient <options>");
            System.err.println("Options: ");
            for (Object[] regArg : registeredArgs) {
                required = (boolean) regArg[2];
                System.err.println("\t" + (required ? "" : "[") + regArg[0] + (required ? "" : "]") + " <" + regArg[1] + ">");
            }
        }
        System.err.println("Passed args : " + Arrays.toString(args));
        new JDBCClient(args).shell();
    }

    public JDBCClient(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {

        sc = new Scanner(System.in);

        /* register the args */
        for (Object[] requiredArg : registeredArgs) {
            argMap.put((String) requiredArg[0], null);
        }

        /* parse the args */
        for (int i = 0; i < args.length; i += 2) {
            argMap.put(args[i], args[i + 1]);
        }

        /* ask for the left out args, if they are required */
        for (Object[] regArg : registeredArgs) {
            if (((Boolean) regArg[2]) && argMap.get((String) regArg[0]) == null) {
                System.out.print("Enter " + regArg[1] + ": ");
                argMap.put((String) regArg[0], sc.nextLine());
            }
        }

        url = argMap.get("-c");
        user = argMap.get("-u");
        driver = argMap.get("-d");
        password = argMap.get("-p");
        txPlugins = argMap.get("-t");
        record = argMap.get("-r");

        System.err.println("Parsed args : " + argMap);

        if (record != null) {
            conf.record = true;
            String recordFile = "jdbcc_record_" + record + "_" + ((new Date()).getTime()) + ".html";
            try {
                fw = new FileWriter(recordFile);
            } catch (Exception e) {
                System.out.println("Failed to open " + recordFile + " " + e.getMessage());
                e.printStackTrace();
            }
            log("Logging to " + recordFile);
        }

        System.err.println("Registering convertor plugins...");
        if (txPlugins == null || txPlugins.isEmpty())
            return;
        String plugins[] = txPlugins.split(";");
        for (String plugin : plugins) {
            String kvp[] = plugin.split("=");
            Class<?> txplugin = Class.forName(kvp[1]);
            transformers.put(kvp[0], (Transformer) txplugin.newInstance());
        }
    }

    public void shell() throws Exception {

        connect(url, user, password);
        stmt = conn.createStatement();

        log("Type help; for help");

        do {
            boolean status = false;
            Object result = null;
            String query = readQuery().trim();

            debug("Text read from console: " + query);
            if(query.equals("help")) {
                query = "!help()";
            }

            boolean sql = !query.startsWith("!");
            String executionMode = (sql ? "sql" : "java code");
            query = sql ? query : query.substring(1);

            debug("executing " + executionMode + " [" + query + "]");
            pstart();
            try {
                if (!sql) {
                    result = processJavaCode(conn.getMetaData(), query.trim());
                } else {
                    result = executeSql(query);
                }
                status = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
            long etime = pend(executionMode + " execution", "(status: " + (status ? "successful" : "failed") + ")");

            if (status) {
                try {
                    rs = null;
                    if (result instanceof ResultSet) {
                        rs = (ResultSet) result;
                    }
                    if (sql && ((boolean) result)) {
                        rs = stmt.getResultSet();
                    }
                    if (rs != null && conf.printResults) {
                        printResult(query, etime, rs);
                    } else {
                        System.out.println(result);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } while (true);
    }

    /* short hand for record */
    public JDBCClient $(Object o) {
        if (conf.record && fw != null) {
            try {
                if (o == null)
                    o = "<NULL>";
                fw.write(o.toString());
                fw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return this;
    }

    String javac;
    long idx = System.currentTimeMillis();

    String newClassName() {
        return "Class" + (++idx);
    }

    Object processJavaCode(DatabaseMetaData dbm, String code) throws Exception {

        if (javac == null) {
            String javaHome = System.getProperty("java.home");
            if (javaHome.endsWith("jre")) {
                javaHome = javaHome + File.separator + "..";
            }
            javac = javaHome + File.separator + "bin" + File.separator + "javac";
            debug("javac = " + javac);
        }

        String cls = newClassName(), jfile = cls + ".java";
        FileWriter fw = new FileWriter(jfile);

        code = String.format(
                "" +
                        "import java.sql.*;\n" +
                        "import java.util.*;\n" +
                        "import java.io.*;\n" +
                        "public class %s extends JDBCClient.ScriptCallable {\n" +
                        "  public Object call() throws Exception {\n" +
                        "    return %s;\n" +
                        "  }\n" +
                        "}",
                cls, code
        );
        debug("Generated code");
        debug(code);

        fw.write(code);
        fw.close();
        Runtime.getRuntime().traceMethodCalls(true);
        Process exec = Runtime.getRuntime().exec(javac + " " + jfile);
        BufferedReader br = new BufferedReader(new InputStreamReader(exec.getErrorStream()));
        String line;
        while((line = br.readLine()) != null) {
            log(line);
        }
        exec.waitFor();
        try {
            ScriptCallable caller = (ScriptCallable) Class.forName(cls).newInstance();
            caller.init(this);
            return caller.call();
        } catch (ClassNotFoundException cnf) {
            log("Error while compiling the code");
            return null;
        }
    }

    private void debug(String s) {
        if (conf.debug) {
            log(s);
        }
    }

    private void printResult(String query, long time, ResultSet rset) throws Exception {

        if (rset == null)
            return;

        $("<pre>")
                .$("<b>").$(htmlize(query)).$("</b>")
                .$("\n")
                .$("Time: ").$((((double) time) / 1000))
                .$("</pre>")
                .$("<table border='1' style='border-collapse: collapse;'>");

        /* print cols headers */
        String coln;
        int numCols, maxsize = 0;
        ArrayList<String> cols = new ArrayList<>();
        ResultSetMetaData rsmd = rset.getMetaData();
        for (numCols = 0; true; numCols++) {
            try {
                coln = rsmd.getColumnName(numCols + 1);
                cols.add(coln);
                maxsize = Math.max(maxsize, coln.length());
            } catch (Exception e) {
                break;
            }
        }

        $("<tr>");
        for (String col : cols) {
            System.out.printf("%-" + maxsize + "s |", col);
            $("<th style='background-color: lightgray'>").$(col).$("</th>");
        }
        $("</tr>");
        System.out.println();
        for (int i = 0; i < numCols; i++) {
            for (int j = 0; j < maxsize; j++)
                System.out.append("-");
            System.out.append("-+");
        }
        System.out.println();

        try {
            while (rset.next()) {
                debug("next");
                $("<tr>");
                for (int i = 0; i < numCols; ++i) {
                    Object data = null;
                    try {
                        data = rset.getObject(i + 1);
                        Transformer tx = null;
                        if (data != null) {
                            tx = transformers.get(cols.get(i));
                        }
                        if (tx != null) {
                            data = tx.transform(data);
                        }
                    } catch (SQLException e) {
                    }
                    System.out.printf("%-" + maxsize + "s |", data);
                    $("<td>").$(data).$("</td>");
                }
                $("</tr>");
                System.out.println();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        $("</table>");
    }

    private static void log(Object o) {
        System.out.println(o.toString());
    }

    private String readQuery() {
        String line, prompt = "jdbcc> ";
        StringBuilder sb = new StringBuilder();
        do {
            System.out.print(prompt);
            line = sc.nextLine().trim();
            sb.append(line).append(" ");
            prompt = "";
        } while (line.trim().equals("") || !(line.charAt(line.length() - 1) == ';'));
        return sb.deleteCharAt(sb.length() - 2).toString();
    }

    private static void pstart() {
        start = System.currentTimeMillis();
    }

    private static long pend(String event) throws Exception {
        return pend(event, "");
    }

    private static long pend(String event, String suffix) throws Exception {
        end = System.currentTimeMillis();
        log(event + " took : " + (end - start) + "ms. " + suffix);
        return (end - start);
    }

    private String htmlize(String s) {
        s = s.replace(">", "&gt;");
        s = s.replace("<", "&lt;");
        s = s.replace("&", "&amp;");
        s = s.replace(" ", "&nbsp;");
        return s;
    }
}
