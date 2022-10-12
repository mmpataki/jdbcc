import java.io.*;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.stream.Collectors;

public class JDBCClient {

    public static final String VERSION = "1.1";

    @Retention(RetentionPolicy.RUNTIME)
    public @interface Exposed {
        String value();
    }


    @Retention(RetentionPolicy.RUNTIME)
    public @interface Argument {
        String[] keys();

        String help();

        boolean required() default false;

        boolean multivalued() default false;

        boolean sensitive() default false;

        String parser() default "defaultParser";
    }

    public interface Transformer {
        String transform(Object o) throws Exception;
    }

    public class BClobToString implements Transformer {
        @Override
        public String transform(Object o) throws Exception {
            if (o instanceof Blob) {
                return new String(((Blob) o).getBytes(1l, (int) ((Blob) o).length()));
            }
            if (o instanceof Clob) {
                Clob clob = (Clob) o;
                StringBuilder sb = new StringBuilder((int) clob.length());
                Reader r = clob.getCharacterStream();
                char[] cbuf = new char[1024];
                int n;
                while ((n = r.read(cbuf, 0, cbuf.length)) != -1) {
                    sb.append(cbuf, 0, n);
                }
                return sb.toString();
            }
            return o.toString();
        }
    }

    // https://gist.github.com/mmpataki/8514550e3b8aa97f3e0cd98011e4e553
    public class Configuration {

        // connect required args
        @Argument(keys = {"-c", "--url"}, required = true, help = "jdbc url")
        private String url;

        @Argument(keys = {"-u", "--user"}, required = true, help = "username")
        private String user;

        @Argument(keys = {"-p", "--password"}, required = true, sensitive = true, help = "password")
        private String password;

        @Argument(keys = {"-d", "--driver"}, help = "JDBC Driver class name")
        private String driver;


        @Exposed("Transformers")
        @Argument(keys = {"-t", "--transformer"}, multivalued = true, parser = "txParser", help = "Column transformers, specify as columnName=transformerClassName")
        public final HashMap<String, Transformer> transformers = new LinkedHashMap<>();


        // output control
        @Argument(keys = {"-l", "--limit"}, help = "Number of records to print from result set")
        @Exposed("The number of records to print from a result set")
        public int resultPrintLimit = 10;

        @Argument(keys = {"-r", "--record"}, help = "Enables recording to HTML file")
        @Exposed("Record to an HTML file")
        public boolean record = false;

        @Argument(keys = {"-x", "--debug"}, help = "Enables debug logging")
        @Exposed("Enable debug logging")
        public boolean debug = false;

        @Argument(keys = {"-h", "--help"}, help = "Prints help")
        private boolean help;

        // input control
        @Argument(keys = {"-i", "--input"}, help = "Input file (Can have SQL or shell commands)")
        private String inputFile;

        @Argument(keys = {"--props"}, help = "Config props file")
        private String propsFile;

        @Argument(keys = {"--nolinenum"}, help = "Disables line number printing in shell")
        @Exposed("Disable line numbers in shell")
        public boolean noLineNumbers;

        @Argument(keys = {"--printProps"}, help = "Prints sample props file")
        private boolean printProps = false;

        // internal for debugging
        boolean __debug = false;

        @Override
        public String toString() {
            return Arrays.stream(getClass().getDeclaredFields()).filter(f -> __debug || f.isAnnotationPresent(Exposed.class)).map(f -> {
                try {
                    return String.format("  %-30s  # %s", String.format("%s = %s", f.getName(), f.get(this)), f.getAnnotation(Argument.class).help());
                } catch (IllegalAccessException e) {
                    return "";
                }
            }).collect(Collectors.joining("\n"));
        }

        @SuppressWarnings("unchecked")
        public void txParser(Field f, Configuration c, String s) throws Exception {
            if (f.get(c) == null) f.set(c, new HashMap<>());
            if (s == null || s.isEmpty()) return;
            ((Map) f.get(c)).put(s.substring(0, s.indexOf("=")), (Transformer) Class.forName(s.substring(s.indexOf('=') + 1)).newInstance());
        }

        public void defaultParser(Field f, Configuration c, String s) throws Exception {
            Object val = null;
            if (Integer.TYPE.isAssignableFrom(f.getType())) {
                val = Integer.parseInt(s);
            } else if (Long.TYPE.isAssignableFrom(f.getType())) {
                val = Long.parseLong(s);
            } else if (Double.TYPE.isAssignableFrom(f.getType())) {
                val = Double.parseDouble(s);
            } else if (Float.TYPE.isAssignableFrom(f.getType())) {
                val = Float.parseFloat(s);
            } else if (Boolean.TYPE.isAssignableFrom(f.getType())) {
                val = s == null || Boolean.valueOf(s);
            } else {
                val = s.isEmpty() ? null : s;
            }
            f.set(c, val);
        }

        private void set(Field f, String val) throws Exception {
            getClass().getMethod(f.getAnnotation(Argument.class).parser(), Field.class, Configuration.class, String.class)
                    .invoke(this, f, this, val);
        }

        public Configuration(String args[]) throws Exception {
            Map<String, Field> fields = new LinkedHashMap<>();
            Arrays.stream(Configuration.class.getDeclaredFields()).filter(f -> f.isAnnotationPresent(Argument.class)).forEach(f -> {
                Argument arg = f.getAnnotation(Argument.class);
                for (String key : arg.keys())
                    fields.put(key, f);
            });
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (!fields.containsKey(arg)) {
                    System.out.println("unknown argument: " + arg);
                    System.exit(0);
                }
                Field field = fields.get(arg);
                set(field, field.getType().isAssignableFrom(boolean.class) ? "true" : args[++i]);
            }
            if (propsFile != null) {
                Properties props = new Properties();
                props.load(new FileReader(propsFile));
                for (Map.Entry<Object, Object> entry : props.entrySet()) {
                    set(getClass().getDeclaredField(entry.getKey().toString()), entry.getValue().toString());
                }
            }
            if (printProps) {
                for (Field f : Configuration.class.getDeclaredFields()) {
                    if (f.isAnnotationPresent(Argument.class)) {
                        Argument arg = f.getAnnotation(Argument.class);
                        System.out.printf("# %s\n", arg.required() ? "REQUIRED" : "OPTIONAL");
                        System.out.printf("# %s\n", f.getAnnotation(Argument.class).help());
                        boolean noVal = f.get(this) == null || Map.class.isAssignableFrom(f.getType()) || Collection.class.isAssignableFrom(f.getType());
                        System.out.printf("%s=%s\n", f.getName(), noVal ? "" : f.get(this));
                        System.out.println();
                    }
                }
                System.exit(0);
            }
            if (!help) {
                for (Field f : fields.values()) {
                    Argument argConf = f.getAnnotation(Argument.class);
                    if (!(f.getAnnotation(Argument.class).required() && f.get(this) == null)) continue;
                    System.out.printf("Enter %s (%s): ", f.getName(), argConf.help());
                    set(f, (argConf.sensitive()) ? new String(System.console().readPassword()) : sc.readLine());
                }
            }
        }

        public String getHelp() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("   %-35s %4s %8s %s\n", "switch", "reqd", "multiple", "help"));
            Arrays.stream(getClass().getDeclaredFields()).filter(f -> f.isAnnotationPresent(Argument.class)).forEach(f -> {
                Argument arg = f.getAnnotation(Argument.class);
                sb.append(String.format(
                        "   %-35s %3s   %4s    %s\n",
                        String.join(", ", arg.keys()) + (f.getType().isAssignableFrom(boolean.class) ? "" : String.format(" <%s>", f.getName())),
                        arg.required() ? "*" : " ",
                        arg.multivalued() ? "*" : " ",
                        arg.help()
                ));
            });
            return sb.toString();
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
                        "Here are some examples for JDBC API execution\n" +
                        "  !md.getSchemas(null, \"MPATAKI%\");\n" +
                        "  !md.getTables(null, \"MPATAKI%\", null, null);\n" +
                        "\n\n" +
                        "All the inputs should end with ; (semicolon)\n\n" +
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
                    System.out.println(on[0] + " is not exposed");
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
    private Configuration conf;
    private ResultSet rs;
    private Statement stmt;

    public Connection connect(String jdbcUrl, String user, String password) throws Exception {
        pstart();
        conn = DriverManager.getConnection(jdbcUrl, user, password);

        $("<div><pre>")
                .$("JDBC connection string : ").$(jdbcUrl).$("\n")
                .$("Driver : ").$(jdbcUrl).$("\n")
                .$("User : ").$(user).$("\n")
                .$("Connecting to DB took : ").$(pend("connecting to db")).$("\n")
                .$("</pre></div>");

        System.out.println("connection status : [" + ((conn != null) ? "successful" : "failed") + "]");

        if (!runTimeHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(this::close));
            runTimeHook = true;
        }

        return conn;
    }

    public boolean close() {
        clog("\nclosing the connection");
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

    private static void clog(String s) {
        System.out.println(s);
    }

    private boolean executeSql(String query) throws Exception {
        return stmt.execute(query);
    }

    static long start, end;
    private FileWriter fw;
    private BufferedReader sc;

    public static void main(String args[]) throws Exception {
        System.out.printf("jdbcc version (%s)\n", VERSION);
        new JDBCClient(args).shell();
    }

    public JDBCClient(String[] args) throws Exception {

        sc = new BufferedReader(new InputStreamReader(System.in));
        conf = new Configuration(args);

        if (conf.inputFile != null) {
            sc.close();
            System.setIn(new FileInputStream(conf.inputFile));
            sc = new BufferedReader(new InputStreamReader(System.in));
        }

        if (conf.help) {
            System.err.println("Usage : java JDBCClient <options>");
            System.err.println("Options: ");
            System.out.println(conf.getHelp());
            System.exit(0);
        }

        if (conf.driver != null) {
            Class.forName(conf.driver);
        }

        if (conf.record) {
            String recordFile = "jdbcc_record_" + "_" + ((new Date()).getTime()) + ".html";
            try {
                fw = new FileWriter(recordFile);
            } catch (Exception e) {
                clog("Failed to open " + recordFile + " " + e.getMessage());
                e.printStackTrace();
            }
            clog("logging to " + recordFile);
            $("<style>")
                    .$("div { padding: 5px 20px; margin: 20px 0px; }")
                    .$(".input { background: #f8f8f8; padding: 5px 0px; }")
                    .$(".output { margin-top: 5px; }")
                    .$(".processing {  }")
                    .$("pre {margin: 0px}")
                    .$("</style>");
        }

        conf.transformers.put("BLOB", new BClobToString());
        conf.transformers.put("CLOB", new BClobToString());
    }

    public void listProps(Object o, String objname) {
        debug(objname);
        for (Method m : o.getClass().getMethods()) {
            if((m.getName().startsWith("get") || m.getName().startsWith("is")) && m.getParameterCount() == 0) {
                try {
                    debug(String.format("\t%s: %s", m.getName(), m.invoke(o)));
                } catch (Exception e) {
                    debug(String.format("\t%s() failed", m.getName()));
                }
            }
        }
    }

    public void shell() throws Exception {

        System.out.println("shell started... ");

        connect(conf.url, conf.user, conf.password);
        stmt = conn.createStatement();

        if (conf.debug) {
            $("<pre>");
            listProps(conn, "connection");
            listProps(conn.getMetaData(), "databasemetadata");
            $("</pre>");
        }

        clog("Type help; for shell help");

        do {
            boolean status = false;
            Object result = null;
            String query = readQuery();

            if (query == null)
                return;

            if (query.trim().isEmpty())
                continue;

            query = query.trim();
            cdebug("Text read from console: " + query);

            if (query.equals("help")) {
                query = "!help()";
            }

            $("<div>").$("<pre class='input'>").$("<b>").$(htmlize(query)).$("</b></pre><pre class='processing'>");

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
            long ellapse = pend(executionMode + " execution", "(status: " + (status ? "successful" : "failed") + ")");

            $("Time: ").$((((double) ellapse) / 1000)).$("s</pre><pre class='output'>");

            if (status) {
                try {
                    rs = null;
                    if (result instanceof ResultSet) {
                        rs = (ResultSet) result;
                    }
                    if (sql && ((boolean) result)) {
                        rs = stmt.getResultSet();
                    }
                    if (rs != null) {
                        printResult(rs);
                    } else {
                        System.out.println(result.toString());
                        $(htmlize(result.toString()));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            $("</pre></div>");
        } while (true);
    }

    private void cdebug(String s) {
        if (conf.debug)
            clog(s);
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

    static String javac;
    static long idx = System.currentTimeMillis();

    Object processJavaCode(DatabaseMetaData dbm, String code) throws Exception {

        if (javac == null) {
            String javaHome = System.getProperty("java.home");
            if (javaHome.endsWith("jre")) {
                javaHome = javaHome + File.separator + "..";
            }
            javac = javaHome + File.separator + "bin" + File.separator + "javac";
            debug("javac = " + javac);
        }

        String cls = "Class" + (++idx), jfile = cls + ".java";
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
        while ((line = br.readLine()) != null) {
            log(line);
        }
        exec.waitFor();
        try {
            ScriptCallable caller = (ScriptCallable) Class.forName(cls).newInstance();
            caller.init(this);
            return caller.call();
        } catch (ClassNotFoundException cnf) {
            clog("Error while compiling the code");
            return null;
        } finally {
            Files.delete(Paths.get(jfile));
            Files.delete(Paths.get(cls + ".class"));
        }
    }

    private void debug(String s) {
        if (conf.debug) {
            log(s);
        }
    }

    private void printResult(ResultSet rset) throws Exception {

        if (rset == null || conf.resultPrintLimit == 0)
            return;

        $("<table border='1' style='border-collapse: collapse;'>");

        /* print cols headers */
        String coln;
        int numCols, maxsize = 0;
        ArrayList<String> cols = new ArrayList<>();
        ResultSetMetaData rsmd = rset.getMetaData();
        for (numCols = 0; true; numCols++) {
            try {
                coln = String.format("%s (%s)", rsmd.getColumnName(numCols + 1), rsmd.getColumnTypeName(numCols + 1));
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

        int rows = 0;
        try {
            pstart();
            for (; (rows != conf.resultPrintLimit) && rset.next(); rows++) {
                $("<tr>");
                for (int i = 0; i < numCols; ++i) {
                    Object data = null;
                    try {
                        data = rset.getObject(i + 1);
                        Transformer tx = null;
                        if (data != null) {
                            tx = conf.transformers.get(cols.get(i));
                            if (tx == null) {
                                tx = conf.transformers.get(rsmd.getColumnTypeName(i + 1).toUpperCase());
                            }
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
        System.out.println();
        pend(String.format("%d rows (%s), time", rows, ((conf.resultPrintLimit == -1 || rows < conf.resultPrintLimit) ? "all" : "limited")));
    }

    private void log(Object o) {
        clog(o.toString());
        $(o.toString()).$("\n");
    }

    private String readQuery() throws Exception {
        String line, prompt = String.format("jdbcc> ");
        StringBuilder sb = new StringBuilder();
        int i = 1;
        do {
            System.out.print(prompt);
            line = sc.readLine();
            if (line == null)
                break;
            if (conf.inputFile != null) {
                System.out.println(line);
            }
            sb.append(line).append(" ");
            prompt = conf.noLineNumbers ? "" : String.format("%5d. ", ++i);
        } while (line.trim().equals("") || !(line.charAt(line.length() - 1) == ';'));
        return sb.toString().trim().isEmpty() ? null : sb.deleteCharAt(sb.length() - 2).toString();
    }

    private static void pstart() {
        start = System.currentTimeMillis();
    }

    private static long pend(String event) throws Exception {
        return pend(event, "");
    }

    private static long pend(String event, String suffix) throws Exception {
        end = System.currentTimeMillis();
        clog(event + ": " + (end - start) + "ms " + suffix);
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
