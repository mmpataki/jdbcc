//package com.informatica.gcs.tools;

import java.util.Date;
import java.util.Scanner;

import java.io.FileWriter;
import java.io.IOException;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class JDBCClient implements Runnable {

    static Object[][] registeredArgs = new Object[][]{
        {"-d", "driver class name", true, null},
        {"-c", "JDBC connection string", true},
        {"-u", "username", true},
        {"-p", "password", true},
        {"-r", "record_file_name", false}
    };

    String url;
    String user;
    String driver;
    String record;
    String password;
    long start, end;

    private FileWriter fw;
    private Scanner sc = null;
    private Connection conn = null;
    HashMap<String, String> argMap = new HashMap<>();

    public static void main(String args[]) throws Exception {
        boolean required;
        System.err.println("Usage : java JDBCClient <options>");
        System.err.println("Options: ");
        for (Object[] regArg : registeredArgs) {
            required = (boolean) regArg[2];
            System.err.println("\t" + (required?"":"[") + regArg[0] + (required?"":"]") + " <" + regArg[1] + ">");
        }
        System.err.println("Passed args : " + Arrays.toString(args));
        new JDBCClient(args).start();
    }

    public JDBCClient(String[] args) {

        sc = new Scanner(System.in);

        /* register the args */
        for (Object[] requiredArg : registeredArgs) {
            argMap.put((String) requiredArg[0], null);
        }

        /* parse the args */
        for (int i = 0; (i+1) < args.length; i += 2) {
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
        record = argMap.get("-r");

        System.err.println("Parsed args : " + argMap);
        
        if (record != null) {
            String recordFile = "jdbcc_record_" + record + "_" + ((new Date()).getTime()) + ".html";
            try {
                fw = new FileWriter(recordFile);
            } catch (Exception e) {
                System.out.println("Failed to open " + recordFile + " " + e.getMessage());
                e.printStackTrace();
            }
            log("Logging to " + recordFile);
        }
    }

    /* short hand for record */
    public JDBCClient $(Object o) {
        if (fw != null) {
            if(o == null)
                o = "<NULL>";
            try {
                fw.write(o.toString());
                fw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return this;
    }

    public void start() throws Exception {

        long etime;
        String query;
        long conntime;
        Statement stmt;
        boolean status;

        Class.forName(driver);
        pstart();
        conn = DriverManager.getConnection(url, user, password);
        conntime = pend("Connecting to DB");
        status = (conn != null);

        $("<pre>")
            .$("JDBC connection string : ").$(url).$("\n")
            .$("Driver : ").$(driver).$("\n")
            .$("User : ").$(user).$("\n")
            .$("Connecting to DB took : ").$(conntime).$("\n")
        .$("</pre>");

        /* add a shutdown hook */
        Runtime.getRuntime().addShutdownHook(new Thread(this));

        log("connection status : [" + (status ? "successful" : "failed") + "]");

        stmt = conn.createStatement();
        do {
            query = readQuery();
            log("executing [" + query + "]");
            pstart();
            try {
                stmt.execute(query);
                status = true;
            } catch (SQLException ex) {
                ex.printStackTrace();
                status = false;
            }
            etime = pend("query execution");
            log("query execution status : [" + (status ? "successful" : "failed") + "]");

            /* flush the results */
            try {
                printResult(query, etime, stmt.getResultSet());
            } catch (Exception e) {
                /* Yes, printing tables can throw exceptions */
                e.printStackTrace();
            }
        } while (true);
    }

    private void printResult(String query, long time, ResultSet rset) throws Exception {

        if(rset == null)
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
                coln = rsmd.getColumnName(numCols+1);
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
                $("<tr>");
                for (int i = 0; i < numCols; ++i) {
                    Object data = null;
                    try {
                        data = rset.getObject(i + 1);
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

    private void log(Object o) {
        System.out.println(o.toString());
    }

    private String readQuery() {
        String line = null;
        StringBuilder sb = new StringBuilder();
        do {
            System.out.print(line == null ? "jdbcc> " : "     > ");
            line = sc.nextLine().trim();
            sb.append(line).append(" ");
        } while (line.trim().equals("") || !(line.charAt(line.length() - 1) == ';'));
        return sb.deleteCharAt(sb.length() - 2).toString();
    }

    private void pstart() {
        start = System.currentTimeMillis();
    }

    private long pend(String event) throws Exception {
        end = System.currentTimeMillis();
        log(event + " took : " + (end - start) + "ms");
        return (end - start);
    }

    private String htmlize(String s) {
        s = s.replace(">", "&gt;");
        s = s.replace("<", "&lt;");
        s = s.replace("&", "&amp;");
        s = s.replace(" ", "&nbsp;");
        return s;
    }

    /* shutdown hook */
    public void run() {
        try {
            log("\nclosing the connection");
        } catch (Exception e) {
        }
        try {
            conn.close();
            if(fw != null)
                fw.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

