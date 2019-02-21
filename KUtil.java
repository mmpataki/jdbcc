import java.util.Scanner;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;

import org.apache.log4j.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

class KUtil {

	public static void main(String args[]) throws Exception {
		System.err.println("Usage : KUtil <options> <util_class_name_to_run>");
		System.err.println("Options: ");
		for (Object[] regArg : registeredArgs) {
			boolean required = (boolean) regArg[2];
			System.err.println("\t" + (required?"":"[") + regArg[0] + (required?"":"]") + " <" + regArg[1] + ">");
		}
		System.err.println("Passed args : " + Arrays.toString(args));
		new KUtil().start(args);
	}

	static Object registeredArgs[][] = new Object[][] {
			{"-u", "principal name", true},
			{"-k", "keytab_path", true},
			{"-conf(i)", "conf-file", false}
		};
	HashMap<String, String> argMap = new HashMap<>();
	String utilClass = "", user = "", keytab = "";
	Scanner sc;
	Integer utilClassIndex=0;

	public void start(String args[]) throws Exception {

		sc = new Scanner(System.in);

        	/* register the args */
		for (Object[] requiredArg : registeredArgs) {
			argMap.put((String) requiredArg[0], null);
		}

		/* parse the args */
		utilClassIndex = 0;
		for (int i = 0; i < args.length; i+=2) {
			if(args[i].startsWith("-") && (i+1)<args.length) {
				argMap.put(args[i], args[i + 1]);
			} else {
				utilClassIndex = i;
				break;				
			}
		}

		if(utilClassIndex == 0 && args.length == 0) {
			System.out.println("No util specified, exitting");
			return;
		}

		/* ask for the left out args, if they are required */
		for (Object[] regArg : registeredArgs) {
			if (((Boolean) regArg[2]) && argMap.get((String) regArg[0]) == null) {
				System.out.print("Enter " + regArg[1] + ": ");
				argMap.put((String) regArg[0], sc.nextLine());
			}
		}

		user = argMap.get("-u");
		keytab = argMap.get("-k");
		utilClass = args[utilClassIndex];
		Configuration conf = new Configuration();
		
		for(String key: argMap.keySet()) {
			if(key.contains("-cf")) {
				String cf = argMap.get(key);
				log("loading conf from: " + cf);
				conf.addResource(new Path("file://" + cf));
			}
		}
	
		for(String key: argMap.keySet()) {
			if(key.contains("-cv")) {
				String cv = argMap.get(key);
				log("adding conf from cmdline: " + cv);
				String kvp[] = cv.split("=");
				conf.set(kvp[0], kvp[1]);
			}
		}

		String[] dummy = new String[0];
		Class clazz = Class.forName(utilClass);
		Method mainMethod = clazz.getMethod("main", String[].class);

		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

		if(conf.get("hadoop.security.authentication").equals("kerberos"))
			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab);
	
		ugi.doAs(
			new PrivilegedAction<Boolean> () {
				public Boolean run() {
					try {
						log("Runninng the tool : " + utilClass);
						mainMethod.invoke(
							null,
							(Object)Arrays.copyOfRange(args, utilClassIndex+1, args.length)
						);
						return true;
					} catch(Exception e) {
						e.printStackTrace();
					}
					return false;
				}
			}
		);
	}
	public void log(String s) {
		System.out.println(s);
	}
}
