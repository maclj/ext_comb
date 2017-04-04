package com.hadoop.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/**
 * 此类仅用于测试类装载，不要在项目中使用。
 * 
 *
 */
public class GenericOptionsParser {

    private static final Log LOG = LogFactory.getLog(GenericOptionsParser.class);
    private Configuration conf;
    private CommandLine commandLine;

    /**
     * Create an options parser with the given options to parse the args.
     * @param opts the options
     * @param args the command line arguments
     * @throws IOException 
     */
    public GenericOptionsParser(Options opts, String[] args) 
        throws IOException {
      this(new Configuration(), opts, args);
    }

    /**
     * Create an options parser to parse the args.
     * @param args the command line arguments
     * @throws IOException 
     */
    public GenericOptionsParser(String[] args) 
        throws IOException {
      this(new Configuration(), new Options(), args);
    }
    
    /** 
     * Create a <code>GenericOptionsParser<code> to parse only the generic Hadoop  
     * arguments. 
     * 
     * The array of string arguments other than the generic arguments can be 
     * obtained by {@link #getRemainingArgs()}.
     * 
     * @param conf the <code>Configuration</code> to modify.
     * @param args command-line arguments.
     * @throws IOException 
     */
    public GenericOptionsParser(Configuration conf, String[] args) 
        throws IOException {
      this(conf, new Options(), args); 
    }

    /** 
     * Create a <code>GenericOptionsParser</code> to parse given options as well 
     * as generic Hadoop options. 
     * 
     * The resulting <code>CommandLine</code> object can be obtained by 
     * {@link #getCommandLine()}.
     * 
     * @param conf the configuration to modify  
     * @param options options built by the caller 
     * @param args User-specified arguments
     * @throws IOException 
     */
    public GenericOptionsParser(Configuration conf,
        Options options, String[] args) throws IOException {
      parseGeneralOptions(options, conf, args);
      this.conf = conf;
    }

    /**
     * Returns an array of Strings containing only application-specific arguments.
     * 
     * @return array of <code>String</code>s containing the un-parsed arguments
     * or <strong>empty array</strong> if commandLine was not defined.
     */
    public String[] getRemainingArgs() {
      return (commandLine == null) ? new String[]{} : commandLine.getArgs();
    }

    /**
     * Get the modified configuration
     * @return the configuration that has the modified parameters.
     */
    public Configuration getConfiguration() {
      return conf;
    }

    /**
     * Returns the commons-cli <code>CommandLine</code> object 
     * to process the parsed arguments. 
     * 
     * Note: If the object is created with 
     * {@link #GenericOptionsParser(Configuration, String[])}, then returned 
     * object will only contain parsed generic options.
     * 
     * @return <code>CommandLine</code> representing list of arguments 
     *         parsed against Options descriptor.
     */
    public CommandLine getCommandLine() {
      return commandLine;
    }

    /**
     * Specify properties of each generic option
     */
    @SuppressWarnings("static-access")
    private static Options buildGeneralOptions(Options opts) {
      Option fs = OptionBuilder.withArgName("local|namenode:port")
      .hasArg()
      .withDescription("specify a namenode")
      .create("fs");
      Option jt = OptionBuilder.withArgName("local|resourcemanager:port")
      .hasArg()
      .withDescription("specify a ResourceManager")
      .create("jt");
      Option oconf = OptionBuilder.withArgName("configuration file")
      .hasArg()
      .withDescription("specify an application configuration file")
      .create("conf");
      Option property = OptionBuilder.withArgName("property=value")
      .hasArg()
      .withDescription("use value for given property")
      .create('D');
      Option libjars = OptionBuilder.withArgName("paths")
      .hasArg()
      .withDescription("comma separated jar files to include in the classpath.")
      .create("libjars");
      Option files = OptionBuilder.withArgName("paths")
      .hasArg()
      .withDescription("comma separated files to be copied to the " +
             "map reduce cluster")
      .create("files");
      Option archives = OptionBuilder.withArgName("paths")
      .hasArg()
      .withDescription("comma separated archives to be unarchived" +
                       " on the compute machines.")
      .create("archives");
      
      // file with security tokens
      Option tokensFile = OptionBuilder.withArgName("tokensFile")
      .hasArg()
      .withDescription("name of the file with the tokens")
      .create("tokenCacheFile");

      opts.addOption(fs);
      opts.addOption(jt);
      opts.addOption(oconf);
      opts.addOption(property);
      opts.addOption(libjars);
      opts.addOption(files);
      opts.addOption(archives);
      opts.addOption(tokensFile);

      return opts;
    }

    /**
     * Modify configuration according user-specified generic options
     * @param conf Configuration to be modified
     * @param line User-specified generic options
     */
    private void processGeneralOptions(Configuration conf,
        CommandLine line) throws IOException {
      if (line.hasOption("fs")) {
        FileSystem.setDefaultUri(conf, line.getOptionValue("fs"));
      }

      if (line.hasOption("jt")) {
        String optionValue = line.getOptionValue("jt");
        if (optionValue.equalsIgnoreCase("local")) {
          conf.set("mapreduce.framework.name", optionValue);
        }

        conf.set("yarn.resourcemanager.address", optionValue, 
            "from -jt command line option");
      }
      if (line.hasOption("conf")) {
        String[] values = line.getOptionValues("conf");
        for(String value : values) {
          conf.addResource(new Path(value));
        }
      }
      if (line.hasOption("libjars")) {
        conf.set("tmpjars", 
                 validateFiles(line.getOptionValue("libjars"), conf),
                 "from -libjars command line option");
        //setting libjars in client classpath
        URL[] libjars = getLibJars(conf);
        if(libjars!=null && libjars.length>0) {
          conf.setClassLoader(new StandardClassLoader(libjars, conf.getClassLoader()));
          Thread.currentThread().setContextClassLoader(
              new StandardClassLoader(libjars, 
                  Thread.currentThread().getContextClassLoader()));
        }
      }
      if (line.hasOption("files")) {
        conf.set("tmpfiles", 
                 validateFiles(line.getOptionValue("files"), conf),
                 "from -files command line option");
      }
      if (line.hasOption("archives")) {
        conf.set("tmparchives", 
                  validateFiles(line.getOptionValue("archives"), conf),
                  "from -archives command line option");
      }
      if (line.hasOption('D')) {
        String[] property = line.getOptionValues('D');
        for(String prop : property) {
          String[] keyval = prop.split("=", 2);
          if (keyval.length == 2) {
            conf.set(keyval[0], keyval[1], "from command line");
          }
        }
      }
      conf.setBoolean("mapreduce.client.genericoptionsparser.used", true);
      
      // tokensFile
      if(line.hasOption("tokenCacheFile")) {
        String fileName = line.getOptionValue("tokenCacheFile");
        // check if the local file exists
        FileSystem localFs = FileSystem.getLocal(conf);
        Path p = localFs.makeQualified(new Path(fileName));
        if (!localFs.exists(p)) {
            throw new FileNotFoundException("File "+fileName+" does not exist.");
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("setting conf tokensFile: " + fileName);
        }
        UserGroupInformation.getCurrentUser().addCredentials(
            Credentials.readTokenStorageFile(p, conf));
        conf.set("mapreduce.job.credentials.binary", p.toString(),
                 "from -tokenCacheFile command line option");

      }
    }
    
    /**
     * If libjars are set in the conf, parse the libjars.
     * @param conf
     * @return libjar urls
     * @throws IOException
     */
    public static URL[] getLibJars(Configuration conf) throws IOException {
      String jars = conf.get("tmpjars");
      if(jars==null) {
        return null;
      }
      String[] files = jars.split(",");
      List<URL> cp = new ArrayList<URL>();
      for (String file : files) {
        Path tmp = new Path(file);
        if (tmp.getFileSystem(conf).equals(FileSystem.getLocal(conf))) {
          cp.add(FileSystem.getLocal(conf).pathToFile(tmp).toURI().toURL());
        } else {
          LOG.warn("The libjars file " + tmp + " is not on the local " +
            "filesystem. Ignoring.");
        }
      }
      return cp.toArray(new URL[0]);
    }

    /**
     * takes input as a comma separated list of files
     * and verifies if they exist. It defaults for file:///
     * if the files specified do not have a scheme.
     * it returns the paths uri converted defaulting to file:///.
     * So an input of  /home/user/file1,/home/user/file2 would return
     * file:///home/user/file1,file:///home/user/file2
     * @param files
     * @return
     */
    private String validateFiles(String files, Configuration conf) 
        throws IOException  {
      if (files == null) 
        return null;
      String[] fileArr = files.split(",");
      if (fileArr.length == 0) {
        throw new IllegalArgumentException("File name can't be empty string");
      }
      String[] finalArr = new String[fileArr.length];
      for (int i =0; i < fileArr.length; i++) {
        String tmp = fileArr[i];
        if (tmp.isEmpty()) {
          throw new IllegalArgumentException("File name can't be empty string");
        }
        String finalPath;
        URI pathURI;
        try {
          pathURI = new URI(tmp);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path path = new Path(pathURI);
        FileSystem localFs = FileSystem.getLocal(conf);
        if (pathURI.getScheme() == null) {
          //default to the local file system
          //check if the file exists or not first
          if (!localFs.exists(path)) {
            throw new FileNotFoundException("File " + tmp + " does not exist.");
          }
          finalPath = path.makeQualified(localFs.getUri(),
              localFs.getWorkingDirectory()).toString();
        }
        else {
          // check if the file exists in this file system
          // we need to recreate this filesystem object to copy
          // these files to the file system ResourceManager is running
          // on.
          FileSystem fs = path.getFileSystem(conf);
          if (!fs.exists(path)) {
            throw new FileNotFoundException("File " + tmp + " does not exist.");
          }
          finalPath = path.makeQualified(fs.getUri(),
              fs.getWorkingDirectory()).toString();
        }
        finalArr[i] = finalPath;
      }
      return StringUtils.arrayToString(finalArr);
    }

    /**
     * Windows powershell and cmd can parse key=value themselves, because
     * /pkey=value is same as /pkey value under windows. However this is not
     * compatible with how we get arbitrary key values in -Dkey=value format.
     * Under windows -D key=value or -Dkey=value might be passed as
     * [-Dkey, value] or [-D key, value]. This method does undo these and
     * return a modified args list by manually changing [-D, key, value]
     * into [-D, key=value]
     *
     * @param args command line arguments
     * @return fixed command line arguments that GnuParser can parse
     */
    private String[] preProcessForWindows(String[] args) {
      if (!Shell.WINDOWS) {
        return args;
      }
      if (args == null) {
        return null;
      }
      List<String> newArgs = new ArrayList<String>(args.length);
      for (int i=0; i < args.length; i++) {
        String prop = null;
        if (args[i].equals("-D")) {
          newArgs.add(args[i]);
          if (i < args.length - 1) {
            prop = args[++i];
          }
        } else if (args[i].startsWith("-D")) {
          prop = args[i];
        } else {
          newArgs.add(args[i]);
        }
        if (prop != null) {
          if (prop.contains("=")) {
            // everything good
          } else {
            if (i < args.length - 1) {
              prop += "=" + args[++i];
            }
          }
          newArgs.add(prop);
        }
      }

      return newArgs.toArray(new String[newArgs.size()]);
    }

    /**
     * Parse the user-specified options, get the generic options, and modify
     * configuration accordingly
     * @param opts Options to use for parsing args.
     * @param conf Configuration to be modified
     * @param args User-specified arguments
     */
    private void parseGeneralOptions(Options opts, Configuration conf, 
        String[] args) throws IOException {
      opts = buildGeneralOptions(opts);
      CommandLineParser parser = new GnuParser();
      try {
        commandLine = parser.parse(opts, preProcessForWindows(args), true);
        processGeneralOptions(conf, commandLine);
      } catch(ParseException e) {
        LOG.warn("options parsing failed: "+e.getMessage());

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("general options are: ", opts);
      }
    }

    /**
     * Print the usage message for generic command-line options supported.
     * 
     * @param out stream to print the usage message to.
     */
    public static void printGenericCommandUsage(PrintStream out) {
      
      out.println("Generic options supported are");
      out.println("-conf <configuration file>     specify an application configuration file");
      out.println("-D <property=value>            use value for given property");
      out.println("-fs <local|namenode:port>      specify a namenode");
      out.println("-jt <local|resourcemanager:port>    specify a ResourceManager");
      out.println("-files <comma separated list of files>    " + 
        "specify comma separated files to be copied to the map reduce cluster");
      out.println("-libjars <comma separated list of jars>    " +
        "specify comma separated jar files to include in the classpath.");
      out.println("-archives <comma separated list of archives>    " +
                  "specify comma separated archives to be unarchived" +
                  " on the compute machines.\n");
      out.println("The general command line syntax is");
      out.println("bin/hadoop command [genericOptions] [commandOptions]\n");
    }
    
  }
