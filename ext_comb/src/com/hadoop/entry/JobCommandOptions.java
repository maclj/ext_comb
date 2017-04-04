package com.hadoop.entry;

import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 命令行参数解析，统一各运行环境下的参数。
 * 
 * -i input:输入路径<br>
 * -o output:输出路径<br>
 * -d date:日期（也可以是月份，仅支持yyyy-MM-dd的格式）<br>
 * 
 * 
 *
 */
public class JobCommandOptions {

    @SuppressWarnings("static-access")
    public static final Option OPTION_INPUT = OptionBuilder.withLongOpt("input").hasArg().isRequired(false)
            .withDescription("Specify the input path").create("i");

    @SuppressWarnings("static-access")
    public static final Option OPTION_OUTPUT = OptionBuilder.withLongOpt("output").hasArg().isRequired(false)
            .withDescription("Specify the input path").create("o");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DATE = OptionBuilder.withLongOpt("date").hasArg().isRequired(false)
            .withDescription("Specify the date").create("d");
    
    @SuppressWarnings("static-access")
    public static final Option OPTION_HELP = OptionBuilder.withLongOpt("help").isRequired(false)
            .withDescription("print help").create("h");
    @SuppressWarnings("static-access")
    public static final Option OPTION_CITY = OptionBuilder.withLongOpt("city").hasArg().isRequired(false)
            .withDescription("Specify the city").create("c");
    /** GenericOptionsParser */
    private GenericOptionsParser parser;
    
    /** CommandLine */
    private CommandLine commandLine;

    /**
     * Constructor
     * @param conf
     * @param args
     * @throws Exception
     */
    public JobCommandOptions(Configuration conf, String[] args) throws Exception {

        Options options = new Options();
        options.addOption(OPTION_INPUT);
        options.addOption(OPTION_OUTPUT);
        options.addOption(OPTION_DATE);
        options.addOption(OPTION_CITY);
        options.addOption(OPTION_HELP);

        this.parser = new GenericOptionsParser(conf, options, args);
        this.commandLine = parser.getCommandLine();
    }

    public String[] getRemainingArgs() {
        return this.parser.getRemainingArgs();
    }

    public String getOptionValue(Option option) {
        return commandLine.getOptionValue(option.getOpt());
    }
    
    public String[] getOptionValues(Option option) {
        return commandLine.getOptionValues(option.getOpt());
    }

    public String getOptionValue(String opt) {
        return commandLine.getOptionValue(opt);
    }
    
    public String[] getOptionValues(String opt) {
        return commandLine.getOptionValues(opt);
    }

    public boolean hasOption(Option option) {
        return commandLine.hasOption(option.getOpt());
    }

    public boolean hasOption(String opt) {
        return commandLine.hasOption(opt);
    }

    public String getDate() {
        return getOptionValue(OPTION_DATE);
    }

    public String getInput() {
        return getOptionValue(OPTION_INPUT);
    }
    
    public String[] getInputs() {
        return getOptionValues(OPTION_INPUT);
    }

    public String getOutput() {
        return getOptionValue(OPTION_OUTPUT);
    }
    
    public String[] getOutputs() {
        return getOptionValues(OPTION_OUTPUT);
    }

    public String getCity() {
        return getOptionValue(OPTION_CITY);
    }
    /**
     * 打印输出
     * @param out
     */
    public static void printGenericCommandUsage(PrintStream out) {
        out.println("Generic options supported are");
        out.println("-i --input        <input file>       specify the application input file");
        out.println("-o --output       <output file>      specify the application output file");
        out.println("-d --date         <date(yyyy-MM-dd)> specify date");
        out.println("-h --help                            print help");
        out.println("bin/hadoop command [genericOptions] [commandOptions]\n");
    }

}
