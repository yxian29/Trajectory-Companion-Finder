package Utils;


import org.apache.commons.cli.*;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Cli {

    public static final String OPT_STR_HELP = "h";
    public static final String OPT_STR_HELP_ALT = "help";
    public static final String OPT_STR_DEBUG = "debug";
    public static final String OPT_STR_INPUTFILE = "i";
    public static final String OPT_STR_OUTPUTDIR = "o";
    public static final String OPT_STR_DISTTHRESHOLD = "e";
    public static final String OPT_STR_DENTHRESHOLD = "u";
    public static final String OPT_STR_TIMEINTERVAL = "T";
    public static final String OPT_STR_LIFETIME = "k";
    public static final String OPT_STR_NUMPART = "n";

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";

    private static final Logger log = Logger.getLogger(Cli.class.getName());
    private String[] args = null;
    private Options options = new Options();
    private CommandLineParser parser = new BasicParser();
    private CommandLine cmd = null;

    public Cli(String[] args) {

        this.args = args;

        options.addOption(OPT_STR_HELP, false, "show help");
        options.addOption(OPT_STR_HELP_ALT, false, "show help");
        options.addOption(OPT_STR_DEBUG, false, "debug mode. (force local mode)");
        options.addOption(OPT_STR_INPUTFILE, true, "input file (required)");
        options.addOption(OPT_STR_OUTPUTDIR, true, "output directory (required)");
        options.addOption(OPT_STR_DISTTHRESHOLD, true, "distance threshold" );
        options.addOption(OPT_STR_DENTHRESHOLD, true, "density threshold");
        options.addOption(OPT_STR_TIMEINTERVAL, true, "time interval of a trajectory slot");
        options.addOption(OPT_STR_LIFETIME, true, "duration threshold");
        options.addOption(OPT_STR_NUMPART, true, "number of sub-partitions");
    }

    public CommandLine getCmd()
    {
        return cmd;
    }

    public void parse() {
        try
        {
            cmd = parser.parse(options, args);

            if(cmd.hasOption(OPT_STR_HELP) || cmd.hasOption(OPT_STR_HELP_ALT))
                help();

        }
        catch (ParseException e) {
            log.log(Level.SEVERE, "Failed to parse command line properties", e);
            help();
        }
    }

    public void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TCFinder", options);
        System.exit(0);
    }

}
