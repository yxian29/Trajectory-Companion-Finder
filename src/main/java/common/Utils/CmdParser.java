package common.Utils;

import TrajectoryCompanion.TCCmdParser;
import org.apache.commons.cli.*;

import java.util.logging.Level;
import java.util.logging.Logger;

public class CmdParser {

    public static final String OPT_STR_HELP = "h";
    public static final String OPT_STR_HELP_ALT = "help";
    public static final String OPT_STR_DEBUG = "debug";

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";

    protected static final Logger log = Logger.getLogger(TCCmdParser.class.getName());
    protected String[] args = null;
    protected Options options = new Options();
    protected CommandLineParser parser = new BasicParser();
    protected CommandLine cmd = null;

    public CmdParser(String[] args) {

        this.args = args;

        options.addOption(OPT_STR_HELP, false, "show help");
        options.addOption(OPT_STR_HELP_ALT, false, "show help");
        options.addOption(OPT_STR_DEBUG, false, "debug mode. (force local mode)");
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
        formatter.printHelp("Help", options);
        System.exit(0);
    }
}
