package sg;
import common.cmd.CmdParserBase;


public class SGCmdParser extends CmdParserBase {

    public static final String OPT_STR_INPUTFILE = "i";
    public static final String OPT_STR_TOPIC = "t";
    public static final String OPT_STR_BROKER = "b";
    public static final String OPT_STR_TIMESCALE = "s";
    public static final String OPT_STR_WINDOW_DURATION = "w";

    public SGCmdParser(String[] args) {
        super(args);

        options.addOption(OPT_STR_HELP, false, "show help");
        options.addOption(OPT_STR_HELP_ALT, false, "show help");
        options.addOption(OPT_STR_DEBUG, false, "debug mode. (force local mode)");
        options.addOption(OPT_STR_INPUTFILE, true, "input file (required)");
        options.addOption(OPT_STR_TOPIC, true, "stream topic (required)");
        options.addOption(OPT_STR_BROKER, true, "broker hostname. e.g. localhost:9092 (required)" );
        options.addOption(OPT_STR_TIMESCALE, true, "Publishing speed");
        options.addOption(OPT_STR_WINDOW_DURATION, true, "Time slot window duration");

    }



}


