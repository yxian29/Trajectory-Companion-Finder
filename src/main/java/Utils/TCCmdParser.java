package Utils;

public class TCCmdParser extends CmdParser{

    public static final String OPT_STR_INPUTFILE = "i";
    public static final String OPT_STR_OUTPUTDIR = "o";
    public static final String OPT_STR_DISTTHRESHOLD = "e";
    public static final String OPT_STR_DENTHRESHOLD = "u";
    public static final String OPT_STR_TIMEINTERVAL = "T";
    public static final String OPT_STR_LIFETIME = "k";
    public static final String OPT_STR_SIZETHRESHOLD = "l";
    public static final String OPT_STR_NUMPART = "n";

    public TCCmdParser(String[] args) {
        super(args);

        options.addOption(OPT_STR_HELP, false, "show help");
        options.addOption(OPT_STR_HELP_ALT, false, "show help");
        options.addOption(OPT_STR_DEBUG, false, "debug mode. (force local mode)");
        options.addOption(OPT_STR_INPUTFILE, true, "input file (required)");
        options.addOption(OPT_STR_OUTPUTDIR, true, "output directory (required)");
        options.addOption(OPT_STR_DISTTHRESHOLD, true, "distance threshold" );
        options.addOption(OPT_STR_DENTHRESHOLD, true, "density threshold");
        options.addOption(OPT_STR_TIMEINTERVAL, true, "time interval of a trajectory slot");
        options.addOption(OPT_STR_LIFETIME, true, "duration threshold");
        options.addOption(OPT_STR_NUMPART, true, "number of partitions");
        options.addOption(OPT_STR_SIZETHRESHOLD, true, "size threshold");
    }



}
