package gp;

import common.cmd.CmdParserBase;

public class GPCmdParser extends CmdParserBase {

    public static final String OPT_STR_INPUTFILE = "i";
    public static final String OPT_STR_OUTPUTDIR = "o";
    public static final String OPT_STR_DISTTHRESHOLD = "e";
    public static final String OPT_STR_DENTHRESHOLD = "u";
    public static final String OPT_STR_TIMETHRESHOLD = "dt";
    public static final String OPT_STR_LIFETIMETHRESHOLD = "kc";
    public static final String OPT_STR_CLUSTERNUMTHRESHOLD = "kp";
    public static final String OPT_STR_PARTICIPATORTHRESHOLD = "mp";
    public static final String OPT_STR_NUMPART = "n";

    public GPCmdParser(String[] args) {
        super(args);

        options.addOption(OPT_STR_INPUTFILE, true, "input file (required)");
        options.addOption(OPT_STR_OUTPUTDIR, true, "output directory (required)");
        options.addOption(OPT_STR_DISTTHRESHOLD, true, "distance threshold" );
        options.addOption(OPT_STR_DENTHRESHOLD, true, "density threshold");
        options.addOption(OPT_STR_TIMETHRESHOLD, true, "time threshold");
        options.addOption(OPT_STR_LIFETIMETHRESHOLD, true, "lifetime");
        options.addOption(OPT_STR_CLUSTERNUMTHRESHOLD, true, "snapshot clusters number threshold");
        options.addOption(OPT_STR_PARTICIPATORTHRESHOLD, true, "participator number threshold");
        options.addOption(OPT_STR_NUMPART, true, "number of partitions");
    }
}
