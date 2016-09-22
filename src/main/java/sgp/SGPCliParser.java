package sgp;

import common.cli.CliParserBase;

import static gp.GPConstants.OPT_STR_GRID_SIZE;

public class SGPCliParser extends CliParserBase {
    public static final String OPT_STR_OUTPUTDIR = "o";
    public static final String OPT_STR_DISTTHRESHOLD = "e";
    public static final String OPT_STR_DENTHRESHOLD = "u";
    public static final String OPT_STR_TIMETHRESHOLD = "dt";
    public static final String OPT_STR_LIFETIMETHRESHOLD = "kc";
    public static final String OPT_STR_CLUSTERNUMTHRESHOLD = "kp";
    public static final String OPT_STR_PARTICIPATORTHRESHOLD = "mp";
    public static final String OPT_STR_NUMPART = "n";

    public SGPCliParser(String[] args) {
        super(args);

        options.addOption(OPT_STR_OUTPUTDIR, true, "output directory (required)");
        options.addOption(OPT_STR_DISTTHRESHOLD, true, "distance threshold" );
        options.addOption(OPT_STR_DENTHRESHOLD, true, "density threshold");
        options.addOption(OPT_STR_TIMETHRESHOLD, true, "time threshold");
        options.addOption(OPT_STR_LIFETIMETHRESHOLD, true, "lifetime");
        options.addOption(OPT_STR_CLUSTERNUMTHRESHOLD, true, "snapshot clusters number threshold");
        options.addOption(OPT_STR_PARTICIPATORTHRESHOLD, true, "participator number threshold");
        options.addOption(OPT_STR_NUMPART, true, "number of partitions");
        options.addOption(OPT_STR_GRID_SIZE, true, "grid size");
    }
}
