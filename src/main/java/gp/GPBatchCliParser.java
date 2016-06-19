package gp;

import common.cli.CliParserBase;
import static gp.GPConstants.*;

public class GPBatchCliParser extends CliParserBase {

    public GPBatchCliParser(String[] args) {
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
