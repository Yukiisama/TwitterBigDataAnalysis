package bigdata.requests.arguments;

import bigdata.TPSpark;
import static bigdata.TPSpark.logger;

public class ArgumentsManager {
    public static void updateProgramArgument(String[] args) {
        if(args.length < 1) { // No arguments
            return;
        }

        for(String arg : args) {
            if(arg.equals("users")) {
                logger.debug("Users option enabled");
                TPSpark.__USERS__ = true;
            } else if(arg.equals("hashtags")) {
                logger.debug("Hashtags option enabled");
                TPSpark.__HASHTAGS__ = true;
            } else if(arg.equals("influencers")) {
                logger.debug("Influencers option enabled");
                TPSpark.__INFLUENCERS__ = true;
            } else if(arg.equals("fresh")) {
                logger.debug("Fresh hbase option enabled");
                TPSpark.__FRESH_HBASE__ = true;
            } else if(arg.equals("help")) {
                logger.debug("Help message");
                TPSpark.__HELP__ = true;
            }
        }

        return;
    }
}
