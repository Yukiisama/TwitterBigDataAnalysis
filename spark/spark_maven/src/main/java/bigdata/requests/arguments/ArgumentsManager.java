package bigdata.requests.arguments;

import bigdata.TPSpark;

public class ArgumentsManager {
    public static void updateProgramArgument(String[] args) {
        if(args.length < 1) { // No arguments
            return;
        }

        for(String arg : args) {
            if(arg.equals("users")) {
                System.out.println("Users option enabled");
                TPSpark.__USERS__ = true;
            } else if(arg.equals("hashtags")) {
                System.out.println("Hashtags option enabled");
                TPSpark.__HASHTAGS__ = true;
            } else if(arg.equals("influencers")) {
                System.out.println("Influencers option enabled");
                TPSpark.__INFLUENCERS__ = true;
            } else if(arg.equals("fresh")) {
                System.out.println("Fresh hbase option enabled");
                TPSpark.__FRESH_HBASE__ = true;
            } else if(arg.equals("help")) {
                System.out.println("Help message");
                TPSpark.__HELP__ = true;
            }
        }

        return;
    }
}
