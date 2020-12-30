package bigdata.requests;

import bigdata.requests.hashtags.RequestHashtags;
import bigdata.requests.users.RequestUsers;

public enum EntryPoint {
    

    HASHTAGS_DAILY_TOPK {
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof Integer)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@HASHTAGS_DAILY_TOPK. Required argument(s) are: (Integer).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@HASHTAGS_DAILY_TOPK. Required argument(s) are: (Integer).");
            }

            int k = (Integer) args[0];

            RequestHashtags.mostUsedHashtags(k);
        }
    },
    

    HASHTAGS_BEST_ALL_FILES_TOPK {
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof Integer)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@HASHTAGS_BEST_ALL_FILES_TOPK. Required argument(s) are: (Integer).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@HASHTAGS_BEST_ALL_FILES_TOPK. Required argument(s) are: (Integer).");
            }

            int k = (Integer) args[0];

            RequestHashtags.mostUsedHashtagsOnAllFiles(k);
        }
    },
    

    HASHTAGS_APPARITIONS_COUNT {
        @Override
        public void apply (Object ... args) {
            RequestHashtags.numberOfApparitions();
        }
    },
    

    HASHTAGS_USED_BY {
        @Override
        public void apply (Object ... args) {

            RequestHashtags.usersList();
        }
    },

    USERS_UNIQUE_HASHTAGS_LIST {        
        @Override
        public void apply (Object ... args) {
            RequestUsers.UserUniqueHashtagsList();
        }
    };

    public abstract void apply(Object ... args);
}
