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
    

    HASHTAGS_BEST_WITH_COUNTS {
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof Integer)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@HASHTAGS_BEST_WITH_COUNTS. Required argument(s) are: (Integer).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@HASHTAGS_BEST_WITH_COUNTS. Required argument(s) are: (Integer).");
            }

            int k = (Integer) args[0];

            RequestHashtags.mostUsedHashtagsWithCount(k);
        }
    },
    

    HASHTAGS_APPARITIONS_COUNT {
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof String)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@HASHTAGS_APPARITIONS_COUNT. Required argument(s) are: (String).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@HASHTAGS_APPARITIONS_COUNT. Required argument(s) are: (String).");
            }

            String k = (String) args[0];

            RequestHashtags.numberOfApparitions(k);
        }
    },
    

    HASHTAGS_USED_BY {
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof String)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@HASHTAGS_USED_BY. Required argument(s) are: (String).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@HASHTAGS_USED_BY. Required argument(s) are: (String).");
            }

            String k = (String) args[0];

            RequestHashtags.usersList(k);
        }
    },

    USERS_UNIQUE_HASHTAGS_LIST {        
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof String)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@USERS_UNIQUE_HASHTAGS_LIST. Required argument(s) are: (String).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@USERS_UNIQUE_HASHTAGS_LIST. Required argument(s) are: (String).");
            }

            String user_name = (String) args[0];

            RequestUsers.UserUniqueHashtagsList(user_name);
        }
    },

    USERS_NUMBER_OF_TWEETS {        
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof String)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@USERS_NUMBER_OF_TWEETS. Required argument(s) are: (String).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@USERS_NUMBER_OF_TWEETS. Required argument(s) are: (String).");
            }

            String user_name = (String) args[0];

            RequestUsers.UserNumberOfTweets(user_name);
        }
    },

    USERS_NUMBER_OF_TWEETS_PER_LANGAGE {        
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof String)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@USERS_NUMBER_OF_TWEETS_PER_LANGAGE. Required argument(s) are: (String).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@USERS_NUMBER_OF_TWEETS_PER_LANGAGE. Required argument(s) are: (String).");
            }

            String user_name = (String) args[0];

            RequestUsers.TweetsPerLangagesAndTimestamp(user_name);
        }
    },

    USERS_MOST_LIKED_TWEET {        
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof String)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@USERS_MOST_LIKED_TWEET. Required argument(s) are: (String).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@USERS_MOST_LIKED_TWEET. Required argument(s) are: (String).");
            }

            String user_name = (String) args[0];

            RequestUsers.UserMostFavoredTweet(user_name);
        }
    },

    USERS_MOST_RETWEETED_TWEET {        
        @Override
        public void apply (Object ... args) {
            if(args.length == 1) {
                if(!(args[0] instanceof String)) {
                    throw new IllegalArgumentException("Invalid arguments for EntryPoint@USERS_MOST_RETWEETED_TWEET. Required argument(s) are: (String).");
                }
            } else {
                throw new IllegalArgumentException("Invalid number of arguments for EntryPoint@USERS_MOST_RETWEETED_TWEET. Required argument(s) are: (String).");
            }

            String user_name = (String) args[0];

            RequestUsers.UserMostRetweetedTweet(user_name);
        }
    };


    public abstract void apply(Object ... args);
}
