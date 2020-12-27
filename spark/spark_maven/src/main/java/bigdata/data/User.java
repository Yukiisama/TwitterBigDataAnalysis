package bigdata.data;

import java.io.Serializable;
import java.util.Set;


public class User implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 4569293528740761201L;

    
    private int nb_tweets;
    private Set<String> hashtags;
    private String id;

    public User(String id, Set<String> hashtags, int nb_tweets) {
        this.id = id;
        this.setHashtags(hashtags);
        this.setNb_tweets(nb_tweets);
    }
    public User(String id, Set<String> hashtags) {
        this(id, hashtags, 0);
    }

    
    /**
     * @return the hashtags
     */
    public Set<String> getHashtags() {
        return hashtags;
    }

    /**
     * @param hashtags the hashtags to set
     */
    public void setHashtags(Set<String> hashtags) {
        this.hashtags = hashtags;
    }

    /**
     * @return the nb_tweets
     */
    public int getNb_tweets() {
        return nb_tweets;
    }

    /**
     * @param nb_tweets the nb_tweets to set
     */
    public void setNb_tweets(int nb_tweets) {
        this.nb_tweets = nb_tweets;
    }


    public void id(String id) {
        this.id = id;
    }    

    public String getID() {
        return id;
    }
}