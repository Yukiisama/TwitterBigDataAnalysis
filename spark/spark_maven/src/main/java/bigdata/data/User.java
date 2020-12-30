package bigdata.data;

import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;



public class User implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 4569293528740761201L;

    
    private int nb_tweets;
    private Set<String> hashtags;
    private List<String> localisations;
    private String id;

    public User(String id, Set<String> hashtags, int nb_tweets) {
        this.id = id;
        this.nb_tweets = nb_tweets;

        this.hashtags = new HashSet<String>(hashtags);
        this.localisations = new ArrayList<String>();
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

    public void setGeo(String localisations) {
        this.localisations.add(localisations);
    }


    @Override
    public boolean equals(Object obj){
        if(obj instanceof User) {
            User user_obj = (User) obj;

            if(this.id.equals(user_obj.id)){
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        String output = "{ User: " + this.id + ", "
        + "Hashtags: {[" + this.hashtags + "]}" + ", " 
        + "counts:" + this.nb_tweets + ", "
        + "geo: {[" + this.localisations + "]}" + ", "
        + "}";

        return output;
    }

    @Override
    public int hashCode() {
        int hash = id.hashCode() * nb_tweets * hashtags.hashCode() * localisations.hashCode(); 
        
        return hash;
    }

}