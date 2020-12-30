package bigdata.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;



public class User implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 4569293528740761201L;

    
    private int nb_tweets;
    private int received_favs;
    private int received_retweets;

    
    private String id;

    private Set<String> hashtags;   // Uniques values

    private List<String> localisations; // Non Unique values

    private List<String> source_history;
    private HashMap<Date, Integer> daily_frequencies;
    private HashMap<String, Integer> used_lang;

    /**
     * Constructors
     */
    public User(String id, Set<String> hashtags, int nb_tweets) {
        this.id = id;
        this.nb_tweets = nb_tweets;

        this.hashtags = new HashSet<String>(hashtags);
        this.localisations = new ArrayList<String>();
        this.source_history = new ArrayList<String>();

        this.daily_frequencies = new HashMap<Date, Integer>();
        this.used_lang = new HashMap<String, Integer>();

        this.received_favs = 0;
        this.received_retweets = 0;
    }

    public User(String id, Set<String> hashtags) {
        this(id, hashtags, 1);
    }

    

    /**
     * Setters
     */


    /**
     * @param hashtags the hashtags to set
     */
    public void setHashtags(Set<String> hashtags) {
        this.hashtags = hashtags;
    }

    /**
     * @param nb_tweets the nb_tweets to set
     */
    public void setNbTweets(int nb_tweets) {
        this.nb_tweets = nb_tweets;
    }
    
    public void setReceivedFavs(int nb_favs) {
        this.received_favs = nb_favs;
    }

    public void setReceivedRTs(int nb_rts) {
        this.received_retweets = nb_rts;
    }


    public void setId(String id) {
        this.id = id;
    }    

    public void addPublicationSource(String source) {
        this.source_history.add(source);
    }
    public void addPublicationSource(List<String> sources) {
        List<String> newList = Stream.of(this.source_history, sources)
        .flatMap(x -> x.stream())    
        .collect(Collectors.toList());

        this.source_history = newList;
    }

    public void addGeo(String localisation) {
        this.localisations.add(localisation);
    }
    public void addGeo(List<String> localisations) {
        List<String> newList = Stream.of(this.localisations, localisations)
        .flatMap(x -> x.stream())    
        .collect(Collectors.toList());

        this.localisations = newList;
    }



    public void addLangUsed(String lang) {
        if(this.used_lang.containsKey(lang)) {
            this.used_lang.put(lang, this.daily_frequencies.get(lang) + 1);
        } else {
            this.used_lang.put(lang, 1);
        }
    }
    public void addLangUsed(HashMap<String, Integer> langs) {
        

        for (Map.Entry<String, Integer> entry : langs.entrySet()) {
            String lang = entry.getKey();
            if(this.used_lang.containsKey(lang)) {
                this.used_lang.put(lang, this.used_lang.get(lang) + entry.getValue());
            } else {
                this.used_lang.put(lang, 1);
            }
        }
    }




    public void addDailyFrequencyData(Date date) {
        if(this.daily_frequencies.containsKey(date)) {
            this.daily_frequencies.put(date, this.daily_frequencies.get(date) + 1);
        } else {
            this.daily_frequencies.put(date, 1);
        }
    }
    public void addDailyFrequencyData(HashMap<Date, Integer> frequencies) {
        

        for (Map.Entry<Date, Integer> entry : frequencies.entrySet()) {
            Date date = entry.getKey();
            if(this.daily_frequencies.containsKey(date)) {
                this.daily_frequencies.put(date, this.daily_frequencies.get(date) + entry.getValue());
            } else {
                this.daily_frequencies.put(date, 1);
            }
        }
    }


    /**
     * Getters
     */

    public int _received_favs() {
        return this.received_favs;
    }

    public int _received_rts() {
        return this.received_retweets;
    }

    public List<String> _geos() {
        return this.localisations;
    }

    public List<String> _sources() {
        return this.source_history;
    }

    public HashMap<String, Integer> _langs() {
        return this.used_lang;
    }

    public HashMap<Date, Integer> _frequencies() {
        return this.daily_frequencies;
    }

    public String _id() {

        return id;
    }

    /**
     * @return the nb_tweets
     */
    public int _nbTweets() {

        return nb_tweets;
    }

    /**
     * @return the hashtags
     */
    public Set<String> _hashtags() {

        return hashtags;
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
        + "tweets_count:" + this.nb_tweets + ", " 
        + "retweets_count:" + this.received_retweets + ", " 
        + "favorites_count:" + this.received_favs + ", "

        + "daily_frequency: {[" + this.daily_frequencies + "]}" + ", "
        + "Hashtags: {[" + this.hashtags + "]}" + ", " 
        + "geo: {[" + this.localisations + "]}" + ", "
        + "lang_count: {[" + this.used_lang + "]}" + ", "
        // + "source_list: {[" + this.source_history + "]}"
        
        + "}";

        return output;
    }

    @Override
    public int hashCode() {
        int hash = id.hashCode() 
            * localisations.hashCode()
            * hashtags.hashCode() 
            * localisations.hashCode() 
            * used_lang.hashCode()
            * daily_frequencies.hashCode()
            * nb_tweets 
            * received_favs 
            * received_retweets; 
        
        return hash;
    }

}