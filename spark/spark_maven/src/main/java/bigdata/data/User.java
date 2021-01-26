package bigdata.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;



import bigdata.TPSpark;


import scala.Tuple2;

public class User  implements Serializable, IData {

    /**
     * Columns for the database (here, HBase). In case of modification, also see
     * 
     * @see User::getContents
     */
    private static final String[] __DATABASE_COLUMNS__ = new String[]{
        "uuid",
        "id",
        "tweets_posted",
        "received_favs",
        "received_retweets",
        "hashtags_used",
        "localisations_history",    // Geolocalisation
        "sources_history",          // Android, IPhone, ...
        "daily_frequency",          // Number of tweets / Days
        "used_langs"                // Count of langs used

    };



    private static final long serialVersionUID = 4569293528740761201L;

    
    private int nb_tweets;
    private int received_favs;
    private int received_retweets;

    private String UUID;
    
    private String id;

    private HashMap<String, Integer> hashtags;   // Uniques values

    private List<String> localisations; // Non Unique values

    private List<String> source_history;
    private HashMap<Integer, Integer> daily_frequencies;
    private HashMap<String, Integer> used_lang;

    /**
     * Constructors
     */
    public User(String UUID, String id, Map<String, Integer> hashtags, int nb_tweets) {
        super();

        if(UUID.equals("")) {
            UUID = "unknown";
        }

        if(id.equals("")) {
			id = "unknown";
        }
        this.UUID = UUID;
        
        this.id = id;
        this.nb_tweets = nb_tweets;

        this.hashtags = new HashMap<String, Integer>(hashtags);
        this.localisations = new ArrayList<String>();
        this.source_history = new ArrayList<String>();

        this.daily_frequencies = new HashMap<Integer, Integer>();
        this.used_lang = new HashMap<String, Integer>();

        this.received_favs = 0;
        this.received_retweets = 0;
    }

    public User(String UUID, String id, Map<String, Integer> hashtags) {
        this(UUID, id, hashtags, 1);
    }


    public static String[] getColumnsName() {
        return __DATABASE_COLUMNS__;
    }


    /**
     * Families are available in @see HBaseUser::familyName
     */
    public Put getContent() {
        // Put row = new Put(Bytes.toBytes(this.UUID));    // Row Unique ID
        Put row = new Put(Bytes.toBytes(this.id.toString()));    // Row Unique ID


        /**
         * Global data
         */
        row.add(
            Bytes.toBytes("global"), // Family Name
            Bytes.toBytes("id"),  // column qualifier
            Bytes.toBytes(this.id.toString())  // Value
        );

        row.add(
            Bytes.toBytes("global"), // Family Name
            Bytes.toBytes("tweets_posted"),  // column qualifier
            Bytes.toBytes(Integer.toString(this.nb_tweets))  // Value
        );

        row.add(
            Bytes.toBytes("global"), // Family Name
            Bytes.toBytes("received_favs"),  // column qualifier
            Bytes.toBytes(Integer.toString(this.received_favs))  // Value
        );

        row.add(
            Bytes.toBytes("global"), // Family Name
            Bytes.toBytes("received_retweets"),  // column qualifier
            Bytes.toBytes(Integer.toString(this.received_retweets))  // Value
        );

        /**
         * Influence
         */
        row.add(
            Bytes.toBytes("influence"), // Family Name
            Bytes.toBytes("hashtags_used"),  // column qualifier
            Bytes.toBytes(this.hashtags.toString())  // Value
        );
        row.add(
            Bytes.toBytes("influence"), // Family Name
            Bytes.toBytes("daily_frequency"),  // column qualifier
            Bytes.toBytes(this.daily_frequencies.toString())  // Value
        );
        row.add(
            Bytes.toBytes("influence"), // Family Name
            Bytes.toBytes("used_langs"),  // column qualifier
            Bytes.toBytes(this.used_lang.toString())  // Value
        );
        

        /**
         * Misc data
         */
        row.add(
            Bytes.toBytes("misc"), // Family Name
            Bytes.toBytes("localisations_history"),  // column qualifier
            Bytes.toBytes(this.localisations.toString())  // Value
        );
        row.add(
            Bytes.toBytes("misc"), // Family Name
            Bytes.toBytes("sources_history"),  // column qualifier
            Bytes.toBytes(this.source_history.toString())  // Value
        );
        

        return row;
    }

    
    public Tuple2<ImmutableBytesWritable, Put> getPuts() throws Exception {
        Put put = this.getContent();

        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
    }
    /**
     * Setters
     */


    /**
     * @param hashtags the hashtags to set
     */
    public void setHashtags(HashMap<String, Integer> hashtags) {
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

    public void addHashtagsUsed(String hashtag) {
        if(this.used_lang.containsKey(hashtag)) {
            this.used_lang.put(hashtag, this.daily_frequencies.get(hashtag) + 1);
        } else {
            this.used_lang.put(hashtag, 1);
        }
    }
    public void addHashtagsUsed(HashMap<String, Integer> hashtags_input) {
        for (Map.Entry<String, Integer> entry : hashtags_input.entrySet()) {
            String hashtag = entry.getKey();
            if(this.hashtags.containsKey(hashtag)) {
                this.hashtags.put(hashtag, this.hashtags.get(hashtag) + entry.getValue());
            } else {
                this.hashtags.put(hashtag, 1);
            }
        }
    }




    public void addDailyFrequencyData(Date date) {
        Calendar cal = GregorianCalendar.getInstance();
        cal.setTime(date);

        if(this.daily_frequencies.containsKey(cal.get(Calendar.HOUR))) {
            this.daily_frequencies.put(cal.get(Calendar.HOUR), this.daily_frequencies.get(cal.get(Calendar.HOUR)) + 1);
        } else {
            this.daily_frequencies.put(cal.get(Calendar.HOUR), 1);
        }
    }
    public void addDailyFrequencyData(HashMap<Integer, Integer> frequencies) {
        for (Map.Entry<Integer, Integer> entry : frequencies.entrySet()) {
            int key = entry.getKey();

            if(this.daily_frequencies.containsKey(key)) {
                this.daily_frequencies.put(key, this.daily_frequencies.get(key) + entry.getValue());
            } else {
                this.daily_frequencies.put(key, 1);
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

    public HashMap<Integer, Integer> _frequencies() {
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
    public HashMap<String, Integer> _hashtags() {

        return hashtags;
    }


    @Override
    public boolean equals(Object obj){
        if(obj instanceof User) {
            User user_obj = (User) obj;

            if(this.UUID.equals(user_obj.UUID)){
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        String output = "{ User UUID: " + this.UUID + ", " 
        + "Username: " + this.id + ", "
        + "tweets_count:" + this.nb_tweets + ", " 
        + "retweets_count:" + this.received_retweets + ", " 
        + "favorites_count:" + this.received_favs + ", "

        + "daily_frequency: {[" + this.daily_frequencies + "]}" + ", "
        + "Hashtags: {[" + this.hashtags + "]}" + ", " 
        + "geo: {[" + this.localisations + "]}" + ", "
        + "lang_count: {[" + this.used_lang + "]}"
        // + "source_list: {[" + this.source_history + "]}"
        
        + "}";

        return output;
    }

    @Override
    public int hashCode() {
        int hash = UUID.hashCode() 
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