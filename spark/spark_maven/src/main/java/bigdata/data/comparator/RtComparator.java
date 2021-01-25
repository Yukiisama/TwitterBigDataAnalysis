package bigdata.data.comparator;

import java.io.Serializable;
import java.util.Comparator;
import bigdata.data.User;


import scala.Tuple2;

public class RtComparator implements Comparator<Tuple2<String, User>>, Serializable {
    /**
     *
     */
    private static final long serialVersionUID = -2987874528633510480L;
    
    @Override
    public int compare(Tuple2<String, User> t1, Tuple2<String, User> t2) {
        return Integer.compare(t1._2._received_rts(), t2._2._received_rts());
    }
    
}