package bigdata.data.comparator;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;
import scala.Tuple3;

public class TripleHashtagComparator implements Comparator<Tuple2<Tuple3<String,String,String>, Integer>>, Serializable {
    /**
     *
     */
    private static final long serialVersionUID = -2987874528633510402L;
    
    @Override
    public int compare(Tuple2<Tuple3<String,String,String>, Integer> t1, Tuple2<Tuple3<String,String,String>, Integer> t2) {
        return Integer.compare(t1._2, t2._2);
    }
    
}