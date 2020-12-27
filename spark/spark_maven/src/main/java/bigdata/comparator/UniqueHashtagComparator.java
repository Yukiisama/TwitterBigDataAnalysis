package bigdata.comparator;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class UniqueHashtagComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    @Override
    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
        return Integer.compare(t1._2, t2._2);
    }
    
}