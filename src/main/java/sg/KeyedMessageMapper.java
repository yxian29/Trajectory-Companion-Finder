package sg;

import kafka.producer.KeyedMessage;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kevinkim on 2016-05-10.
 */
public class KeyedMessageMapper implements PairFunction <Tuple2<Integer,String>,Integer,List<KeyedMessage<String, String>>> {

    private String _topic;
    List<KeyedMessage<String, String>> keyedMessage = new ArrayList<>();

    public KeyedMessageMapper(String topic)
    {
        _topic = topic;
    }

    @Override
    public Tuple2<Integer, List<KeyedMessage<String, String>>> call(Tuple2 line) throws Exception {
        String locationPoints = line._2().toString();
        Integer timestamp = Integer.parseInt(line._1().toString());
        String[] locationPointsSplit = locationPoints.split("-");
        keyedMessage = new ArrayList<>();

        for(int i=0; i < locationPointsSplit.length; i++){

            keyedMessage.add(new KeyedMessage<String, String>(_topic, locationPointsSplit[i]));

        }

        //Return Tuple (timestamp,list of locationPoints)
        return new Tuple2<>( timestamp, keyedMessage);

    }



}
