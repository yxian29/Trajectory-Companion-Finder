package gp;

import common.data.UserData;
import common.data.Crowd;
import common.data.DBSCANCluster;
import common.data.TCPoint;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.List;

import static gp.GPConstants.*;

public class GPQuery {

    public static JavaPairRDD<Integer, TCPoint> getSnapshotRDD(
            JavaRDD<String> lineRDD, UserData data)
    {
        return lineRDD.mapToPair(new SnapshotMapper());
    }

    public static JavaPairRDD<Integer, Tuple2<String, DBSCANCluster>> getClusterRDD
            (JavaPairRDD<String, Iterable<TCPoint>> rdd, UserData data)
    {
        final double distanceThreshold = data.getValueDouble(OPT_STR_DISTTHRESHOLD);
        final int densityThreshold = data.getValueInt(OPT_STR_DENTHRESHOLD);

        return rdd.flatMapToPair(new DBSCANClusterMapper(
                distanceThreshold, densityThreshold));
    }

    public static JavaPairRDD<Integer, Tuple2<String, DBSCANCluster>> getMergedClusterRDD
            (JavaPairRDD<Integer, Tuple2<String, DBSCANCluster>> clusterRDD, UserData data)
    {
        JavaPairRDD<Integer, Tuple2<String, DBSCANCluster>> clusterWithBoRDD =
                clusterRDD.filter(new Function<Tuple2<Integer, Tuple2<String, DBSCANCluster>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Integer, Tuple2<String, DBSCANCluster>> input) throws Exception {

                        String[] gridId = input._2()._1().split("_");

                        int gridIdX = Integer.parseInt(gridId[0]);
                        int gridIdY = Integer.parseInt(gridId[1]);

                        DBSCANCluster cluster = input._2()._2();
                        for (Object objPoint : cluster._cluster.getPoints()) {
                            TCPoint point = (TCPoint)objPoint;
                            if(point.getX() % gridIdX == 0)
                                return true;

                            if(point.getY() % gridIdY == 0)
                                return true;
                        }
                        return false;
                    }
                });

        JavaPairRDD<Integer, Tuple2<String, DBSCANCluster>> clusterWithoutBoRDD =
                clusterRDD.subtract(clusterWithBoRDD);

        //clusterWithBoRDD.join(clusterRDD)

        return clusterWithoutBoRDD;
    }

    public static JavaPairRDD<Tuple2<String, Crowd>, Long> getCrowdRDD(
            JavaPairRDD<Integer, Tuple2<String, DBSCANCluster>> clusterRDD, UserData data)
    {
        final int numSubPartitions = data.getValueInt(OPT_STR_NUMPART);
        final int timeInterval = data.getValueInt(OPT_STR_TIMETHRESHOLD);
        final double distanceThreshold = data.getValueDouble(OPT_STR_DISTTHRESHOLD);

        // K = gid
        // V = cluster
        JavaPairRDD<String, DBSCANCluster> gridClusterRDD =
        clusterRDD.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<String,DBSCANCluster>>,
                String, DBSCANCluster>() {
            @Override
            public Tuple2<String, DBSCANCluster> call(Tuple2<Integer, Tuple2<String, DBSCANCluster>> input) throws Exception {
                return input._2();
            }
        });

        // K = gid
        // V = {cluster}
        JavaPairRDD<String, Iterable<DBSCANCluster>> partitionedClusterRDD =
        gridClusterRDD.repartitionAndSortWithinPartitions(new HashPartitioner(numSubPartitions))
                .groupByKey();

        JavaPairRDD<String, Crowd> crowdRDD =
        partitionedClusterRDD
                .flatMapValues(new CrowdCandidatesMapper(timeInterval, distanceThreshold));

        JavaPairRDD<Tuple2<String, Crowd>, Long> indexCrowdRDD =
                crowdRDD.zipWithIndex();

        return indexCrowdRDD;
    }

//    public static JavaPairRDD<Tuple2<Integer, Crowd>, Long> getCrowdRDDByCartesian
//    {
//
//        // prepare to broadcast the full cluster list
//        // TODO: investigate the best practice instead of collect()
//        Broadcast<List<Tuple2<Integer, DBSCANCluster>>> clusterBroadcast =
//                ctx.broadcast(clusterRDD.sortByKey().collect());
//
//        JavaPairRDD<Tuple2<Integer, Cluster>, Tuple2<Integer, Cluster>> clusterPairRDD =
//                clusterRDD.flatMapToPair(new ClusterMapsideJoinMapper(clusterBroadcast,
//                        timeInterval, densityThreshold, distanceThreshold));
//    }

//    public static JavaPairRDD<Tuple2<Integer, Crowd>, Long> getCrowdRDDByMapSideJoin(
//            JavaSparkContext sc, JavaPairRDD<Integer, DBSCANCluster> clusterRDD, UserData data)
//    {
//        // prepare to broadcast the full cluster list
//        Broadcast<List<Tuple2<Integer, DBSCANCluster>>> boardcastClusters =
//                sc.broadcast(clusterRDD.sortByKey().collect());
//    }

    public static JavaPairRDD<Long, Iterable<Integer>> getParticipatorRDD(
            JavaPairRDD<Tuple2<String, Crowd>, Long> crowdRDD, UserData data
    )
    {
        final int clusterNumThreshold = data.getValueInt(OPT_STR_CLUSTERNUMTHRESHOLD);

        JavaPairRDD<String, Iterable<Integer>> objectCountRDD =
                crowdRDD.flatMapToPair(new CrowdObjectToTimestampMapper())
                        // <-: <(crowdId-objectId), timestamp>
                        .groupByKey();
                        // <-: <crowdId-objectId, {timestamp}>

        JavaPairRDD<String, Iterable<Integer>> filterRDD =
                objectCountRDD.filter(new Function<Tuple2<String, Iterable<Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Iterable<Integer>> input) throws Exception {
                List<Integer> count = IteratorUtils.toList(input._2().iterator());
                return count.size() >= clusterNumThreshold;
            }
        });

        JavaPairRDD<Long, Integer> crowdToObjectRDD = filterRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, Long, Integer>() {
            @Override
            public Tuple2<Long, Integer> call(Tuple2<String, Iterable<Integer>> input) throws Exception {
                String[] split = input._1().split("-");
                long crowdId = Long.parseLong(split[0]);
                int objectId = Integer.parseInt(split[1]);
                return new Tuple2<Long, Integer>(crowdId, objectId);
            }
        });

        return crowdToObjectRDD.groupByKey();
    }

    public static JavaPairRDD<Long, Iterable<Tuple2<Integer, Iterable<Integer>>>> getGatheringRDD(
            JavaPairRDD<Long, Iterable<Tuple2<Integer, Integer>>> crowdToObjectTimestampRDD,
            JavaPairRDD<Long, Iterable<Integer>> parRDD,
            UserData data
    )
    {
        int participatorNumThreshold = data.getValueInt(OPT_STR_PARTICIPATORTHRESHOLD);

        return crowdToObjectTimestampRDD.join(parRDD)
                        // <-: <crowdId, {(objectId, timestamp)}, {participator}>
                        .filter(new GatheringFilter(participatorNumThreshold))
                        // <-: <crowdId, {(objectId, timestamp)}, {participator}>
                        .mapToPair(new GatheringMapper())
                        // <-: <crowdId, (timestamp, objectId)>
                        .groupByKey();
                        // <-: <crowdId, {(timestamp, {objectId})}>
    }

}
