package gp;

import common.data.UserData;
import common.utils.IntOrdering;
import common.data.Crowd;
import common.data.DBSCANCluster;
import common.data.TCPoint;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import static gp.GPConstants.*;

public class GPQuery {

    public static JavaPairRDD<Integer, Iterable<TCPoint>> getSnapshotRDD(
            JavaRDD<String> lineRDD, UserData data)
    {
        return lineRDD
                .mapToPair(new SnapshotMapper())
                .groupByKey();
    }

    public static JavaPairRDD<Integer, DBSCANCluster> getClusterRDD
            (JavaPairRDD<Integer, Iterable<TCPoint>> snapshotRDD, UserData data)
    {
        final double distanceThreshold = data.getValueDouble(OPT_STR_DISTTHRESHOLD);
        final int densityThreshold = data.getValueInt(OPT_STR_DENTHRESHOLD);

        return snapshotRDD.flatMapToPair(new DBSCANClusterMapper(
                distanceThreshold, densityThreshold));
    }

    public static JavaPairRDD<Tuple2<Integer, Crowd>, Long> getCrowdRDDByRangePartitionJoin
            (JavaPairRDD<Integer, DBSCANCluster> clusterRDD, UserData data)
    {
        final int numSubPartitions = data.getValueInt(OPT_STR_NUMPART);
        final int timeInterval = data.getValueInt(OPT_STR_TIMETHRESHOLD);
        final double distanceThreshold = data.getValueDouble(OPT_STR_DISTTHRESHOLD);

        // temporal partition
        Ordering<Integer> ordering = Ordering$.MODULE$.comparatorToOrdering(
                new IntOrdering());
        ClassTag<Integer> intTag = ClassTag$.MODULE$.apply(Integer.class);
        RangePartitioner<Integer, DBSCANCluster> rangePartitioner =
                new RangePartitioner<>(numSubPartitions, clusterRDD.rdd(), true, ordering, intTag);
        clusterRDD = clusterRDD.repartitionAndSortWithinPartitions(rangePartitioner);

        JavaRDD<Tuple2<Integer, Crowd>> crowdCandidateRDD =
                clusterRDD.mapPartitionsWithIndex(new CrowdCandidatesMapper(
                        timeInterval, distanceThreshold
                ), true).filter(new Function<Tuple2<Integer, Crowd>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Integer, Crowd> input) throws Exception {
                        return input._2().size() > 1;
                    }
                });

        JavaPairRDD<Integer, Crowd> crowdPairRDD =
                crowdCandidateRDD.mapToPair(new PairFunction<Tuple2<Integer,Crowd>, Integer, Crowd>() {
                    @Override
                    public Tuple2<Integer, Crowd> call(Tuple2<Integer, Crowd> input) throws Exception {
                        return new Tuple2<>(input._1(), input._2());
                    }
                });

        JavaPairRDD<Integer, Crowd> crowdWithOffsetPairRDD =
                crowdPairRDD.mapToPair(new PairFunction<Tuple2<Integer, Crowd>, Integer, Crowd>() {
                    @Override
                    public Tuple2<Integer, Crowd> call(Tuple2<Integer, Crowd> input) throws Exception {
                        if(numSubPartitions == 1)
                            return input;

                        if(numSubPartitions == 2)
                        {
                            if(input._1() == 1)
                                return new Tuple2<Integer, Crowd>(2, input._2());
                            else if(input._1() == 2)
                                return new Tuple2<Integer, Crowd>(1, input._2());
                        }

                        return new Tuple2<Integer, Crowd>(input._1() - 1, input._2());
                    }
                }).filter(new Function<Tuple2<Integer, Crowd>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Integer, Crowd> input) throws Exception {
                        return input._1() > 0;
                    }
                });

        JavaPairRDD<Tuple2<Integer, Crowd>, Long> mergedCrowdRDD =
                crowdPairRDD.join(crowdWithOffsetPairRDD)
                        .filter(new Function<Tuple2<Integer, Tuple2<Crowd, Crowd>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<Integer, Tuple2<Crowd, Crowd>> input) throws Exception {
                                Crowd c1 = input._2()._1();
                                Crowd c2 = input._2()._2();
                                DBSCANCluster cluster_tail = c1.last();
                                DBSCANCluster cluster_head = c2.first();

                                double dist = cluster_tail.centroid().distanceFrom(cluster_head.centroid());
                                double time = cluster_head.getTimeStamp() - cluster_tail.getTimeStamp();

                                return dist <= distanceThreshold && time <= timeInterval;
                            }
                        })
                        .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Crowd, Crowd>>, Integer,
                                Crowd>() {
                            @Override
                            public Tuple2<Integer, Crowd> call(Tuple2<Integer, Tuple2<Crowd, Crowd>> input) throws Exception {
                                Crowd combined = new Crowd();
                                combined.addAll(input._2()._1());
                                combined.addAll(input._2()._2());
                                return new Tuple2<Integer, Crowd>(input._1(), combined);
                            }
                        }).zipWithUniqueId();

        return mergedCrowdRDD;
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
            JavaPairRDD<Tuple2<Integer, Crowd>, Long> crowdRDD, UserData data
    )
    {
        int clusterNumThreshold = data.getValueInt(OPT_STR_CLUSTERNUMTHRESHOLD);

        return crowdRDD.flatMapToPair(new CrowdObjectToTimestampMapper())
                        // <-: <(crowdId-objectId), {timestamp}>
                        .groupByKey()
                        // <-: <crowdId, {timestamp}>
                        .mapToPair(new ParticipatorMapper())
                        // <-: <crowdId, {participator}>
                        .filter(new ParticipatorFilter(clusterNumThreshold))
                        // <-: <crowdId, {participator}>
                        .reduceByKey(new ParticipatorReducer());
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
