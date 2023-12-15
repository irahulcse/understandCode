package thesis.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.context.data.ImageData;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;
import thesis.context.predicates.ComplexPredicate;
import thesis.policy.SingleDataSectionPolicy;
import thesis.policy.SingleDataSectionSinglePriorityPolicy;

import java.awt.geom.Point2D;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.UUID;

/**
 * A collection of descriptors of operator states
 */
public class Descriptors {

    public static final MapStateDescriptor<String, SingleDataSectionPolicy> policyStateDescriptor =
            new MapStateDescriptor<>("policies", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(SingleDataSectionPolicy.class));
    public static final ValueStateDescriptor<Integer> highestSatisfiedPriority = new ValueStateDescriptor<>("highest satisfied priority", BasicTypeInfo.INT_TYPE_INFO);
    public static final ValueStateDescriptor<Integer> counter = new ValueStateDescriptor<>("Context building counter", BasicTypeInfo.INT_TYPE_INFO);
    public static final ValueStateDescriptor<? extends Data<?>> intermediateBufferForContext = new ValueStateDescriptor<>("context builder buffer", new TypeHint<Data<?>>() {
    }.getTypeInfo());
    public static final ListStateDescriptor<ScalarData> scalarValueList = new ListStateDescriptor<>("speed list for context building", ScalarData.class);
    public static final ListStateDescriptor<LocationData> locationValueList = new ListStateDescriptor<>("location list for context building", LocationData.class);
    public static final ListStateDescriptor<ImageData> imageDataList = new ListStateDescriptor<>("image list for context building", ImageData.class);
    public static final MapStateDescriptor<String, Tuple3<String, Long, UUID>> decisionMapState = new MapStateDescriptor<>("decision", BasicTypeInfo.STRING_TYPE_INFO,
            new TypeHint<Tuple3<String, Long, UUID>>() {
            }.getTypeInfo());
    public static final ValueStateDescriptor<SingleDataSectionPolicy> singleDataSectionPolicyStateDescriptor = new ValueStateDescriptor<>("single data section policy state", TypeInformation.of(SingleDataSectionPolicy.class));
    public static final ValueStateDescriptor<SingleDataSectionSinglePriorityPolicy> atomicPolicyDescriptor = new ValueStateDescriptor<>("atomic policy state", TypeInformation.of(SingleDataSectionSinglePriorityPolicy.class));
}
