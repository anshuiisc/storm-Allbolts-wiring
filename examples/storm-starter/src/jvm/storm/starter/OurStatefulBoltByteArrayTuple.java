package storm.starter;

//import org.apache.commons.collections.map.HashedMap;

import org.apache.storm.Config;
import org.apache.storm.serialization.KryoTupleDeserializer;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.util.*;

//import org.eclipse.jetty.util.ArrayQueue;

/**
 * Created by anshushukla on 28/02/17.
 */
public abstract class OurStatefulBoltByteArrayTuple<T,V> extends BaseStatefulBolt<KeyValueState<T, V>> {
    private static final Object DRAIN_LOCK = new Object();
    //    private List<byte []> ourOutTuples ;
    public List<byte[]> redisTuples = new ArrayList();
    public String name;
    //FIXME: our declared vars start
    boolean commitFlag=false,drainDone=true,passThrough=false;
    KeyValueState<T, V> kvstate;
    OutputCollector collector;

    TopologyContext _context;
    KryoTupleSerializer kts;
    KryoTupleDeserializer ktd;
    KryoValuesSerializer kvs;
    KryoValuesDeserializer kvd;
    Map p = new HashMap();
    private List<byte[]> ourPendingTuples;
    private boolean kryoInit;

    private void initKryo() {
        if (kryoInit) return;
        p = new HashMap();
        p.put(Config.TOPOLOGY_KRYO_FACTORY, "org.apache.storm.serialization.DefaultKryoFactory");
        p.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);
        p.put(Config.TOPOLOGY_TUPLE_SERIALIZER, "org.apache.storm.serialization.types.ListDelegateSerializer");
        p.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
        kvs = new KryoValuesSerializer(p);
        kts = new KryoTupleSerializer(p, _context);
        kryoInit = true;
    }

    @Override
    public void prePrepare(long txid) {
        System.out.println("TEST:prePrepare:" + Thread.currentThread().toString());

        synchronized (DRAIN_LOCK) {
            kryoInit = false;
            drainDone=false;
            System.out.println("TEST_prePrepare_drainDone_FALSE" + drainDone);
////            commitFlag=true;
////            long startDrain=System.currentTimeMillis();
//            /*do {
//                try {
//                    System.out.println("TEST:locked for next 1 sec.");
//                    DRAIN_LOCK.wait(1); // race condition to be fixed (5 sec wait)
//                    System.out.println("TEST:setting drainDone flag true");
//                    drainDone=true;
//                    return;
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
////                while (System.currentTimeMillis() - startDrain < 30000) ;
//            while(true);  */
////            drainDone=true;
        }


    }


    public  boolean preExecute(Tuple in) // logic for checking commit flag thing accumulate msg //    execute or store it
    {
//        drainDone=true;
//        File f2 = new File(Config.BASE_SIGNAL_DIR_PATH +"STARTCHKPT");
//        if(f2.exists()){
//            drainDone=false;
//            System.out.println("TEST_preExecute_drainDone_FALSE_STARTCHKPT_exists:"+drainDone);
//        }


        System.out.println("TEST:preExecute:" + Thread.currentThread().toString());
//        p.put(Config.TOPOLOGY_KRYO_FACTORY,"org.apache.storm.serialization.DefaultKryoFactory");
//        p.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION,true);
//        p.put(Config.TOPOLOGY_TUPLE_SERIALIZER,"org.apache.storm.serialization.types.ListDelegateSerializer");
//        p.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS,true);
//
//        kts=new KryoTupleSerializer(p,_context);

//        ourPendingTuples.add(kts.serialize(in)); // custom constr

        System.out.println("TEST_preExecute_drainDone_Flag_Value" + drainDone);
//        synchronized (DRAIN_LOCK) {
        if (!drainDone) {
            initKryo();
            ourPendingTuples.add(kts.serialize(in)); // custom constr
            System.out.println("TEST_preExecute_written_to_ourPendingTuples" + ourPendingTuples.size() + "," + _context.getThisComponentId());
//                    DRAIN_LOCK.notify();
            return false;
        }
//        }
        return true;
    }


    @Override
    public void preCommit(long txid) {
        System.out.println("TEST:preCommit");
        System.out.println("TEST_writing_tuples_to_redis_Bolt_" + _context.getThisComponentId() + ",preCommit_getThisTaskId," + _context.getThisTaskId());
//        System.out.println("_ourOutTuples:"+ourOutTuples+"NULL_CHECK"+ourOutTuples);
        System.out.println("ourPendingTuples:" + ourPendingTuples + "NULL_CHECK" + ourPendingTuples.size());
//        kvstate.put((T)"OUR_OUT_TUPLESX",(V)ourOutTuples);
        kvstate.put((T) "OUR_PENDING_TUPLESX", (V) ourPendingTuples);

//        System.out.println(_context.getThisTaskId());

//        //FIXME:AS8
        drainDone=true;
    }

    public  void emit(Tuple input, Values out) // used by user for emitting
    {
        System.out.println("TEST:emit:" + Thread.currentThread().toString());


//        p.put(Config.TOPOLOGY_KRYO_FACTORY,"org.apache.storm.serialization.DefaultKryoFactory");
//        p.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION,true);
//        p.put(Config.TOPOLOGY_TUPLE_SERIALIZER,"org.apache.storm.serialization.types.ListDelegateSerializer");
//        p.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS,true);
//        kvs=new KryoValuesSerializer(p);
//        kts=new KryoTupleSerializer(p,_context);

//        try {
//            ourOutTuples.add(kts.serialize(input));
//            ourOutTuples.add(kvs.serialize(out));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//         logic for checking ack after post processing and then emit
        System.out.println("TEST_emit_drainDone_Flag_Value" + drainDone);
//        synchronized (DRAIN_LOCK) {
//        if (!drainDone) {
//            try {
////                    ourOutTuples.add(kts.serialize(input));
////                ourOutTuples.add(kvs.serialize(out));
//                System.out.println("TEST_emit_written_to_ourOutTuples");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
////                DRAIN_LOCK.notify();
//            return ;
//        }
//        }
        System.out.println("TEST_emitting_downstream");
        collector.emit(out);

//                collector.emit(input,out);
//                collector.ack(input);

    }







    @Override
    public void initState(KeyValueState<T, V> state) {
        System.out.println("TEST_initState_start");
        drainDone = true;
//        //FIXME:AS8
//        System.out.println("TEST_unsetting_drainDone_"+drainDone);
//        drainDone=true;

        File file = new File(Config.BASE_SIGNAL_DIR_PATH +"INIT_CALLED_"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
        try {
            if(file.createNewFile()) {
                System.out.println("INIT_creation_successfull");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // FIXME: if state is null, then nothing to do.
        initKryo();

        p.put(Config.TOPOLOGY_KRYO_FACTORY,"org.apache.storm.serialization.DefaultKryoFactory");
        p.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION,true);
        p.put(Config.TOPOLOGY_TUPLE_SERIALIZER,"org.apache.storm.serialization.types.ListDelegateSerializer");
        p.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS,true);
        ktd=new KryoTupleDeserializer(p,_context);
        kvd=new KryoValuesDeserializer(p);

        kvstate=state;
//        ourOutTuples= (List<byte[]>) kvstate.get((T) "OUR_OUT_TUPLESX", (V) new ArrayList<byte []>());
        ourPendingTuples= (List<byte[]>) kvstate.get((T) "OUR_PENDING_TUPLESX", (V) new ArrayList<byte []>());

        System.out.println("TEST_restored_tuples_from_redis_for_bolt_" + _context.getThisComponentId() + "ourPendingTuples:" + ourPendingTuples.size()
                + ",initState_getThisTaskId," + _context.getThisTaskId());

//        for (int i = 0; i < ourOutTuples.size(); i++) {
//            try {
////                System.out.println("TEST_initState_ourOutTuples_TUPLE_"+ktd.deserialize(ourOutTuples.get(i)).toString());
//                System.out.println("TEST_initState_ourOutTuples_VALUE_"+kvd.deserialize(ourOutTuples.get(i)).toString());
//                collector.emit(kvd.deserialize(ourOutTuples.get(i)));// FIXME: WRONG ack and emit the tuple (Re-chek)
////                collector.emit(ktd.deserialize(ourOutTuples.get(i)),kvd.deserialize(ourOutTuples.get(i+1)));// FIXME: WRONG ack and emit the tuple (Re-chek)
////                collector.ack(ktd.deserialize(ourOutTuples.get(i)));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        ourOutTuples.clear();

        for (int i = 0; i < ourPendingTuples.size(); i++) {
            try {
                System.out.println("TEST_initState_ourPendingTuples_TUPLE_" + ktd.deserialize(ourPendingTuples.get(i)).toString());
                execute(ktd.deserialize(ourPendingTuples.get(i)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        ourPendingTuples.clear();


//        redisTuples = (List<byte[]>) kvstate.get((T)"redisTuples", (V) new ArrayList<byte[]>());
//        System.out.println("SIZE_OF_REDIS_TUPLES:"+redisTuples.size());
//        for(int i=0;i<redisTuples.size();i++){
//            System.out.println("Redis_entry:"+ new String(redisTuples.get(i)));
//        }

        // send first
//        for (CustomPair tuple : ourOutTuples) {
//            collector.emit(tuple.input,tuple.output);// FIXME: WRONG ack and emit the tuple (Re-chek)
//            collector.ack(tuple.input);
//        }
//        ourOutTuples.clear();
////        passThrough=false;
//
//        for (CustomPair tuple : ourPendingTuples) {
//            execute(tuple.input);
//        }
//        ourPendingTuples.clear();
        System.out.println("TEST_initState_finish");
    }



//    public class CustomPair implements Serializable{/// FIXME: implements serilizable
//
//        private static final long serialVersionUID = -5606842333916087978L;
//
//        Tuple input;
//        Values output; // values out ??
//        boolean outFlag;
//
//        CustomPair(){}
//        CustomPair(Tuple in){
//            System.out.println("TEST:CustomPair1");
//            input=in;
//            outFlag=false;
//        }
//        CustomPair(Tuple in, Values out){
//            System.out.println("TEST:CustomPair2");
//            input=in;
//            output=out;
//            outFlag=true;
//        }
//
//        private void writeObject(ObjectOutputStream o)
//                throws IOException {
//            System.out.println("TEST:writeObject");
//
//            o.writeObject(input);
//            o.writeBoolean(outFlag);
//            if(outFlag)
//                o.writeObject(output);
//        }
//
//        private void readObject(ObjectInputStream o)
//                throws IOException, ClassNotFoundException {
//            System.out.println("TEST:readObject");
//
//            input = (Tuple) o.readObject();
//            outFlag=o.readBoolean();
//            if(outFlag)
//                output = (Values) o.readObject();
//        }
//    }

//    private byte[] convertToBytes(Tuple object) throws IOException {
//         ByteArrayOutputStream bos = new ByteArrayOutputStream();
//         ObjectOutput out = new ObjectOutputStream(bos);
////         out.writeObject(object);
//
//        out.writeInt(object.getSourceTask());
//        out.writeObject(object.getSourceStreamId());
//        out.writeObject(object.getValues());
//        Map<Long, Long> _anchorsToIds = object.getMessageId().getAnchorsToIds();
//        out.writeInt(_anchorsToIds.size());
//        for(Map.Entry<Long, Long> anchorToId: _anchorsToIds.entrySet()) {
//            out.writeLong(anchorToId.getKey());
//            out.writeLong(anchorToId.getValue());
//        }
//
////        collector.
////        object.getSourceGlobalStreamId(). // some options to set
//        return bos.toByteArray();
//
//    }
//
//    private Tuple convertFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
//        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
//             ObjectInput in = new ObjectInputStream(bis)) {
//
//            int taskId=in.readInt(); // sourceTaskID
//            String streamId= (String) in.readObject();// stream id of tuple emitted to
//            List<Object>  vals = (List<Object>) in.readObject();// all the values in this tuple
//
//            int numAnchors = in.readInt();
//            Map<Long, Long> anchorsToIds = new HashMap<>();
//            for(int i=0; i<numAnchors; i++) {
//                anchorsToIds.put(in.readLong(), in.readLong());
//            }
//
//            MessageId messageId = MessageId.makeId(anchorsToIds);// verify used static function
//
//            return  new TupleImpl(_context, vals, taskId, streamId, messageId);
//
//        }
//    }



}
