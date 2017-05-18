/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.spout;


import org.apache.storm.Config;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Emits a random integer and a timestamp value (offset by one day),
 * every 100 ms. The ts field can be used in tuple time based windowing.
 */
public class OurCheckpointSpout extends CheckpointSpout {
    private static final Logger LOG = LoggerFactory.getLogger(OurCheckpointSpout.class);
    public int val = 0;
//    private Random rand;
//    private long msgId = 0;
//    public static int val=0;
//    public boolean doemit = true;
public int isPausedFlag = 1;
    boolean haveLastCheckpointAck;
    boolean pause ;
    Map<String, Map<String, Grouping>> spoutTargets;
    int valBound;
    private SpoutOutputCollector collector;

    // used for logging only
    public static void logTimeStamp(String s) {
        try {
            String filename = Config.BASE_SIGNAL_DIR_PATH + "LOGTS";
            FileWriter fw = new FileWriter(filename, true); //the true will append the new data
            fw.write(s + "\n");//appends the string to the file
            fw.close();
        } catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getMessage());
        }
    }

    //    Map<String,List<String>> componentID_streamID_map;
//    Map<String,List<Integer>> componentID_taskID_map;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//        this.collector = collector;

        ///////////////
        Set<String> componentID_set = context.getComponentIds();
        componentID_set.remove("spout");
        componentID_taskID_map = new HashMap();
        for (String cmpnt : componentID_set) {
            componentID_taskID_map.put(cmpnt, context.getComponentTasks(cmpnt));
//				System.out.println("REWIRE_cmpnt_getTargets"+context.getTargets(cmpnt));
        }
        System.out.println("REWIRE_componentID_taskID_map:" + componentID_taskID_map);

        //		"PREPARE_STREAM"
        componentID_streamID_map = new HashMap();
        spoutTargets = context.getThisTargets();
        System.out.println("stream_IDs_to_spout:" + spoutTargets.keySet());

        for (String streamID : spoutTargets.keySet()) {
            Map<String, Grouping> boltNameGroupingMap = spoutTargets.get(streamID);
//			componentID_streamID_map.put(streamID,boltNameGroupingMap.keySet());

            for (String boltname : boltNameGroupingMap.keySet()) {
                if (!componentID_streamID_map.containsKey(boltname)) {
                    List<String> temp = new ArrayList<>();
                    temp.add(streamID);
                    componentID_streamID_map.put(boltname, temp);
                } else {
                    List<String> temp2 = componentID_streamID_map.get(boltname);
                    temp2.add(streamID);
                    componentID_streamID_map.put(boltname, temp2);
                }
            }
//			System.out.println("REWIRE_componentID_streamID_map:"+streamID+","+componentID_streamID_map);
        }
        System.out.println("REWIRE_componentID_streamID_map:" + "," + componentID_streamID_map);
//		System.out.println("REWIRE_getThisTargets:"+ Arrays.asList(context.getThisTargets()));
//		System.out.println("REWIRE_getThisInputFields:"+context.getThisInputFields());
/////////////


        super.open(conf,context,collector);
    }
//
//    public void nextTuple(boolean doemit) {
//        if(!doemit) {
//            System.out.println("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
//            super.nextTuple();  //TODO: call to nexttuple should be non blocking (once started) bcz ack will change state from PREPARE to COMMIT
//            return;
//        }
//    }


//    @Override
//    public void nextTuple() {
////        Boolean doemit = true;
////        super.nextTuple(doemit);
////        if(!doemit) return;
//
//        Utils.sleep(2000);
//        val+=1;
//        if(val<=30) {
//            System.out.println("TEST_emitting_data_tuple");
////            collector.emit("datastream", new Values(val, System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
//        }
//            if(val>=20 ) {
//            System.out.println("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
//            super.nextTuple();
//            //TODO: call to nexttuple should be non blocking bcz ack will change state from PREPARE to COMMIT
//        }
//    }


//    @Override
//    public void nextTuple() {
//        Utils.sleep(2000);
//        val+=1;
//        if(val<=30) {
//            System.out.println("TEST_emitting_data_tuple");
//            collector.emit("datastream", new Values(val, System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
//        }
////        else
//        if(val>=20 ) {
//            System.out.println("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
//            super.nextTuple();
//            //TODO: call to nexttuple should be non blocking bcz ack will change state from PREPARE to COMMIT
////            val=0;
//        }
//    }

    public boolean isPaused() {
        System.out.println("*********************************************");
        // TODO logic to decide if we should pause
        pause = false;
        haveLastCheckpointAck=false;
        valBound+=1;

//        System.out.println("val_bound_and_pause_flag"+valBound+","+pause);
        if(valBound==1) // for INITilisation
            super.nextTuple();

        File f1 = new File(Config.BASE_SIGNAL_DIR_PATH +"RECOVERSTATE");
        if(f1.exists()){
            OurCheckpointSpout.logTimeStamp("RECOVERSTATE,"+System.currentTimeMillis());
            System.out.println("###########_Got_SIGNAL_to_RECOVERSTATE_###########");
            System.out.println("TEST:EMITTING_on_CHECKPOINT_STREAM just 1 init ......");
            recovering=true;
            recoveryStepInProgress=false;
            super.nextTuple();  //TODO: call to nexttuple should be non blocking (once started) bcz ack will change state from PREPARE to COMMIT
//            System.out.println("deleting_RECOVERSTATE_file");
////            TODO:logic to delete file
//            File recoverFile = new File(Config.BASE_SIGNAL_DIR_PATH +"RECOVERSTATE");
//
//            if(recoverFile.delete()){
//                System.out.println(recoverFile.getName() + " is deleted!");
//            }else{
//                System.out.println("Delete operation is failed.");
//            }

            //old logic
//            recovering=true;
//            recoveryStepInProgress=false;
//            pause=true;// to pause the datastream unless we recover [delete RECOVERSTATE file]
        }

//        if(valBound>10  ) {
//            pause=true; // set flag to pause spout
//        }

        File f2 = new File(Config.BASE_SIGNAL_DIR_PATH +"STARTCHKPT");
        if(f2.exists()){
            System.out.println("###########_Got_SIGNAL_to_start_CHKPT_###########");
            logTimeStamp("STARTCHKPT,"+System.currentTimeMillis());
            pause=true; // set flag to pause spout
        }

        File folder = new File(Config.BASE_SIGNAL_DIR_PATH);
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles)
        {
            if (file.isFile())
            {
                String filename = file.getName().split("-")[0]; //split filename from it's extension
//                System.out.println(filename);
                if(filename.equals("LastCheckpointAck")) {
//                System.out.println(filename);
                    haveLastCheckpointAck = true;
                    break;
                }
            }
        }
//        if(f2.exists() && haveLastCheckpointAck){
            if(haveLastCheckpointAck){
            System.out.println("###########_Pausing_CHKPT_stream_also_###########");
            return true;
        }


        if(pause) {
            System.out.println("pausing_datastream_at_value"+valBound);
            System.out.println("TEST:EMITTING_on_CHECKPOINT_STREAM ......");
            super.nextTuple();  //TODO: call to nexttuple should be non blocking (once started) bcz ack will change state from PREPARE to COMMIT
        }

        return pause;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CHECKPOINT_STREAM_ID, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
//        declarer.declareStream("PREPARE_STREAM_ID", new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
        for (int i = 1; i < 50; i++) {
//            if(streamID.contains("PREPARE_STREAM_ID")) {
            System.out.println("REWIRE_declaring_for_streamID-" + "PREPARE_STREAM_ID" + i);
            declarer.declareStream("PREPARE_STREAM_ID" + i, true, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
//            }
        }
    }

    @Override
    public void ack(Object msgId) {
//        LOG.debug("Got ACK for msgId : " + msgId);
//        if ( isCheckpointAckBymsgId(msgId)) {
        System.out.println("ACK_for_CHECKPOINT_STREAM_ID msgId:" + msgId);//FIXME:SYSO REMOVED
            super.ack(msgId);
            if(CheckPointState.LastCheckpointAck) {
                OurCheckpointSpout.logTimeStamp("ACK_COMMIT,"+System.currentTimeMillis());
                val=1;
//                System.out.println("TEST_LOG_COMMIT_ACK:" + ","+System.currentTimeMillis());//FIXME:SYSO REMOVED
//                System.out.println("checkpointing_is_done_check_file" + CheckPointState.LastCheckpointAck);//FIXME:SYSO REMOVED
                CheckPointState.LastCheckpointAck=false;
                File file = new File(Config.BASE_SIGNAL_DIR_PATH +"LastCheckpointAck-"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
                try {
                    if(file.createNewFile()) {
//                        System.out.println("File_creation_successfull");//FIXME:SYSO REMOVED
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // code for ack for INIT msg
            if(CheckPointState.LastINITAck) {
//                System.out.println("TEST_LOG_INIT_ACK:"+"," + System.currentTimeMillis());//FIXME:SYSO REMOVED
//                System.out.println("INIT_is_done_check_file" + CheckPointState.LastINITAck);//FIXME:SYSO REMOVED
                CheckPointState.LastINITAck=false;
                File file = new File(Config.BASE_SIGNAL_DIR_PATH +"FLUSH_REDIS_INITAck-"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
                try {
                    if(file.createNewFile()) {
                        System.out.println("File_LastINITAck_creation_successfull");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                OurCheckpointSpout.logTimeStamp("ACK_INIT,"+System.currentTimeMillis());
            }
            //code for PREPARE msg ack
            if(CheckPointState.LastPREPAREAck) {
                OurCheckpointSpout.logTimeStamp("ACK_PREP,"+System.currentTimeMillis());
//                System.out.println("TEST_LOG_PREP_ACK:" + ","+System.currentTimeMillis());//FIXME:SYSO REMOVED
//                System.out.println("PREPARE_is_done_check_file" + CheckPointState.LastPREPAREAck);//FIXME:SYSO REMOVED
                CheckPointState.LastPREPAREAck=false;
                File file = new File(Config.BASE_SIGNAL_DIR_PATH +"PREPAck-"+Thread.currentThread().getId()+"_"+ UUID.randomUUID());
                try {
                    if(file.createNewFile()) {
//                        System.out.println("File_LastPREPAck_creation_successfull");//FIXME:SYSO REMOVED
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
//        }
//        else{
//            System.out.println("ACK_for_datastream_msgId:"+msgId);
//        }

    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got FAIL for msgId : " + msgId);
        if (isCheckpointAckBymsgId(msgId)) {
            System.out.println("FAIL_for_CHECKPOINT_STREAM_ID msgId:"+msgId);
            super.fail(msgId);
        }
        else{
            System.out.println("FAIL_for_datastream msgId:"+msgId);
        }
    }

}
