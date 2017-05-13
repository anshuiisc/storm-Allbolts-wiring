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
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.storm.spout.CheckPointState.Action;
import static org.apache.storm.spout.CheckPointState.State.COMMITTED;

/**
 * Emits checkpoint tuples which is used to save the state of the {@link org.apache.storm.topology.IStatefulComponent}
 * across the topology. If a topology contains Stateful bolts, Checkpoint spouts are automatically added
 * to the topology. There is only one Checkpoint task per topology.
 * Checkpoint spout stores its internal state in a {@link KeyValueState}.
 *
 * @see CheckPointState
 */
public class CheckpointSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointSpout.class);

//    public static final String PREPARE_STREAM_ID = "$checkpoint";
    public static final String CHECKPOINT_STREAM_ID = "$checkpoint";
    public static final String CHECKPOINT_COMPONENT_ID = "$checkpointspout";
    public static final String CHECKPOINT_FIELD_TXID = "txid";
    public static final String CHECKPOINT_FIELD_ACTION = "action";
    private static final String TX_STATE_KEY = "__state";
    private TopologyContext context;
    private SpoutOutputCollector collector;
    private long lastCheckpointTs;
    private int checkpointInterval;
    private int sleepInterval;
//    private boolean recoveryStepInProgress;
    public boolean recoveryStepInProgress;
    private boolean checkpointStepInProgress;
//    private boolean recovering;
    public boolean recovering;
    private KeyValueState<String, CheckPointState> checkpointState;
    private CheckPointState curTxState;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        open(context, collector, loadCheckpointInterval(conf), loadCheckpointState(conf, context));
    }

    // package access for unit test
    void open(TopologyContext context, SpoutOutputCollector collector,
              int checkpointInterval, KeyValueState<String, CheckPointState> checkpointState) {
        this.context = context;
        this.collector = collector;
        this.checkpointInterval = checkpointInterval;
        this.sleepInterval = checkpointInterval / 100;
        this.checkpointState = checkpointState;
        this.curTxState = checkpointState.get(TX_STATE_KEY);
        lastCheckpointTs = 0;
        recoveryStepInProgress = false;
        checkpointStepInProgress = false;
        recovering = true;
    }

    @Override
    public void nextTuple() {
        System.out.println("TEST_CheckpointSpout_nextTuple:state:"+curTxState.getState().name());
        System.out.println("TEST_shouldRecover_recovering_recoveryStepInProgress"+shouldRecover()+","+recovering+","+recoveryStepInProgress);
        if (shouldRecover()) {
            System.out.println("TEST_handleRecovery");
            handleRecovery();
            startProgress();
        } else if (shouldCheckpoint()) {
            System.out.println("TEST_doCheckpoint");
            doCheckpoint();
            startProgress();
        } else {
            System.out.println("SLEEPING...");
            Utils.sleep(sleepInterval);
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ack with txid {}, current txState {}", msgId, curTxState);
        if (curTxState.getTxid() == ((Number) msgId).longValue()) {
            if (recovering) {
                handleRecoveryAck();
            } else {
                handleCheckpointAck();
            }
        } else {
            LOG.warn("Ack msgid {}, txState.txid {} mismatch", msgId, curTxState.getTxid());
        }
        resetProgress();
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got fail with msgid {}", msgId);
        if (!recovering) {
            LOG.debug("Checkpoint failed, will trigger recovery");
            recovering = true;
        }
        resetProgress();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CHECKPOINT_STREAM_ID, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
    }

    public static boolean isCheckpoint(Tuple input) {
        //FIXME:AS7
        return CHECKPOINT_STREAM_ID.equals(input.getSourceStreamId())||"PREPARE_STREAM_ID".equals(input.getSourceStreamId());
    }

    /**
     * Loads the last saved checkpoint state the from persistent storage.
     */
    private KeyValueState<String, CheckPointState> loadCheckpointState(Map conf, TopologyContext ctx) {
        String namespace = ctx.getThisComponentId() + "-" + ctx.getThisTaskId();
        KeyValueState<String, CheckPointState> state =
                (KeyValueState<String, CheckPointState>) StateFactory.getState(namespace, conf, ctx);
        if (state.get(TX_STATE_KEY) == null) {
            CheckPointState txState = new CheckPointState(-1, COMMITTED);
            state.put(TX_STATE_KEY, txState);
            state.commit();
            System.out.println("Initialized checkpoint spout state with txState {}"+ txState);
            LOG.debug("Initialized checkpoint spout state with txState {}", txState);
        } else {
            System.out.println("Got checkpoint spout state {}"+ state.get(TX_STATE_KEY));
            LOG.debug("Got checkpoint spout state {}", state.get(TX_STATE_KEY));
        }
        return state;
    }

    private int loadCheckpointInterval(Map stormConf) {
        int interval = 0;
        if (stormConf.containsKey(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)) {
            interval = ((Number) stormConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)).intValue();
        }
        // ensure checkpoint interval is not less than a sane low value.
        interval = Math.max(100, interval);
        LOG.info("Checkpoint interval is {} millis", interval);
        return interval;
    }

    private boolean shouldRecover() {
        return recovering && !recoveryStepInProgress;
    }

    private boolean shouldCheckpoint() {
        System.out.println("TEST_calling_shouldCheckpoint...");
        System.out.println("TEST_conditions:"+!recovering+","+!checkpointStepInProgress+",("+curTxState.getState()+",||"+checkpointIntervalElapsed()+")");
//        return !recovering && !checkpointStepInProgress &&
//                (curTxState.getState() != COMMITTED || checkpointIntervalElapsed());
        //FIXME:AS3
        return !recovering && !checkpointStepInProgress ;  //  once commited do recover only
    }

    private boolean checkpointIntervalElapsed() {
        System.out.println("TEST_timer_"+(System.currentTimeMillis() - lastCheckpointTs)+":"+checkpointInterval);
        return (System.currentTimeMillis() - lastCheckpointTs) > checkpointInterval;
    }

    private void handleRecovery() {
        LOG.debug("In recovery");
        Action action = curTxState.nextAction(true);
        emit(curTxState.getTxid(), action);
    }

    private void handleRecoveryAck() {
        CheckPointState nextState = curTxState.nextState(true);
        if (curTxState != nextState) {
            System.out.println("TEST_handleRecoveryAck_"+"equal_state"+curTxState);
            saveTxState(nextState);
        } else {
            LOG.debug("Recovery complete, current state {}", curTxState);
            System.out.println("TEST_Recovery_complete_setting_recovering_FALSE"+curTxState);
            recovering = false;
        }
    }

    private void doCheckpoint() {
        LOG.debug("In checkpoint");
        if (curTxState.getState() == COMMITTED) {
            saveTxState(curTxState.nextState(false));
            lastCheckpointTs = System.currentTimeMillis();
//            System.out.println("TEST_TIMER_STARTED.....");//FIXME:SYSO REMOVED
        }
        Action action = curTxState.nextAction(false);
//        System.out.println("TEST_CheckpointSpout_doCheckpoint:action:"+action.name());//FIXME:SYSO REMOVED
        emit(curTxState.getTxid(), action);
        OurCheckpointSpout.logTimeStamp(action+","+System.currentTimeMillis());
    }

    private void handleCheckpointAck() {
        CheckPointState nextState = curTxState.nextState(false);
        saveTxState(nextState);
//        System.out.println("TEST_CheckpointSpout_handleCheckpointAck_"+nextState);//FIXME:SYSO REMOVED
    }

    private void emit(long txid, Action action) {
        LOG.debug("Current state {}, emitting txid {}, action {}", curTxState, txid, action);
//        System.out.println("TEST_emitting_Current_state {}, emitting txid {}, action {}"+","+ curTxState+","+ txid+","+ action);//FIXME:SYSO REMOVED
        //FIXME:LOG1
//        System.out.println("TEST_LOG_emit_spout:"+context.getThisComponentId()+"_"+action+","+System.currentTimeMillis());//FIXME:SYSO REMOVED
        if(action.name().equals("COMMIT")) {
//            System.out.println("TEST_Emitting_on_CHECKPOINT_STREAM_ID_ID");//FIXME:SYSO REMOVED
            collector.emit(CHECKPOINT_STREAM_ID, new Values(txid, action), txid);
        }
        else {
//            System.out.println("TEST_Emitting_on_PREPARE_STREAM_ID");//FIXME:SYSO REMOVED
            collector.emit("PREPARE_STREAM_ID", new Values(txid, action), txid);
        }

//        if(action.name().equals("PREPARE")) {
//            System.out.println("TEST_Emitting_on_PREPARE_STREAM_ID");
//            collector.emit("PREPARE_STREAM_ID", new Values(txid, action), txid);
//        }
//
//        else if(action.name().equals("INITSTATE")) {
//            System.out.println("TEST_Emitting_on_PREPARE_STREAM_ID");
//            collector.emit("PREPARE_STREAM_ID", new Values(txid, action), txid);
//        }
//        else {
//            System.out.println("TEST_Emitting_on_CHECKPOINT_STREAM_ID");
//            collector.emit(CHECKPOINT_STREAM_ID, new Values(txid, action), txid);
//        }
    }

    private void saveTxState(CheckPointState txState) {
        LOG.debug("saveTxState, current state {} -> new state {}", curTxState, txState);
        checkpointState.put(TX_STATE_KEY, txState);
        checkpointState.commit();
        curTxState = txState;
    }

    private void startProgress() {
        if (recovering) {
            recoveryStepInProgress = true;
        } else {
            checkpointStepInProgress = true;
        }
    }

    private void resetProgress() {
        if (recovering) {
            System.out.println("setting_recoveryStepInProgress_false_on_ACK");
            recoveryStepInProgress = false;
        } else {
            checkpointStepInProgress = false;
        }
    }


//    //FIXME: extra dded by A.S.
    public boolean isCheckpointAckBymsgId(Object msgId){
        return (curTxState.getTxid() == ((Number) msgId).longValue()) ;
    }
//    public boolean islastCommitMessage(){
//        return CheckPointState.createFile;
//    }


}
