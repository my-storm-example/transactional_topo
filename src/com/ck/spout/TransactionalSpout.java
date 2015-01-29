package com.ck.spout;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.ck.pojo.CountTransactionalMeta;

public class TransactionalSpout implements ITransactionalSpout<CountTransactionalMeta> {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  private int _amount = 10;

  private Map<Integer, String> _logMap = new HashMap<Integer, String>();

  public Map<Integer, String> logSource() {
    Map<Integer, String> logMap = new HashMap<Integer, String>();
    for (int i = 0; i < 90; i++) {
      logMap.put(i, "log");
    }
    return logMap;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("txid", "log"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public backtype.storm.transactional.ITransactionalSpout.Coordinator<CountTransactionalMeta> getCoordinator(
      Map conf, TopologyContext context) {
    return new Coordinator();
  }

  @Override
  public backtype.storm.transactional.ITransactionalSpout.Emitter<CountTransactionalMeta> getEmitter(
      Map conf, TopologyContext context) {
    return new Emmit(logSource());
  }

  /**
   * @author xuer
   * @date 2014-8-27 - 上午10:42:37
   * @Description transactionSpout的inner class Coordinator
   */
  class Coordinator implements ITransactionalSpout.Coordinator<CountTransactionalMeta> {

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void close() {
      System.out.println("");
    }

    /**
     * @Title: initializeTransaction
     * @Description: 初始化事务信息，每次新事务开始都应该调用此方法
     * @param txid
     * @param prevMetadata
     * @return
     */
    @Override
    public CountTransactionalMeta initializeTransaction(BigInteger txid,
        CountTransactionalMeta prevMetadata) {
      long _index;
      if (prevMetadata == null) {
        _index = 0;
        prevMetadata = new CountTransactionalMeta();
      } else {
        _index = prevMetadata.getIndex() + prevMetadata.getAmount();
        _amount = prevMetadata.getAmount();
      }
      System.out.println("事务开始的index:" + _index + ",amount:" + _amount);
      return new CountTransactionalMeta(_index, _amount);
    }

  }

  class Emmit implements ITransactionalSpout.Emitter<CountTransactionalMeta> {

    public Emmit(Map<Integer, String> logMap) {
      _logMap = logMap;
    }

    /**
     * @Title: emitBatch
     * @Description: 每批次的tuple都是通过此方法发送
     * @param tx
     * @param coordinatorMeta
     * @param collector
     */
    @Override
    public void emitBatch(TransactionAttempt tx, CountTransactionalMeta coordinatorMeta,
        BatchOutputCollector collector) {
      for (Long i = coordinatorMeta.getIndex(); i < coordinatorMeta.getIndex()
          + coordinatorMeta.getAmount(); i++) {
        if (_logMap.get(i.intValue()) != null) {
          collector.emit(new Values(tx, _logMap.get(i.intValue())));
        }
      }
    }

    @Override
    public void cleanupBefore(BigInteger txid) {
      System.out.println("");
    }

    @Override
    public void close() {
      System.out.println("");
    }

  }

}
