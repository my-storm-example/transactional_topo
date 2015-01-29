package Example;

import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyTransactionBolt extends BaseTransactionalBolt {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;

  Integer count = 0;
  BatchOutputCollector collector;
  TransactionAttempt tx;

  @Override
  public void execute(Tuple tuple) {
    // TODO Auto-generated method stub

    tx = (TransactionAttempt) tuple.getValue(0);
    System.err.println("MyTransactionBolt TransactionAttempt " + tx.getTransactionId()
        + "  attemptid" + tx.getAttemptId());
    String log = tuple.getString(1);
    if (log != null && log.length() > 0) {
      count++;
    }

  }

  @Override
  public void finishBatch() {
    // TODO Auto-generated method stub
    System.err.println("finishBatch " + count);
    collector.emit(new Values(tx, count));
  }

  @Override
  public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector,
      TransactionAttempt id) {
    // TODO Auto-generated method stub
    this.collector = collector;
    System.err.println("MyTransactionBolt prepare " + id.getTransactionId() + "  attemptid"
        + id.getAttemptId());

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // TODO Auto-generated method stub

    declarer.declare(new Fields("tx", "count"));
  }

}
