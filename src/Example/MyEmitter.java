package Example;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

public class MyEmitter implements ITransactionalSpout.Emitter<MyMata> {

  Map<Long, String> dbMap = null;

  public MyEmitter(Map<Long, String> dbMap) {
    this.dbMap = dbMap;
  }

  @Override
  public void cleanupBefore(BigInteger txid) {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public void emitBatch(TransactionAttempt tx, MyMata coordinatorMeta,
      BatchOutputCollector collector) {
    // TODO Auto-generated method stub
    long beginPoint = coordinatorMeta.getBeginPoint();
    int num = coordinatorMeta.getNum();

    for (long i = beginPoint; i < num + beginPoint; i++) {
      if (dbMap.get(i) == null) {
        continue;
      }
      collector.emit(new Values(tx, dbMap.get(i)));
    }
  }



}
