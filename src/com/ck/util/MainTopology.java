package com.ck.util;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.transactional.TransactionalTopologyBuilder;

import com.ck.bolt.SumTransactionBolt;
import com.ck.bolt.TransactionBolt;
import com.ck.spout.TransactionalSpout;


/**
 * @author xuer
 * @date 2014-10-16 - 上午10:13:12
 * @Description storm事务topo
 */
public class MainTopology {

  /**
   * @Title: main
   * @Description: TODO
   * @param args
   * @return: void
   */
  @SuppressWarnings("deprecation")
  public static void main(String[] args) {
    TransactionalTopologyBuilder ttb =
        new TransactionalTopologyBuilder("firstTopo", "transactionalSpout",
            new TransactionalSpout(), 1);

    ttb.setBolt("countBolt", new TransactionBolt(), 2).shuffleGrouping("transactionalSpout");
    ttb.setBolt("sumCountBolt", new SumTransactionBolt()).globalGrouping("countBolt");

    Config conf = new Config();

    // 此处设置为debug，适合我们开发阶段，每发送一个tuple，都会在控制台打印信息。
    conf.setDebug(true);

    if (args == null || args.length <= 0) {
      LocalCluster lc = new LocalCluster();
      lc.submitTopology("mytopology", conf, ttb.buildTopology());
    } else {
      try {
        StormSubmitter.submitTopology(args[0], conf, ttb.buildTopology());
      } catch (AlreadyAliveException e) {
        e.printStackTrace();
      } catch (InvalidTopologyException e) {
        e.printStackTrace();
      }
    }

  }
}
