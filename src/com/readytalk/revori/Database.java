package com.readytalk.revori;

import com.readytalk.revori.server.RevisionServer;
import com.readytalk.revori.subscribe.DiffServer;
import com.readytalk.revori.subscribe.DiffMachine;
import com.readytalk.revori.subscribe.RowListener;
import com.readytalk.revori.subscribe.Subscription;

public class Database {

  private RevisionServer server;
  private DiffServer diffServer;

  private DiffMachine machine;
  private DiffMachine manualDeliverMachine;

  public Database(RevisionServer server) {
    this.server = server;
    this.diffServer = new DiffServer(server);
  }

  public interface Transaction {
    public void execute(RevisionBuilder b);
  }

  public Revision head() {
    return server.head();
  }

  public void update(Transaction trans) {
    Revision head = head();
    RevisionBuilder b = head.builder();
    trans.execute(b);
    Revision res = b.commit();
    merge(head, res);
  }

  public QueryResult query(QueryTemplate template, Object... arguments) {
    return Revisions.Empty.diff(head(), template, arguments);
  }

  public void merge(Revision base, Revision fork) {
    server.merge(base, fork);
  }

  public void next() {
    if(manualDeliverMachine != null) {
      manualDeliverMachine.next();
    }
  }

  public void process() {
    if(manualDeliverMachine != null) {
      while(manualDeliverMachine.next()) {}
    }
  }

  public Subscription subscribe(RowListener listener, QueryTemplate template, Object... arguments) {
    if(machine == null) {
      machine = new DiffMachine(diffServer, true);
    }
    return machine.subscribe(listener, template, arguments);
  }

  public Subscription subscribeManualDelivery(RowListener listener, QueryTemplate template, Object... arguments) {
    if(manualDeliverMachine == null) {
      manualDeliverMachine = new DiffMachine(diffServer, false);
    }
    return manualDeliverMachine.subscribe(listener, template, arguments);
  }

  //public void subscribeTranslator(Query query, RowTranslator trans, MessageHandler handler)

}