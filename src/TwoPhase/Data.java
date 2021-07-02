package TwoPhase;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class Data {
    String name;
    Queue <Transaction> lockedTransactions;
    ArrayList <Transaction> sLocks;
    Transaction xLock;

    public Data (String name) {
        this.name = name;
        xLock = null;
        sLocks =  new ArrayList<Transaction>();
        lockedTransactions = new LinkedList<>();
    }
}
