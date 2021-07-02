package TwoPhase;

import java.util.ArrayList;

public class Transaction {
    int id;
    boolean locked = false;
    boolean commit = false;
    Transaction lockedBy;
    ArrayList <Data> xLocks;
    ArrayList <Data> sLocks;

    public Transaction(int num) {
        this.id = num;
        xLocks = new ArrayList<>();
        sLocks = new ArrayList<>();
    }

    public String toString() {
            return "SL" + this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Transaction)) return false;

        Transaction that = (Transaction) o;

        return id == that.id;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
