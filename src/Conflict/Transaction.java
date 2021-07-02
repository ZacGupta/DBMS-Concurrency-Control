package Conflict;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Transaction {
    int id;
    Set<Transaction> edgeTo;


    public Transaction(int num) {
        edgeTo = new HashSet<>();
        this.id = num;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Transaction)) return false;

        Transaction that = (Transaction) o;

        if (id != that.id) return false;
        return Objects.equals(edgeTo, that.edgeTo);
    }

    @Override
    public int hashCode() {
        return id;
    }
}
