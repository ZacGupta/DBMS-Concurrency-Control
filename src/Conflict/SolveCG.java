package Conflict;

import java.util.*;

/**
 * This class is designed to determine whether a series of transactions are conflict-serializable.
 */

public class SolveCG {
    ArrayList<Operation> input;

    public SolveCG() {}
    public SolveCG(ArrayList<Operation> schedule) {
        this.input = schedule;
    }

    public boolean serialize () {

        Set <Transaction> tSet = new HashSet<>();
        boolean serializable = true;

        for (Operation op : input) {
            tSet.add(op.transaction);
            op.done = true;

            for (Operation o : input) {
                if (!o.done && o.data.equals(op.data) && !o.transaction.equals(op.transaction) && !op.transaction.edgeTo.contains(o.transaction)) {
                    if (op.type == Operation.Type.WRITE && o.type == Operation.Type.READ ) {
                        op.transaction.edgeTo.add(o.transaction);
                        System.out.println("Edge from " + op.transaction.id + " to " + o.transaction.id);
                    }
                    else if (op.type == Operation.Type.READ && o.type == Operation.Type.WRITE) {
                        op.transaction.edgeTo.add(o.transaction);
                        System.out.println("Edge from " + op.transaction.id + " to " + o.transaction.id);
                    }
                    else if (op.type == Operation.Type.WRITE && o.type == Operation.Type.WRITE) {
                        op.transaction.edgeTo.add(o.transaction);
                        System.out.println("Edge from " + op.transaction.id + " to " + o.transaction.id);
                    }
                }
            }

        }
        HashMap <Transaction, Set <Transaction>> cycles = new HashMap<>();

        for (Transaction t : tSet) {
            for (Transaction edge : t.edgeTo) {
                if (edge.edgeTo.contains(t)) {

                    if (!cycles.containsKey(t)) {
                        Set <Transaction> value = new HashSet<>();
                        value.add(edge);
                        cycles.put(t, value);
                        System.out.println("Cycle: " + t.id + " and " + edge.id);
                    }
                    else {
                        if (!cycles.get(t).contains(edge)) {
                            cycles.get(t).add(edge);
                            System.out.println("Cycle: " + t.id + " and " + edge.id);
                        }
                    }
                    if (!cycles.containsKey(edge)) {
                        Set <Transaction> value = new HashSet<>();
                        value.add(t);
                        cycles.put(edge, value);
                    }
                    else {
                        cycles.get(edge).add(t);
                    }
                    serializable = false;
                }
            }
        }
        return serializable;
    }

    public String toStringInput () {
        String s = "";

        for (int i = 0; i < this.input.size(); i++) {
            s+= input.get(i);
        }
        return s;
    }

    public ArrayList<Operation> createList(String schedule) {
        String[] StringArray = schedule.split(" ");
        ArrayList <Operation> operations = new ArrayList<>();

        //Loop through entire StringArray
        for (String opString : StringArray) {
            //Create new operation
            Operation op = new Operation();

            //Loop through each operation string in the stringArray
            for (char c : opString.toCharArray()) {

                //If the character represents a transaction.
                try {
                    int tID = Integer.parseInt(Character.toString(c));

                    //Loop through all existing operations in final array to find if this transaction already exists.
                    for (Operation o : operations) {
                        if (o.transaction.id == tID) {
                            op.transaction = o.transaction;
                            break;
                        }
                    }
                    //If there was no match, create new transaction
                    if (op.transaction == null) {
                        op.transaction = new Transaction(tID);
                    }
                }
                catch (NumberFormatException e) {
                    if (c == '(' || c == ')' || c == ',') {
                        //Do nothing
                    }
                    else {
                        if (c == 'r') {
                            op.type = Operation.Type.READ;
                        }
                        else if (c == 'w') {
                            op.type = Operation.Type.WRITE;
                        }
                        else {
                            String data = Character.toString(c);

                            //Check if data already exists
                            for (Operation o : operations) {
                                if (o.data.name.equals(data)) {
                                    op.data = o.data;
                                    break;
                                }
                            }
                            //If no match was found, create new object.
                            if (op.data == null) {
                                op.data = new Data(data);
                            }
                        }
                    }
                }
            }
            operations.add(op);
        }
        return operations;
    }

    public static void main(String[] args) {

        //Question (Wk5 Q6)
        //r1(x) r1(t) r3(z) r4(z) w2(z) r4(x) r3(x) w4(x) w4(y) w3(y) w1(y) w2(t)

        SolveCG s = new SolveCG();
        ArrayList <Operation> schedule = s.createList("r1(x) r2(x) w2(x) r1(y) r2(z) w1(y) w2(z)");
        s.input = schedule;

        System.out.println("Input: " + s.toStringInput() +"\n");
        boolean cs = s.serialize();

        System.out.println("Conflict Serializable? " + cs);

    }
}