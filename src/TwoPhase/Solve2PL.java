package TwoPhase;

import java.util.*;

/**
 * This class is designed to simulate a the Two Phase Locking concurrency control algorithm.
 */

public class Solve2PL {
    ArrayList<Operation> input;
    ArrayList<Operation> output;

    public Solve2PL() {};
    public Solve2PL(ArrayList<Operation> schedule) {
        this.input = schedule;
    }

    public void serialize () {
        ArrayList <Operation> outputArray = new ArrayList<>();
        Set<Transaction> committed = new HashSet<>();
        boolean finished = false;

        //While every single operation is not finished.
        while (!finished) {
            //Loop through list
            for (Operation op : input) {
                //Check if transaction is locked and can be unlocked
                if (op.transaction.locked) {
                    if (op.transaction.lockedBy.commit && op.transaction.equals(op.data.lockedTransactions.peek())) {
                        op.transaction.locked = false;
                        op.data.lockedTransactions.poll();
                    }
                }
                //If the transaction is not done and is not locked out
                if (!op.done && !op.transaction.locked) {

                    //Write
                    if (op.type == Operation.Type.WRITE) {
                        //if transaction already has an XL.
                        if (op.transaction.equals(op.data.xLock)) {
                            //Execute
                            outputArray.add(op);
                            op.done = true;

                            System.out.println("T" + op.transaction.id + ": " + op);

                            //Check if this was the last operation for this transaction.
                            if (commitCheck(op)) {
                                op.transaction.commit = true;
                            }
                        }
                        else {
                            //if data is not locked
                            if (op.data.xLock == null && op.data.sLocks.isEmpty()) {
                                //Grant XL and execute
                                op.data.xLock = op.transaction;
                                op.transaction.xLocks.add(op.data);
                                outputArray.add(op);
                                op.done = true;

                                System.out.println("T" + op.transaction.id + ": " + "XL" + op.transaction.id + "(" + op.data.name + ")");
                                System.out.println("T" + op.transaction.id + ": " + op);

                                //Check if this was the last operation for this transaction.
                                if (commitCheck(op)) {
                                    op.transaction.commit = true;
                                }
                            }
                            else {
                                //Lock out transaction.
                                op.transaction.locked = true;
                                op.transaction.lockedBy = op.data.xLock;
                                op.data.lockedTransactions.offer(op.transaction);
                                if (op.transaction.lockedBy == null) {
                                    op.transaction.lockedBy = op.data.sLocks.get(0);
                                }
                                System.out.println("T" + op.transaction.id + ": XL" + op.transaction.id + "(" + op.data.name + ")" + " DENIED");
                            }
                        }
                    }
                    //Read
                    else if (op.type == Operation.Type.READ) {
                        //if transaction already has XL or SL.
                        if (op.transaction.equals(op.data.xLock) || op.data.sLocks.contains(op.transaction)) {
                            //Execute
                            outputArray.add(op);
                            op.done = true;

                            System.out.println("T" + op.transaction.id + ": " + op);

                            //Check if this was the last operation for this transaction.
                            if (commitCheck(op)) {
                                op.transaction.commit = true;
                            }
                        }
                        else {
                            //If data is not locked
                            if (op.data.xLock == null) {
                                //Loop through entire schedule
                                for (Operation o : input) {
                                    //if there's a write operation in the future for this transaction on this data item
                                    if (!o.done && !o.equals(op) && o.transaction.id == op.transaction.id && o.data.equals(op.data) && o.type == Operation.Type.WRITE) {
                                        //If there's no SL on this data item, or if this transaction has the only SL.
                                        if (op.data.sLocks.isEmpty() || op.data.sLocks.get(0).equals(op.transaction)) {
                                            //Grant XL and execute
                                            op.data.xLock = op.transaction;
                                            op.transaction.xLocks.add(op.data);
                                            outputArray.add(op);
                                            op.done = true;

                                            System.out.println("T" + op.transaction.id + ": " + "XL" + op.transaction.id + "(" + op.data.name + ")");
                                            System.out.println("T" + op.transaction.id + ": " + op);
                                        }
                                        else {
                                            //Lock out transaction.
                                            op.transaction.locked = true;
                                            op.transaction.lockedBy = op.data.sLocks.get(0);
                                            op.data.lockedTransactions.offer(op.transaction);

                                            System.out.println("T" + op.transaction.id + ": XL" + op.transaction.id + "(" + op.data.name + ")" + " DENIED");
                                        }
                                        break;
                                    }
                                }
                                //Check if this was the last operation for this transaction.
                                if (commitCheck(op)) {
                                    op.transaction.commit = true;
                                }

                                //If the operation didn't get executed with an XL
                                if (!op.done && !op.transaction.locked) {
                                    //Grant SL and execute
                                    op.data.sLocks.add(op.transaction);
                                    op.transaction.sLocks.add(op.data);
                                    outputArray.add(op);
                                    op.done = true;

                                    System.out.println("T" + op.transaction.id + ": " + "SL" + op.transaction.id + "(" + op.data.name + ")");
                                    System.out.println("T" + op.transaction.id + ": " + op);

                                    //Check if this was the last operation for this transaction.
                                    if (commitCheck(op)) {
                                        op.transaction.commit = true;
                                    }
                                }
                            }
                            else {
                                //Lock out transaction.
                                op.transaction.locked = true;
                                op.transaction.lockedBy = op.data.xLock;
                                op.data.lockedTransactions.offer(op.transaction);

                                System.out.println("T" + op.transaction.id + ": XL" + op.transaction.id + "(" + op.data.name + ")" + " DENIED");
                            }
                        }
                    }
                }
                boolean restartLoop = false;

                //Release locks
                if (op.transaction.commit && !committed.contains(op.transaction)) {
                    System.out.print("T" + op.transaction.id + ": COMMIT | ");

                    for (Data data : op.transaction.xLocks) {
                        data.xLock = null;
                        System.out.print(data.name + " UNLOCKED | ");
                    }
                    for (Data data : op.transaction.sLocks) {
                        data.sLocks.remove(op.transaction);
                        if (!data.sLocks.isEmpty()) {
                            System.out.print("SL for " + data.name + ": " + data.sLocks + " | ");
                        }
                        else {
                            System.out.print(" | " + data.name + " UNLOCKED");
                        }
                    }
                    System.out.println();
                    restartLoop = true;
                    committed.add(op.transaction);
                }
                if (restartLoop) {
                    break;
                }
            }

            //Check if all operation are done.
            int finishedCount = 0;
            for (Operation o : input) {
                if (!o.done) {
                    break;
                } else {
                    finishedCount++;
                }
            }
            if (finishedCount == input.size()) {
                finished = true;
            }
        }
        this.output = outputArray;
    }

    public boolean commitCheck(Operation op) {
        for (Operation o : input) {
            //If there's a future operation for the current transaction.
            if (!o.done && o.transaction.id == op.transaction.id) {
                return false;
            }
        }
        return true;
    }

    public String toStringOutput () {
        String s = "";

        for (int i = 0; i < this.output.size(); i++) {
            s+= output.get(i);
        }
        return s;
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

        //Question (Wk7 Q6)
        //r1(x) r1(t) r3(z) r4(z) w2(z) r4(x) r3(x) w4(x) w4(y) w3(y) w1(y) w2(t)

        Solve2PL s = new Solve2PL();

        ArrayList <Operation> schedule = s.createList("r6(x) w2(y) r7(y) r8(z) w7(z) w6(x) w8(z)");

        s.input = schedule;

        System.out.println("Input: " + s.toStringInput() +"\n");
        s.serialize();
        System.out.println("\nOutput: "+ s.toStringOutput());

    }
}