package TimeStamping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This class is designed to simulate a timestamp based concurrency-control algorithm.
 */

public class SolveTS {
    ArrayList<Operation> input;
    ArrayList<Operation> output;

    public SolveTS() {};
    public SolveTS(ArrayList<Operation> schedule) {
        this.input = schedule;
    }

    public void serialize () {

        ArrayList <Operation> output = new ArrayList<>();
        ArrayList <Operation> abortedOperations = new ArrayList<>();
        Iterator <Operation> iter = input.iterator();

        int i = 0;

        while (iter.hasNext()) {
            Operation op = iter.next();

            if (op.transaction.aborted) {
                //Skip the aborted transaction.
            }
            else {
                //If transaction doesn't have a timestamp yet, set timestamp.
                if (op.transaction.timeStamp == 0) {
                    op.transaction.timeStamp = i + 1;
                }
                //Read operation
                if (op.type == Operation.Type.READ) {
                    //Abort
                    if (op.data.writeTS > op.transaction.timeStamp) {
                        //Set aborted and done
                       op.transaction.aborted = true;
                       op.done = true;
                       System.out.println("Time: " + (i + 1)  + " | Operation: " + op + " | Logic: " + op.transaction.timeStamp + " < WTS(" + op.data.name + ")? F"
                               + " | Status: ABORT T" + op.transaction.id);

                        //Abort all operations from this transaction.
                        for (int j = 0; j < input.size(); j++) {
                            if (input.get(j).transaction.equals(op.transaction)) {
                                abortedOperations.add(input.get(j));
                            }
                            //Calculate new timestamp
                            int opCount = 0;
                            int notDoneAborted = 0;
                            int doneAborted = 0;
                            //Loop through input
                            for (int k = 0; k < input.size(); k++) {
                                //If the operation is not done and is from this transaction, don't count.
                                if (!input.get(k).done && input.get(k).transaction == op.transaction) {
                                    notDoneAborted++;
                                }
                                //If the operation is done and is from this transaction, remove from output.
                                else if (input.get(k).done && input.get(k).transaction == op.transaction) {
                                    output.remove(input.get(k));
                                    doneAborted++;
                                }
                                else {
                                    opCount++;
                                }
                            }
                            op.transaction.timeStamp = opCount + (abortedOperations.size() - notDoneAborted) + 1;
                        }
                    }
                    //Execute and update
                    else {
                        if (op.transaction.timeStamp > op.data.readTS) {
                            op.data.readTS = op.transaction.timeStamp;
                        }
                        op.done = true;
                        output.add(op);
                        System.out.println("Time: " + (i + 1)  + " | Operation: " + op + "| Read time of " + op.data.name + ": " + op.data.readTS
                                + " | Logic: " + op.transaction.timeStamp + " < WTS(" + op.data.name + ")? T" + " | Status: OK");
                    }
                }
                //Write operation
                if (op.type == Operation.Type.WRITE) {
                    //Abort
                    if (op.data.writeTS > op.transaction.timeStamp || op.data.readTS > op.transaction.timeStamp) {
                        //Set aborted and done
                        op.transaction.aborted = true;
                        op.done = true;
                        System.out.println("Time: " + (i + 1) + " | Operation: " + op + " | Logic: " + op.transaction.timeStamp + " < WTS(" + op.data.name + ") OR " +
                                op.transaction.timeStamp + " < RTS(" + op.data.name + ")? F" + " Status: ABORT T" + op.transaction.id);

                        //Abort all operations from this transaction.
                        for (int j = 0; j < input.size(); j++) {
                            if (input.get(j).transaction.equals(op.transaction)) {
                                abortedOperations.add(input.get(j));
                            }
                        }
                        //Calculate new timestamp
                        int opCount = 0;
                        int notDoneAborted = 0;
                        int doneAborted = 0;
                        //Loop through input
                        for (int k = 0; k < input.size(); k++) {
                            //If the operation is not done and is from this transaction, don't count.
                            if (!input.get(k).done && input.get(k).transaction == op.transaction) {
                                notDoneAborted++;
                            }
                            //If the operation is done and is from this transaction, remove from output.
                            else if (input.get(k).done && input.get(k).transaction == op.transaction) {
                                output.remove(input.get(k));
                                doneAborted++;
                            }
                            else {
                                opCount++;
                            }
                        }
                        op.transaction.timeStamp = opCount + (abortedOperations.size() - notDoneAborted) + 1;
                    }
                    //Execute and update
                    else {
                        op.data.writeTS = op.transaction.timeStamp;
                        op.done = true;
                        output.add(op);
                        System.out.println("Time: " + (i + 1) + " | Operation: " + op + "| Write time of " + op.data.name + ": " + op.data.writeTS
                                + " | Logic: " + op.transaction.timeStamp + " < WTS(" + op.data.name + ") OR " +
                                op.transaction.timeStamp + " < RTS(" + op.data.name + ")? T" + " | Status: OK");
                    }
                }
                i++;
            }
        }
        //Print aborted operations
        for (int l = 0; l < abortedOperations.size(); l++) {
            Operation abortedOp = abortedOperations.get(l);
            System.out.println("Time: " + (i + 1) + " | Operation: " + abortedOp);
            i++;
        }

        output.addAll(abortedOperations);
        this.output = output;
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

        //Question (Wk7 Q3)
        // r1(x) r2(x) w2(x) r3(x) r4(z) w1(x) w3(y) w3(x) w1(y) w5(x) w1(z) w5(y) r5(z)
        //Solution
        // r2(x) w2(x) r3(x) r4(z) w3(y) w3(x) w5(x) w5(y) r5(z) r1(x) w1(x) w1(y) w1(z)

        SolveTS s = new SolveTS();
        ArrayList <Operation> schedule = s.createList("r1(x) r2(x) w2(x) r3(x) r4(z) w1(x) w3(y) w3(x) w1(y) w5(x) w1(z) w5(y) r5(z)");
        s.input = schedule;

        System.out.println("Input: " + s.toStringInput() +"\n");
        s.serialize();
        System.out.println("\nOutput: "+ s.toStringOutput());

    }
}