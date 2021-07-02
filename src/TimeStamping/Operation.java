package TimeStamping;

public class Operation {
    public enum Type {
        READ, WRITE
    }
    Type type;
    Transaction transaction;
    Data data;
    int timeStamp = 0;
    boolean done = false;

    public Operation() {};
    public Operation(String type, Transaction num, Data on) {
        this.transaction = num;
        this.data = on;

        if (type.equals("r") || type.equals("R")) {
            this.type = Type.READ;
        }
        else if (type.equals("w") || type.equals("W")) {
            this.type = Type.WRITE;
        }
        else {
            System.err.println(type + " is not a valid operation type, enter r or w only");
            throw new IllegalArgumentException();
        }
    }

    public String toString() {
        String s = "";

        if (type == Type.READ) {
            s+= "r";
        }
        else {
            s+="w";
        }
        s+= transaction.id;
        s+= "(" + data.name + ")";
        s+= transaction.timeStamp + " ";
        return s;
    }
}
