package TimeStamping;

public class Transaction {
    int id;
    int timeStamp = 0;
    boolean aborted = false;

    public Transaction(int num) {
        this.id = num;
    }
}
