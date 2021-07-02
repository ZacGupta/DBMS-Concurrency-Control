package TimeStamping;

public class Data {
    String name;
    int readTS = 0;
    int writeTS = 0;

    public Data (String name) {
        this.name = name;
    }
}
