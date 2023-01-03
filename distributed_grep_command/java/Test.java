public class Test {
    // expected values are counted by grep locally
    public static void main(String[] args) {
        System.out.println("\tStart Test\t");
        System.out.println("======================");
        Client client = new Client();
        testFrequent(client);
        System.out.println("======================");
        testInfrequent(client);
        System.out.println("======================");
        testEc(client);
    }

    private static void testEc(Client client) {
        int expected = 1355070;
        int searchLineCount = client.startQuery(new String[]{"grep", "-Ec", "www.*"});
        System.out.println("Test Regexp: search result is " + searchLineCount);
        if (expected == searchLineCount) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }
    }

    private static void testFrequent(Client client) {
        int expected = 1625405;
        int searchLineCount = client.startQuery(new String[]{"grep", "-c", "GET"});
        System.out.println("Test Frequent Pattern: search result is " + searchLineCount);
        if (expected == searchLineCount) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }
    }

    private static void testInfrequent(Client client) {
        int expected = 6523;
        int searchLineCount = client.startQuery(new String[]{"grep", "-c", "graham"});
        System.out.println("Test Infrequent Pattern: search result is " + searchLineCount);
        if (expected == searchLineCount) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }
    }
}
