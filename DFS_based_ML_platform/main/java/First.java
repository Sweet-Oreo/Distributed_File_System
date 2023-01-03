import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;

public class First {
    public static void main(String[] args) throws IOException {
//        System.out.println(InetAddress.getLocalHost().getHostName());
//        File file = new File("test.txt");
//        // Create file if not exist
//        if (!file.exists()) {
//            file.createNewFile();
//        }
//        SDFSSender sender = new SDFSSender();
//        sender.getFile("aker.txt", "test.txt", "localhost", 15001);
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        System.out.println(ts.toString());

        SDFS s = new SDFS();
//        s.put();

    }
}
