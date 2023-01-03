import org.json.JSONObject;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;

import static utils.Constant.*;

public class ClientTest {
    public static void main(String[] args) throws IOException {
//        FileInputStream fileInputStream = new FileInputStream(file);
        Socket socket = new Socket("127.0.0.1", 6666);
        JSONObject putFileRequest = new JSONObject();
        File file = new File("src/main/java/prediction");
        putFileRequest.put(TYPE, "PUTDICT");
        putFileRequest.put(FILENAME, "prediction");

        byte[] buffer = new byte[4*1024];
        int bytes;

        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.write(buffer,0,1024);
        dataOutputStream.flush();

//        fileInputStream.close();
        socket.close();
    }
}
