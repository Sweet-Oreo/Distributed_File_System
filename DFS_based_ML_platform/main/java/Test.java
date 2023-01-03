import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static utils.Constant.FILENAME;
import static utils.Constant.TYPE;

public class Test {
    public static void main(String[] args) throws IOException {
//        DataOutputStream localFileDos = new
//                DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File("").getAbsolutePath() + "/src/main/java/SDFS_files/sb", false)));
//
//        ServerSocket ss = new ServerSocket(6666); // 监听指定端口
//        System.out.println("server is running...");
//        Socket socket = ss.accept();
//        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
//        byte[] buffer = new byte[4*1024];
//        int read = dataInputStream.read(buffer);
//        System.out.println(new String(buffer,0,read, StandardCharsets.UTF_8).length());
//        FileOutputStream fileOutputStream = new FileOutputStream("test22.txt");
//        int bytes;
//        while ((bytes = dataInputStream.read(buffer,0,4096)) != -1) {
////            fileOutputStream.write(buffer,0,bytes);
//            System.out.println("123123");
//        }
//        fileOutputStream.close();
        String filePath = "src/main/java/prediction";
        String zippath = "src/ggg";
        try {
            File file = new File(filePath);// 要被压缩的文件夹
            File zipFile = new File(zippath);
            InputStream input = null;
            ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFile));
            if(file.isDirectory()){
                File[] files = file.listFiles();
                for(int i = 0; i < files.length; ++i){
                    input = new FileInputStream(files[i]);
                    zipOut.putNextEntry(new ZipEntry(file.getName() + File.separator + files[i].getName()));
                    int temp = 0;
                    while((temp = input.read()) != -1){
                        zipOut.write(temp);
                    }
                    input.close();
                }
            }
            zipOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String srcPath = "src/ggg";
        String dest = "src";
        File file = new File(srcPath);
        if (!file.exists()) {
            throw new RuntimeException(srcPath + "所指文件不存在");
        }
        ZipFile zf = new ZipFile(file);
        Enumeration entries = zf.entries();
        ZipEntry entry = null;
        while (entries.hasMoreElements()) {
            entry = (ZipEntry) entries.nextElement();
            System.out.println("解压" + entry.getName());
            if (entry.isDirectory()) {
                String dirPath = dest + File.separator + entry.getName();
                File dir = new File(dirPath);
                dir.mkdirs();
            } else {
                // 表示文件
                File f = new File(dest + File.separator + "aaa/" + entry.getName().split("/")[1]);
                if (!f.exists()) {
                    //String dirs = FileUtils.getParentPath(f);
                    String dirs = f.getParent();
                    File parentDir = new File(dirs);
                    parentDir.mkdirs();
                }
                f.createNewFile();
                // 将压缩文件内容写入到这个文件中
                InputStream is = zf.getInputStream(entry);
                FileOutputStream fos = new FileOutputStream(f);
                int count;
                byte[] buf = new byte[8192];
                while ((count = is.read(buf)) != -1) {
                    fos.write(buf, 0, count);
                }
                is.close();
                fos.close();
            }
        }
        File zipFile = new File(zippath);
        zipFile.delete();
//        File x = tryZipDict("src/main/java/prediction");
//        System.out.println(x.getName());
    }

}
