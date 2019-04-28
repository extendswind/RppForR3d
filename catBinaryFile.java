import java.io.*;

public class Test{
       public static void main(String[] argvs) throws IOException{
               DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream("grid_test.dat_0_2")));
               System.out.println(inputStream.readInt());
       }
}
