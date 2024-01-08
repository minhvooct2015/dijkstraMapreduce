import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DijkstraMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        /**Key    là chỉ số dòng đang đọc
         *Value  có dạng <n p D m1:d1;m2:d2;...;>
         *       n là đỉnh đang được xét
         *       p là đỉnh lien truoc dinh n tren duong di ngan nhat
         *       D là khoảng cách ngắn nhất đi từ đỉnh xuất phát đến đỉnh n
         *       m1;m2;...; là danh sách các đỉnh kề của n,
         *       độ dài đường đi từ đỉnh n đến các đỉnh m1;m2;...; là d1;d2;...;
         */
        //Mỗi dòng input được lưu vào value
        //có dạng: <n p D m1:d1;m2:d2;...;>
        //Ví dụ:   1 1 0 2:1;3:1;
        String line = value.toString();
        System.out.println("Mapper - input:========================");
        System.out.println(line);
        String[] sp = line.split(" ");
        String n = sp[0];
        String p = sp[1];
        long D = Long.parseLong(sp[2]);
        String[] pointsTo = sp[3].split(";");
        long newDistance;

        Text output_key;
        Text output_value = new Text();

        //smallest = -1
        long smallest = Long.parseLong(context.getConfiguration().get("smallest_value"));
        long INF_value = Long.parseLong(context.getConfiguration().get("INF_value"));

        //Giá trị phát ra có 4 dạng
        //  1. I là đỉnh có nhãn là vô cùng
        //  2. F là đỉnh đã FIXED
        //  3. N là trạng thái lưu lại các đỉnh kề của nút n => phục vụ cho việc phục hồi lại nút
        //  4. V là trạng thái lưu lại độ dài đường đi từ nút xuất phát đến nút hiện tại thông qua nút n

        //Nếu D > vô cùng (1009) => không truyền dữ liệu sang các đỉnh khác
        //                   chỉ gửi tính hiệu sang reducer để phục hồi lại trạng thái đỉnh
        if (D >= INF_value) {
            output_key = new Text();
            output_key.set(n);
            String value_output = "I " + p + " " + D + " " + sp[3];
            System.out.println("===========================Mapper INF========================");
            System.out.println("Key output: " + n + " Value output: " + value_output);
            output_value.set(value_output);
            context.write(output_key, output_value);
            output_value.clear();
        } else if (D > smallest) {
            //Nếu D > smallest => đây là đỉnh có thể cập nhật cho đường đi ngắn hơn nữa
            if (!sp[3].equalsIgnoreCase("null;"))
                for (String md : pointsTo) {
                    String[] data = md.split(":");
                    output_key = new Text();
                    String key_output = data[0];
                    output_key.set(key_output);
                    newDistance = D + Long.parseLong(data[1]);
                    //Giá trị xuất ra ứng với mỗi đỉnh m là
                    //   - dùng để tìm ĐDĐĐNN từ đỉnh xuất phát đến đỉnh m ở hàm Reducer
                    //   key:m    -     value:"V n newDistance"
                    String value_output = "V " + n + " " + newDistance;
                    System.out.println("===========================Mapper SMALLEST Not NuLL========================");
                    System.out.println("Key output: " + key_output + " Value output: " + value_output);
                    output_value.set(value_output);
                    context.write(output_key, output_value);
                    output_value.clear();
                }
            output_key = new Text();
            output_key.set(n);

            //Mỗi đỉnh n cần xuất ra giá trị
            //   - dùng để tìm ĐDĐĐNN từ đỉnh xuất phát đến đỉnh n ở hàm Reducer
            //   key:n    -     value:"V p D"
            String value_output = "V " + p + " " + D;
            output_value.set(value_output);
            context.write(output_key, output_value);

            System.out.println("===========================Mapper SMALLEST IS NuLL========================");
            System.out.println("Key output: " + n + " Value output: " + value_output);

            output_value.clear();

            //Mỗi đỉnh n cần xuất ra giá trị
            //   - dùng để lưu trạng thái của đỉnh n ở hàm Reducer <n p D m1:d1;m2:d2;...;>
            //   key:n    -     value:"N m1:d1;m2:d2;...;"
            String adjacentOfn = "N " + sp[3];
            output_value.set(adjacentOfn);
            context.write(output_key, output_value);

            System.out.println("===========================Mapper Adjacent of n ========================");
            System.out.println("Key output: " + n + " Value output: " + adjacentOfn);
            output_value.clear();
        } else {
            //Nếu D <= smallest => không cần phát dữ liệu sang các đỉnh khác từ đỉnh này
            //                     chỉ gửi tính hiệu sang reducer để phục hồi lại trạng thái đỉnh
            output_key = new Text();
            output_key.set(n);
            String value_output = "F " + p + " " + D + " " + sp[3];
            output_value.set(value_output);
            context.write(output_key, output_value);
            System.out.println("===========================Mapper: D <= smallest========================");
            System.out.println("Key output: " + n + " Value output: " + value_output);

            output_value.clear();
        }
    }
}