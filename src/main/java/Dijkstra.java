import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Dijkstra extends Configured implements Tool {
    public static final long INF = 10009;
    public static String OUT = "/output/dijkstra";
    public static String IN = "/input/dijkstra";
    public static long STEP = 0;
    HashMap<String, Long> _map;
    HashMap<String, Long> imap;

    public static class DijkstraMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            /**Key    là chỉ số dòng đang đọc
             *Value  có dạng <n p D m1:d1;m2:d2;...;>
             *       n là đỉnh đang được xét
             *       p là đỉnh lien truoc dinh n tren duong di ngan nhat
             *       D là khoảng cách ngắn nhất đi từ đỉnh xuất phát đến đỉnh n
             *       m1;m2;...; là danh sách các đỉnh kề của n,
             *       độ dài đường đi từ đỉnh n đến các đỉnh m1;m2;...; là d1;d2;...;
             */

            1 1 0 2:1;3:1;
            2 1 10000 1:1;4:1;5:1;
            3 1 10000 1:1;
            4 1 10000 2:1;5:1;
            5 1 10000 2:1;4:1;

            //Mỗi dòng input được lưu vào value
            //có dạng: <n p D m1:d1;m2:d2;...;>
            //Ví dụ:   1 1 0 2:1;3:1;
            String line = value.toString();
            String[] sp = line.split(" ");
            String n = sp[0];
            String p = sp[1];
            long D = Long.parseLong(sp[2]);
            String[] pointsTo = sp[3].split(";");
            long newDistance;

            Text output_key;
            Text output_value = new Text();

            long smallest = Long.parseLong(context.getConfiguration().get("smallest_value"));
            long INF_value = Long.parseLong(context.getConfiguration().get("INF_value"));

            //Giá trị phát ra có 4 dạng
            //  1. I là đỉnh có nhãn là vô cùng
            //  2. F là đỉnh đã FIXED
            //  3. N là trạng thái lưu lại các đỉnh kề của nút n => phục vụ cho việc phục hồi lại nút
            //  4. V là trạng thái lưu lại độ dài đường đi từ nút xuất phát đến nút hiện tại thông qua nút n

            //Nếu D > vô cùng => không truyền dữ liệu sang các đỉnh khác
            //                   chỉ gửi tính hiệu sang reducer để phục hồi lại trạng thái đỉnh
            if (D >= INF_value) {
                output_key = new Text();
                output_key.set(n);
                output_value.set("I " + p + " " + D + " " + sp[3]);
                context.write(output_key, output_value);
                output_value.clear();
            } else if (D > smallest) {
                //Nếu D > smallest => đây là đỉnh có thể cập nhật cho đường đi ngắn hơn nữa
                if (!sp[3].equalsIgnoreCase("null;"))
                    for (String md : pointsTo) {
                        String[] data = md.split(":");
                        output_key = new Text();
                        output_key.set(data[0]);
                        newDistance = D + Long.parseLong(data[1]);
                        //Giá trị xuất ra ứng với mỗi đỉnh m là
                        //   - dùng để tìm ĐDĐĐNN từ đỉnh xuất phát đến đỉnh m ở hàm Reducer
                        //   key:m    -     value:"V n newDistance"
                        output_value.set("V " + n + " " + newDistance);
                        context.write(output_key, output_value);
                        output_value.clear();
                    }
                output_key = new Text();
                output_key.set(n);

                //Mỗi đỉnh n cần xuất ra giá trị
                //   - dùng để tìm ĐDĐĐNN từ đỉnh xuất phát đến đỉnh n ở hàm Reducer
                //   key:n    -     value:"V p D"
                output_value.set("V " + p + " " + D);
                context.write(output_key, output_value);
                output_value.clear();

                //Mỗi đỉnh n cần xuất ra giá trị
                //   - dùng để lưu trạng thái của đỉnh n ở hàm Reducer <n p D m1:d1;m2:d2;...;>
                //   key:n    -     value:"N m1:d1;m2:d2;...;"
                output_value.set("N " + sp[3]);
                context.write(output_key, output_value);
                output_value.clear();
            } else {
                //Nếu D <= smallest => không cần phát dữ liệu sang các đỉnh khác từ đỉnh này
                //                     chỉ gửi tính hiệu sang reducer để phục hồi lại trạng thái đỉnh
                output_key = new Text();
                output_key.set(n);
                output_value.set("F " + p + " " + D + " " + sp[3]);
                context.write(output_key, output_value);
                output_value.clear();
            }
        }
    }

    public static class DijkstraReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            /**Key:     đỉnh n
             *Value:   có 3 dạng:
             *         V p D                - p la dinh lien truoc cua dinh n tren duong di
             *                              - D là độ dài đường đi từ đỉnh xuất phát đến đỉnh n
             *         N m1:d1;m2:d2;...;   - m1:d1;m2:d2;...; dùng để khôi phục lại trạng thái của đỉnh n
             *         I p D m1:d1;m2:d2;...; DDDD tu dinh xuat phat den đỉnh n là vô cùng => chỉ phục hồi lại trạng thái của đỉnh này
             *         F p D m1:d1;m2:d2;...; DDDD tu dinh xuat phat den đỉnh n da ngan nhat => khong the cap nhat nua
             */
            String pointsTo = "";
            Text output_value = new Text();
            long shortest = INF;
            long D;
            String p = key.toString();
            String status = "";

            for (Text val : values) {
                String[] sp = val.toString().split(" ");
                if (sp[0].equalsIgnoreCase("V") ||
                        sp[0].equalsIgnoreCase("I")) {
                    //V p D                - p la dinh lien truoc cua dinh n tren duong di
                    //                     - D là độ dài đường đi từ đỉnh xuất phát đến đỉnh n
                    D = Long.parseLong(sp[2]);
                    if (D < shortest) {
                        shortest = D;
                        p = sp[1];
                    }
                    if (sp[0].equalsIgnoreCase("I")) {
                        pointsTo = sp[3];
                        //status = "INF";
                    }
                } else if (sp[0].equalsIgnoreCase("N")) {
                    //N m1;m2;...;   - m1:d1;m2:d2;...; dùng để khôi phục lại trạng thái của đỉnh n
                    pointsTo = sp[1];
                } else {
                    //F p D m1:d1;m2:d2;...; DDDD tu dinh xuat phat den đỉnh n da ngan nhat => khong the cap nhat nua
                    p = sp[1];
                    shortest = Long.parseLong(sp[2]);
                    pointsTo = sp[3];
                    //status   = "FIXED";
                    break;
                }
            }
            //Giá trị xuất ra có dạng <n p D pointsTo>
            output_value.set(p + " " + shortest + " " + pointsTo + " " + status);
            context.write(key, output_value);
            output_value.clear();
        }
    }

    public static void deleteSubFolder(FileSystem dfs, String pFolder, String SubFolderPrefix) throws Exception {
        FileStatus[] fileStatus = dfs.listStatus(new Path(pFolder));
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        for (Path path : paths) {
            if (path.getName().startsWith(SubFolderPrefix))
                dfs.delete(path, true);
        }
    }


    public int run(String[] args) throws Exception {
        //Cấu hình giá trị key - value ngăn cách bởi khoảng trắng
        getConf().set("mapred.textoutputformat.separator", " ");

        //Lấy giá trị của file input và output
        IN = args[0];
        //OUT = args[1];

        System.out.println("---------------------------------------------");
        System.out.println("- INPUT FILE: " + IN);
        System.out.println("---------------------------------------------");


        STEP = 0;
        String inputfile = IN;
        String outputfile = OUT + (++STEP);

        boolean isdone = false;
        boolean success = false;

        //xoa bo cac thu muc output cu - cua nhung lan chay truoc do
        FileSystem dfs = FileSystem.get(getConf());
        deleteSubFolder(dfs, "/output/", "dijkstra");
        System.out.println("--------------------------------------");
        System.out.println("- All old output file are deleted!!! -");
        System.out.println("--------------------------------------");

        //Lưu lại các cặp giá trị n D ở bước lặp trước đó vào _map
        //Lưu lại các cặp giá trị n D ở bước lặp hiện tại vào imap
        //Vòng lặp sẽ dừng lại khi _map giống như imap => không thể cập nhật được thêm
        _map = new HashMap<String, Long>();

        getConf().set("INF_value", Long.toString(INF));
        long smallest = INF;
        long smallest_value = -1;
        long fix_count = 0;
        String fixed = "";
        long stt = 0;
        while (!isdone) {
            getConf().set("smallest_value", Long.toString(smallest_value));

            Job job = new Job(getConf(), "Dijkstra");
            job.setJarByClass(Dijkstra.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(DijkstraMapper.class);
            job.setReducerClass(DijkstraReducer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(inputfile));
            FileOutputFormat.setOutputPath(job, new Path(outputfile));

            success = job.waitForCompletion(true);

            //Xóa bỏ các file input cũ
            if (!inputfile.equals(IN)) {
                String indir = inputfile.replace("part-r-00000", "");
                Path ddir = new Path(indir);
                dfs.delete(ddir, true);
            }
            //Thiết lập lại cấu hình mới cho inputfile và outputfile, inputfile mới là outputfile cũ
            inputfile = outputfile + "/part-r-00000";
            outputfile = OUT + (++STEP);

            //Thiết đặt để không chạy lại giải thuật - giả sử đã hoàn thành
            isdone = true;
            Path ofile = new Path(inputfile);
            imap = new HashMap<String, Long>();
            BufferedReader br = new BufferedReader(new InputStreamReader(dfs.open(ofile)));
            String line = br.readLine(); //Mỗi dòng có dạng 1 1 0 2:1;3:1;
            String n;
            long D;

            smallest = INF;
            fix_count = 0;
            fixed = "";

//            System.out.println("***************** TEST *****************");
            System.out.println("\n***************** Ket thuc lan lap " + (++stt) + " *****************\n");
            while (line != null) {
                //Cần ghi nhận lại cặp n D - đỉnh n và D: độ dài đường đi từ đỉnh xuất phát đến đỉnh n
                String[] sp = line.split(" ");
                n = sp[0];
                D = Long.parseLong(sp[2]);
                imap.put(n, D);

                if (D <= smallest_value) {
                    fix_count++;
                    fixed += n + ";";
                } else if (D < smallest) {
                    smallest = D;
                }
//                System.out.println("* " + line);
                line = br.readLine();
            }

            smallest_value = smallest;
/*
            System.out.println("****************************************");
            System.out.println("* outputfile    : " + inputfile);
            System.out.println("* smallest value: " + smallest_value);
            System.out.println("* fixed         : " + fix_count + " - " + fixed);
            System.out.println("****************************************");
*/
            //Nếu đã lặp đến lần 2 => so sánh giữa 2 lần (so sánh _map và imap)
            if (!_map.isEmpty()) {
                //Kiểm tra giá trị giữa lần lặp trước và lần này có giống nhau không
                for (String dinh_n : imap.keySet()) {
                    D = imap.get(dinh_n);
                    if (_map.get(dinh_n) != D) {
                        //Giá trị D lần lặp trước và lần này khác nhau => còn cập nhật được
                        isdone = false;
                        break;
                    }
                }
            } else { //Ngược lại, đây là lần đầu tiên => hiển nhiên _map là empty
                isdone = false;
            }
            if (!isdone) { //Nếu cần chạy lại giải thuật thì chép imap thành _map
                _map.putAll(imap);
            }
        }
        System.out.println("****************************************");
        System.out.println("* outputfile    : " + inputfile);
        System.out.println("****************************************");
        Path ofile = new Path(inputfile);
        BufferedReader br = new BufferedReader(new InputStreamReader(dfs.open(ofile)));
        String line = br.readLine();
        while (line != null) {
            System.out.println("* " + line);
            line = br.readLine();
        }
//        => in ra duong di tu 1 den dinh nao do
//            vd dinh 4 => 1->2>4
        System.out.println("****************************************");

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Dijkstra(), args));
    }
}
