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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

public class Dijkstra extends Configured implements Tool {
    public static final long INF = 10009;
    public static final long MAX = 10000;
    public static String OUT = "/output/dijkstra";
    public static String IN = "/input/dijkstra";
    public static long STEP = 0;
    HashMap<String, Long> beforeMap;
    HashMap<String, Long> currentMap;
    Map<String, LinkedList<String>> result = new HashMap<>();
    String originalNode = "";

    public void deleteSubFolder(FileSystem dfs, String pFolder, String SubFolderPrefix) throws Exception {
        FileStatus[] fileStatus = dfs.listStatus(new Path(pFolder));
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        for (Path path : paths) {
            if (path.getName().startsWith(SubFolderPrefix))
                dfs.delete(path, true);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //Cấu hình giá trị key - value ngăn cách bởi khoảng trắng
        System.out.println("================RUN===================================");
        getConf().set("mapreduce.output.textoutputformat.separator", " ");
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
        beforeMap = new HashMap<String, Long>();

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
            currentMap = new HashMap<String, Long>();
            BufferedReader br = new BufferedReader(new InputStreamReader(dfs.open(ofile)));
            String line = br.readLine(); //Mỗi dòng có dạng 1 1 0 2:1;3:1;
            String n;
            long D;

            smallest = INF;
            fix_count = 0;
            fixed = "";

            System.out.println("\n***************** Ket thuc lan lap " + (++stt) + " *****************\n");
            while (line != null) {
                //Cần ghi nhận lại cặp n D - đỉnh n và D: độ dài đường đi từ đỉnh xuất phát đến đỉnh n
                System.out.println("*** RUN *** Line0 === " + line);
                String[] sp = line.split(" ");
                n = sp[0];
                D = Long.parseLong(sp[2]);
                currentMap.put(n, D);

                if (D <= smallest_value) {
                    fix_count++;
                    fixed += n + ";";
                } else if (D < smallest) {
                    smallest = D;
                }
                System.out.println("*** RUN *** Line === " + line);
                line = br.readLine();
            }

            smallest_value = smallest;

            System.out.println("****************************************");
            System.out.println("* outputfile    : " + inputfile);
            System.out.println("* smallest value: " + smallest_value);
            System.out.println("* fixed         : " + fix_count + " - " + fixed);
            System.out.println("****************************************");
            System.out.println("============= INPUT BEFORE MAP ================== " + stt + " " + maptoString(beforeMap));
            System.out.println("============= INPUT CURRENT MAP ================== " + stt + " " + maptoString(currentMap));
            //Nếu đã lặp đến lần 2 => so sánh giữa 2 lần (so sánh _map và imap)
            if (!beforeMap.isEmpty()) {
                //Kiểm tra giá trị giữa lần lặp trước và lần này có giống nhau không
                for (String dinh_n : currentMap.keySet()) {
                    System.out.println("*** RUN *** Dinh_n === " + dinh_n);
                    D = currentMap.get(dinh_n);
                    if (beforeMap.get(dinh_n) != D) {
                        //Giá trị D lần lặp trước và lần này khác nhau => còn cập nhật được
                        isdone = false;
                        System.out.println("*** RUN *** isdone beforeMap.isEmpty() === " + isdone);
                        break;
                    }
                }
            } else { //Ngược lại, đây là lần đầu tiên => hiển nhiên _map là empty
                isdone = false;
                System.out.println("*** RUN *** isdone ELSE === " + isdone);
            }
            if (!isdone) { //Nếu cần chạy lại giải thuật thì chép imap thành _map
                System.out.println("*** RUN *** isdone !isdone === " + isdone);
                //do 2 map khac nhau, nen cap nhap lai map
                beforeMap.putAll(currentMap);
            }
            System.out.println("============= BEFORE MAP ================== " + stt + " " + maptoString(beforeMap));
            System.out.println("============= CURRENT MAP ================== " + stt + " " + maptoString(currentMap));
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

        System.out.println("****************************************");

        return success ? 0 : 1;
    }

    private String maptoString(HashMap<String, Long> beforeMap) {
        if(beforeMap.isEmpty()) return  " EMPTY MAP ";
        return beforeMap.keySet().stream()
                .map(key -> key + "=" + beforeMap.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Dijkstra(), args));

    }
}
