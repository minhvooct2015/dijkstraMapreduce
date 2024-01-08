import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DijkstraReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
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
        long shortest = Dijkstra.INF;
//            (10009)
        long D;
        String p = key.toString();
        String status = "";

        for (Text val : values) {
            String valString = val.toString();
            System.out.println("=======================Reducer============================");
            System.out.println("=== INPUT VAlue: " + valString);
            String[] sp = valString.split(" ");
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
        String value_input = p + " " + shortest + " " + pointsTo + " " + status;
        output_value.set(value_input);
        context.write(key, output_value);

        System.out.println("===========================REDUCER: PROCESSED output ========================");
        System.out.println("Key output: " + key.toString() + " Value output: " + value_input);

        output_value.clear();
    }
}