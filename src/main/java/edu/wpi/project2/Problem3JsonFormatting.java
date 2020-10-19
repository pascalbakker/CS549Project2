package edu.wpi.project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.util.Arrays;

public class Problem3JsonFormatting {
    static class JsonInputFormat extends FileInputFormat<Text, Text> {
        static class JSONRecordReader extends RecordReader<Text, Text> {
            private FSDataInputStream fsDataInputStream;
            private Text key;
            private Text value;
            private long endOfFileLong;
            public LineRecordReader lineRecordReader;
            private DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
            private boolean inPart = true;

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                FileSplit fileSplit = (FileSplit)inputSplit;
                Configuration conf = taskAttemptContext.getConfiguration();
                Path path = fileSplit.getPath();
                FileSystem fileSystem = path.getFileSystem(conf);

                fsDataInputStream = fileSystem.open(path);
                endOfFileLong = fileSplit.getStart()+fileSplit.getLength();
                long start = fileSplit.getStart();
                fsDataInputStream.seek(start);
                if (start != 0) {
                    readBytes("}".getBytes(), false);
                }
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return key;
            }

            @Override
            public Text getCurrentValue() throws IOException, InterruptedException {
                return value;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0.f;
            }

            @Override
            public void close() throws IOException {
                fsDataInputStream.close();
            }

            public boolean isEqalToByte(int index, byte[] identifier, int buffer){
                    return identifier[index] == buffer;
            }

            private boolean readBytes(byte[] identifier, boolean insidePart) throws IOException {
                int i = 0;
                while (true) {
                    int b = fsDataInputStream.read();
                    if (b == -1){
                        //System.out.println(i);
                        return false;
                    }
                    if(insidePart){
                        dataOutputBuffer.write(b);
                    }
                    if(isEqalToByte(i, identifier, b)) {
                        i++;
                        if (i >= identifier.length) {
                            return fsDataInputStream.getPos() < endOfFileLong;
                        }
                    } else {
                        i = 0;
                    }
                }
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (!inPart)
                    return false;
                else{
                    if (!readBytes("}".getBytes(), true)){
                        inPart = false;
                    }
                    String jsonString = new String(Arrays.copyOfRange(dataOutputBuffer.getData(), 0, dataOutputBuffer.getLength())).trim();

                    // get flags and elevation
                    String[] jsonvalues = jsonString.replace("}","")
                                                    .replace("{","")
                                                    .replace("\"","")
                                                    .replace(": ", "~")
                                                    .trim()
                                                    .split(",");
                    String combined = "";
                    for (String v : jsonvalues){
                        if(v.contains("Flags") || v.contains("Elevation"))
                            combined += v.trim() + ",";
                    }
                    // System.out.println(combined);
                    if(combined.length()>0)
                        combined = combined.substring(0,combined.length()-1);
                    value = new Text(combined);
                    key = new Text(fsDataInputStream.getPos() + "");
                    dataOutputBuffer.reset();
                    return true;
                }
            }
        }

        public long computeSplitSize(long bS, long minS, long maxS) {
            // return bs/minS;
            // 967618 bytes / 5= 193523.6 bytes
            return 193524;
        }

        public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            JSONRecordReader jsonRecordReader = new JSONRecordReader();
            jsonRecordReader.initialize(inputSplit, taskAttemptContext);
            return jsonRecordReader;
        }
    }

    static class CustomMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            if (values.length == 2) {
                // map flags and elevations
                String flag = values[0].split("~")[1];
                String elevation = values[1].split("~")[1];
                context.write(new Text(flag), new Text(elevation));
            }else{

            }
        }
    }

    static class CustomReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // get min/max elevation
            int minElevation = Integer.MAX_VALUE;
            int maxElevation = Integer.MIN_VALUE;
            for (Text t : values) {
                int elevation = Integer.parseInt(t.toString());
                if (elevation<minElevation) minElevation=elevation;
                if (elevation>maxElevation) maxElevation=elevation;
            }
            Text outputText = new Text(minElevation + "," + maxElevation);
            context.write(new Text(key), outputText);
        }
    }
}
