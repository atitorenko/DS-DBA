package main.java;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class RecordReaderWrapper extends RecordReader<LongWritable, Text> {
    private LineReader in;
    private final static Text EOL = new Text("\n");
    private Pattern delimiterPattern;
    private String delimiterRegex;
    private int maxLengthRecord;

    @Override
    public void initialize(InputSplit split,
                           TaskAttemptContext context)
            throws IOException, InterruptedException {

        Configuration job = context.getConfiguration();
        this.delimiterRegex = job.get("record.delimiter.regex");
        this.maxLengthRecord = job.getInt(
                "mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);

        delimiterPattern = Pattern.compile(delimiterRegex);
        ../..
    }

    private int readNext(Text text,
                         int maxLineLength,
                         int maxBytesToConsume)
            throws IOException {

        int offset = 0;
        text.clear();
        Text tmp = new Text();

        for (int i = 0; i < maxBytesToConsume; i++) {

            int offsetTmp = in.readLine(
                    tmp,
                    maxLineLength,
                    maxBytesToConsume);
            offset += offsetTmp;
            Matcher m = delimiterPattern.matcher(tmp.toString());

            // End of File
            if (offsetTmp == 0) {
                break;
            }

            if (m.matches()) {
                // Record delimiter
                break;
            } else {
                // Append value to record
                text.append(EOL.getBytes(), 0, EOL.getLength());
                text.append(tmp.getBytes(), 0, tmp.getLength());
            }
        }
        return offset;
    }
}
