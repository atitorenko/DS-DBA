package ru.mephi.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Класс редьюсера.
 */
public class BSReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable();

    /**
     * Функция консолидации данных. Суммирует вхождения каждого из браузеров в логах.
     * @param key имя браузера.
     * @param values коллекция вхождений в лог файле.
     * @param context контекст.
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
