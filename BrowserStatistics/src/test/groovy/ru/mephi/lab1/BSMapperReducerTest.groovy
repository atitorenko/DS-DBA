package ru.mephi.lab1

import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.apache.hadoop.mrunit.mapreduce.MapDriver
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver
import org.apache.hadoop.mrunit.types.Pair
import spock.lang.*


/**
 * Created by Alex on 15.10.2017.
 */

class BSMapperReducerTest extends Specification {
    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver
    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver
    MapReduceDriver<LongWritable, Text, Text,  LongWritable, Text, LongWritable> mapReduceDriver

    def MOZILLA = 'Mozilla/5.0'
    def IE = 'IE/5.0'

    void setup() {
        def mapper = new BSMapper()
        def reducer = new BSReducer()
        mapDriver = MapDriver.newMapDriver(mapper)
        reduceDriver = ReduceDriver.newReduceDriver(reducer)
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer)
    }

    def "MapperToReducer test"() {
        given:
        List<Pair<Text, LongWritable>> expected  = newExpected()

        expect:
        mapDriver.withInput(new LongWritable(), newText())
        expected.each {it -> mapDriver.addOutput(it)}
        mapDriver.runTest()
    }

    def "Reducer test"() {
        given:
        def valuesMozilla = new ArrayList<LongWritable>()
        (1..4).each {valuesMozilla.add(new LongWritable(1))}

        expect:
        reduceDriver.withInput(new Text(MOZILLA), valuesMozilla)
        reduceDriver.withOutput(new Text(MOZILLA), new LongWritable(4))
        reduceDriver.runTest()
    }

    def "MapReduce test"() {
        given:
        def logs = newText()

        expect:
        mapReduceDriver.withInput(new LongWritable(), logs)
        mapReduceDriver.addOutput(new Text(IE), new LongWritable(2))
        mapReduceDriver.addOutput(new Text(MOZILLA), new LongWritable(4))
        mapReduceDriver.runTest()
    }

    Text newText() {
        def logs = """127.0.0.1 - - [24/Apr/2011:04:06:01 -0400] "GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1" 200 40028 "-" "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)"
127.0.0.1 - - [24/Apr/2011:04:18:54 -0400] "GET /~strabal/grease/photo1/T97-4.jpg HTTP/1.1" 200 6244 "-" "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)"
127.0.0.1 - - [24/Apr/2011:04:20:11 -0400] "GET /sun_ss5/ HTTP/1.1" 200 14917 "http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F" "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16"
127.0.0.1 - - [24/Apr/2011:04:20:11 -0400] "GET /sun_ss5/ HTTP/1.1" 200 14917 "http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F" "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16"
127.0.0.1 - - [24/Apr/2011:04:20:11 -0400] "GET /sun_ss5/pdf.gif HTTP/1.1" 200 390 "http://host2/sun_ss5/" "IE/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16"
127.0.0.1 - - [24/Apr/2011:04:20:11 -0400] "GET /sun_ss5/pdf.gif HTTP/1.1" 200 390 "http://host2/sun_ss5/" "IE/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16"
        """ as String
        Text text = new Text(logs.getBytes('utf-8'))
        return text
    }

    List<Pair<Text, LongWritable>> newExpected() {
        def list = []
        (1..4).each {list.add(getPair(MOZILLA, 1))}
        (1..2).each {list.add(getPair(IE, 1))}
        list
    }

    Pair<Text, LongWritable> getPair(String key, long value){
        return new Pair<Text, LongWritable>(new Text(key), new LongWritable(value))
    }
}
