import main.java.BSReducer
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.apache.hadoop.mrunit.mapreduce.MapDriver
import main.java.BSMapper
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

    def "MapperReducer test"() {
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
       return new Text(new File("src/main/test/groovy/test_logs").getText('UTF-8'))
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
