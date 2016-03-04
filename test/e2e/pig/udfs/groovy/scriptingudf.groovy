import org.apache.pig.scripting.groovy.OutputSchemaFunction;
class GroovyUDFs {
    @OutputSchemaFunction('squareSchema')
    public static square(x) {
        return x * x;
    }
    public static squareSchema(input) {
        return input;
    }
}
