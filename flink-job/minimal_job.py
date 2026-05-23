from pyflink.datastream import StreamExecutionEnvironment

# Minimal PyFlink streaming job that doubles numbers and prints them

env = StreamExecutionEnvironment.get_execution_environment()
# simple collection source
nums = env.from_collection([1, 2, 3, 4, 5])
# transform and print
nums.map(lambda x: x * 2).print()

env.execute('minimal-pyflink-job')
