# ext_comb：merges function to run
<br>
This project focuses on the common needs of development: merging and running multiple functions based on the same data source (data format) at the same time.<br>
Annotations are used to define the Job to simplify the code and make the command line configuration take precedence over the program definition to make changes to the parameters during runtime.
<br>



## Script sample：<br>
hadoop jar hadoop-ext.jar com.hadoop.entry.Main \
-libjars your.jar \
-D mapreduce.input.fileinputformat.split.minsize=1 \
-D mapreduce.input.fileinputformat.split.maxsize=256000000 \
-D job.comb.0.lazyOutputEnabled=true \
-D job.name=comb \
-i /input/ \
-o /output/ \
-d 2017-04-01

## Code sample：<br>
@JobDefine( \
    project="comb", \
    inputPath = "input", \
    outputPath = "output", \
    jobSeq = 0, \
    jarByClass = Main.class, \
    inputFormatClass = TextInputFormat.class, \
    mapperClass = ContainerKeyMapperImpl.class, \
    combinerClass = ContainerKeyCombinerTextImpl.class, \
    reducerClass = ContainerKeyReducerTextImpl.class, \
	  mapOutputKeyClass = Text.class, \
	  mapOutputValueClass = Text.class, \
	  outputKeyClass = Text.class, \
	  outputValueClass = Text.class, \
    lazyOutputEnabled = false, \
    numReduceTasks = 24, \
    containerMappers = { \
            PvMapper.class, \
            UvMapper.class \
    }, \
    containerReducers = { \
            PvReducer.class, \
            UvReducer.class \
    }, \
    extAction = JobExtActionImpl.class, \
    jobPathResolver = JobPathResolverImpl.class \
) \
