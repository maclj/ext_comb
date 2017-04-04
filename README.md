# ext_comb：Hadoop多项目合并
<br>
此项目主要针对目前项目开发的常见需求：合并且同时运行多个基于相同数据源（数据格式）的程序。<br>
同时，采用annotation定义Job以简化代码，并使命令行配置优先于程序定义以便在运行期间进行参数的变更。<br>



## 运行脚本样例：<br>
hadoop jar hadoop-ext.jar com.hadoop.entry.Main \
-libjars your.jar \
-D mapreduce.input.fileinputformat.split.minsize=1 \
-D mapreduce.input.fileinputformat.split.maxsize=256000000 \
-D job.comb.0.lazyOutputEnabled=true \
-D job.name=comb \
-i /input/ \
-o /output/ \
-d 2017-04-01

## 运行配置样例：<br>
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
