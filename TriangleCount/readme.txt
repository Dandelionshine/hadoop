1.jar包执行：
首先进入2017st33用户下
我们存放jar包的位置workspace/triangle，然后在命令行输入
	hadoop jar tar.jar /data/graphTriangleCount/twitter_graph_v2.txt output1/ output2/
	就可以运行成功
2.output1中为构建无向图之后输出的图的顶点之间的链接关系
  output2中为输出的最终的三角形个数
  
  主要设计思路：
实现图的三角形计数主要分为两个job来执行，第一个job将有向图转为无向图，并且输出与a顶点相连的所有顶点linknodes,那么对于与a顶点相连的两条边（a,b），（a,c）来说，则只要找到（b,c），则就找到一个三角形，所以第二个job计算无向图中的三角形个数。

算法设计、程序和各个类的设计说明：
程序分为三个步骤，第一个是GraphBuild，负责将有向图转为无向图，输出每个节点链接的所有其他节点<node,nodei,nodej,...>,nodei,nodej为与node相连的其他节点，并且node<nodei<nodej.
第二个是TriangleNum，负责计算无向图中的三角形个数。对于nodea，在它的链接节点中找到所有的两个节点nodeb,nodec，已经有边nodea-nodeb,nodea-nodec，则在nodeb节点的所有链接节点中，只要找到nodec，则说明nodeb,nodec之间有边相连，则说明nodea,nodeb,nodec之前两两相连，则三角形个数加一。
第三个是TriangleDriver,负责将前两个步骤串联起来执行两个MapReduce job。
1.CreatUndirectGraph
该步骤主要是从输入的有向图文件中分析建立无向图之间的链接关系。
输入源数据文件为Twitter 局部关系图，每一行由两个以空格分隔的整数组成。
Mapper:对于每行输入<a,b>,输出<Text(min(a,b)),Text(max(a,b))>，确保节点编号小的链接到节点编号大的。对于输入文件中x x这样的自己指向自己的边，不输出到reducer中。
public static class CreatUndirectGraphMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        Text outkey=new Text();
        Text outvalue=new Text();

        protected void map(LongWritable key,Text value,Context context)
                throws IOException,InterruptedException
        {
            String line=value.toString();
            String ids[]=line.split(" ");
            int a=Integer.parseInt(ids[0]);
            int b=Integer.parseInt(ids[1]);
            //Text key = min(a,b)，Text value = max(a,b)
            if(a>b)
            {
                String newkey=b+"";
                outkey.set(newkey);
                String newvalue=a+"";
                outvalue.set(newvalue);
                context.write(outkey,outvalue);

            }
            else
            {
                String newkey=a+"";
                outkey.set(newkey);
                String newvalue=b+"";
                outvalue.set(newvalue);
                context.write(outkey,outvalue);
            }
        }
    }
Reducer：对于map阶段输出的有边相连的节点对，map自动做了排序，对于nodea来说，与它有边相连的其他节点，我们采用TreeSet进行存储，起到去掉无向图中重复边的作用。然后对TreeSet进行遍历，并将节点以nodei,nodej...的形式进行拼接为字符串存储。就得到与nodea相连节点的链接关系，并输出<nodea,nodei + “,”+nodej + “,” + ... + node>到hdfs。
public static class CreatUndirectGraphReducer extends Reducer<Text,Text,Text,Text>
    {
        Text outvalue=new Text();
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException,InterruptedException
        {
            String s="";
            TreeSet<Integer>set=new TreeSet<Integer>();
            for(Text val : values)
            {
                String str=val.toString();
                int i=Integer.parseInt(str);
                set.add(i);
            }
            Iterator<Integer> it=set.iterator();
            while (it.hasNext())
            {
                s=s+it.next()+",";
            }
            outvalue.set(s.substring(0,s.length()-1));
            context.write(key,outvalue);
        }
    }
2.CreatLink
该步骤主要是从无向图的链接关系中分析出三角关系并计数。对于输入的无向图的链接关系<nodea,nodei + “,”+nodej + “,” + ... + node>，在nodea的链接关系中，对于任意与它相连的两个节点nodei,nodej，已经确定有边nodea-nodei,nodea-nodej存在，则在nodei的链接关系中，找到nodej节点，则nodea,nodei,nodej构成一个三角形，三角形个数加一。
Mapper:对于nodea的链接关系，找到所有任意与它相连的两个节点nodei,nodej，输出<Text(nodei),Text(nodej)>,并且输出链接关系<nodea,“a”+ nodei + “,”+nodej + “,” + ... + node>.
public static class CreatLinkMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        Text outkey=new Text();
        Text outvalue=new Text();

        protected void map(LongWritable key,Text value,Context context)
                throws IOException, InterruptedException
        {
            String line=value.toString();
            String split[]=line.split("\t");
            outkey.set(split[0]);

            String value1="a"+split[1];
            outvalue.set(value1);

            context.write(outkey,outvalue);

            if (split[1].contains(","))
            {
                String sp2[]=split[1].split(",");
                for(int i=0;i<sp2.length-1;i++)
                {
                    for (int j=i+1;j<sp2.length;j++)
                    {
                        outkey.set(sp2[i]);
                        outvalue.set(sp2[j]);
                        context.write(outkey,outvalue);
                    }
                }
            }
        }
    }
Reducer:若收到的输入是nodea的链接关系（以“a”开头的），则将链接关系中与nodea相连的所有节点存储到一个HashMap中方便查询（复杂度为n），若收到的输入是<nodea,nodeb>，则是需要查找是否有nodea-nodeb边存在的，则在nodea的链接关系中查找是否有nodeb节点（代码在HashMap中查询），若找到，则三角形个数加一。在最后的cleanup中输出三角形的总数。
public static class CreatLinkReducer extends Reducer<Text,Text,IntWritable,Text>
    {
        static IntWritable outkey=new IntWritable();
        static Text outvalue=new Text("");
        static int l=0;
        protected void reduce(Text key,Iterable<Text>values,Context context)
                throws IOException,InterruptedException
        {
            ArrayList<String> list=new ArrayList<String>();
            HashMap<String,Boolean> map=new HashMap<String, Boolean>();
            for (Text val:values)
            {
                String v=val.toString();
                if(v.startsWith("a"))
                {
                    String v2=v.substring(1);
                    String sp[]=v2.split(",");
                    for (int i=0;i<sp.length;i++)
                    {
                        map.put(sp[i],true);
                    }
                }
                else
                {
                    list.add(v);
                }
            }
            for(int i=0;i<list.size();i++)
            {
                if(map.containsKey(list.get(i)))
                {
                    l++;
                }
            }
            outkey.set(l);
        }
protected void cleanup(Reducer<Text,Text,IntWritable,Text>.Context context)
                throws IOException,InterruptedException
        {
            context.write(outkey,outvalue);
        }
3.Driver
将上述两个步骤串联起来执行两次MapReduce。运行时将这三个类打包成一个jar文件。
public static void main(String[]args)
            throws Exception
    {
        String forCD[]={args[0],args[1]};
        CreatUndirectGraph.main(forCD);
        String forCL[]={args[1],args[2]};
        CreatLink.main(forCL);
    }
二、最终统计出的三角形个数
数据集	三角形个数	Driver程序在集群上运行的时间（秒）
Twitter	13082506	1mins6sec+31mins39sec=1965sec
