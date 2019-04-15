1.jar包执行：
首先进入2017st33用户下
我们存放jar包的位置workspace/triangle，然后在命令行输入
	hadoop jar tar.jar /data/graphTriangleCount/twitter_graph_v2.txt output1/ output2/
	就可以运行成功
2.output1中为构建无向图之后输出的图的顶点之间的链接关系
  output2中为输出的最终的三角形个数