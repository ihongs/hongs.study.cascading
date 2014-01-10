hongs.study.cascading
=====================

学习 Cascading 的一些代码和笔记

1. 编译方法:

mvn package

注: 在项目目录下执行. 需要安装 maven. 如果打包出错, 请修改 pom.xml, 将 cascading-local 和 cascading-hadoop 的 classifier 的注释暂时去掉, 打包完成后, 再将这两项注释, 再打一次包.

2. 执行方法:

java -cp target/hongs-study-cascading-0.0.1-jar-with-dependencies.jar hongs.study.cascading.Basic words.txt words_ext.txt words_out.txt

注: 如果需要在 hadoop 上执行, 请在命令末尾增加一个参数: hadoop.

3. 演示代码:

https://github.com/ihongs/hongs.study.cascading/blob/master/src/main/java/hongs/study/cascading/Basic.java

注: 代码已很精简, 内有少量注释标明注意点.
