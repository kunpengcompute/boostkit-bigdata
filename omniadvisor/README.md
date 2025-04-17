1 对负载进行调优
python tunning.py -l 16 -r backend -rt 1 -t iterative
各参数说明，可以通过python3 tunning.py --help 查看

2 负载劫持优化
如果需要开启优化，需要加上环境变量 enable_omniadvisor=true
（对于开发人员，如果需要指定hijack.py文件，那么可以在环境变量中添加：HIJACK_PATH=xxx）

