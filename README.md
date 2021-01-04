# monitor_long_active_threads
监控MySQL正在执行的超过指定时间的thread，并发送钉钉推送


默认监控执行时间大于5s的session，程序每秒执行一次，故存在1s误差，可能执行时间为5s-6s之间的session不被抓取。

使用方法：
1.setting.py中配置db连接信息
2.修改钉钉机器人配置
3.python check_thread.py -t 8     # 监控执行时间大于8s的session