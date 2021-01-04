'''

监控的DB列表
仅支持MySQL
账号需要有information_schema.`processlist`的读权限

'''


DBLIST_INFO = [{'host': '192.168.211.1', 'port': 3306, 'username': 'root', 'password': 'root123','project': '测试环境CRM主实例'},
               {'host': '192.168.211.1', 'port': 3307, 'username': 'root', 'password': 'root123','project': '测试环境CMS主实例'}]
