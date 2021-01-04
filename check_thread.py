#!/usr/local/bin/python3.7

import pymysql
import time
import json
import requests
import schedule
import argparse
from settings import DBLIST_INFO


def connectdb(**dblist):
    # 获取执行时间大于5s的SQL，where time>5
    sql = '''
        select
            id,
            user,
            host,
            "%s" as project,
            db,
            time,
            date_add(now(), INTERVAL -time SECOND)	exec_stime,
            command,
            state,
            info,
            sha(
                concat(
                    id,
                    ifnull(host, ''),
                    ifnull(db, ''),
                    info
                )
            ) as vsha
        from
            information_schema.`processlist`
        where
            info <> ''
        and time >= %s
        and user <>'mysqldump'
     ''' % (dblist['project'], args.t)

    try:
        conn = pymysql.connect(host=dblist['host'], user=dblist['username'], passwd=dblist['password'],
                               port=dblist['port'], charset='utf8',
                               cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        cursor.execute(sql)
        tmp_result = cursor.fetchall()
        if not tmp_result:
            tmp_result = list(tmp_result)
        return tmp_result

    except Exception as e:
        print('Error By connectdb %s' % e)

    finally:
        cursor.close()
        conn.close()


def checkthred(plist, result):
    try:
        # 判断session是否新SQL，若是，则钉钉推送，若不是，则更新time的值
        if not plist:
            for v_sql in result:
                plist.append(v_sql)
                ding_md = '# <font face=\"微软雅黑\">发现SLOW SQL</font> \n **InstanceName:**  <font font-weight=700 color=\"#000080\">%s</font><br /> \n \n **ThreadID:**  <font color=\"#000080\">%s</font><br /> \n \n  **User:**  <font color=\"#000080\">%s</font><br /> \n \n **Host:**  <font color=\"#000080\">%s</font><br /> \n \n**DB:**  <font color=\"#000080\">%s</font><br /> \n \n **Command:**  <font color=\"#000080\">%s</font><br /> \n \n **State:**  <font color=\"#000080\">%s</font><br /> \n \n **ExecStartTime:**  <font color=\"#000080\">%s</font><br /> \n \n **ExecTime(s):**  <font color=\"#f90202\">%s</font><br /> \n \n**SQL:**  <font color=\"#000080\">%s</font><br /> \n' % (
                    v_sql['project'], v_sql['id'], v_sql['user'], v_sql['host'], v_sql['db'], v_sql['command'],
                    v_sql['state'], v_sql['exec_stime'], v_sql['time'], v_sql['info'])
                dingding_robot(ding_md)
        else:
            for active_session in result:
                for i in range(len(plist)):
                    if active_session['vsha'] != plist[i]['vsha']:
                        if i == len(plist) - 1:
                            plist.append(active_session)
                            ding_md = '# <font face=\"微软雅黑\">发现SLOW SQL</font> \n **InstanceName:**  <font font-weight=700 color=\"#000080\">%s</font><br /> \n \n **ThreadID:**  <font color=\"#000080\">%s</font><br /> \n \n  **User:**  <font color=\"#000080\">%s</font><br /> \n \n **Host:**  <font color=\"#000080\">%s</font><br /> \n \n**DB:**  <font color=\"#000080\">%s</font><br /> \n \n **Command:**  <font color=\"#000080\">%s</font><br /> \n \n **State:**  <font color=\"#000080\">%s</font><br /> \n \n **ExecStartTime:**  <font color=\"#000080\">%s</font><br /> \n \n **ExecTime(s):**  <font color=\"#f90202\">%s</font><br /> \n \n**SQL:**  <font color=\"#000080\">%s</font><br /> \n' % (
                                active_session['project'], active_session['id'], active_session['user'],
                                active_session['host'], active_session['db'],
                                active_session['command'],
                                active_session['state'], active_session['exec_stime'], active_session['time'],
                                active_session['info'])
                            dingding_robot(ding_md)
                        else:
                            continue
                    else:
                        plist[i]['time'] = active_session['time']
                        break

        # 判断session是否执行完毕，若是，则从列表中删除此sql，并钉钉推送执行完成，若否，则不做处理
        if not result:
            for index, plist_session in enumerate(plist):
                ding_md = '# <font face=\"微软雅黑\">SLOW SQL执行完成</font> \n **InstanceName:**  <font font-weight=700 color=\"#000080\">%s</font><br /> \n \n **ThreadID:**  <font color=\"#000080\">%s</font><br /> \n \n  **User:**  <font color=\"#000080\">%s</font><br /> \n \n **Host:**  <font color=\"#000080\">%s</font><br /> \n \n**DB:**  <font color=\"#000080\">%s</font><br /> \n \n **Command:**  <font color=\"#000080\">%s</font><br /> \n \n **State:**  <font color=\"#000080\">%s</font><br /> \n \n **ExecStartTime:**  <font color=\"#000080\">%s</font><br /> \n \n **ExecTime(s):**  <font color=\"#32CD32\">%s</font><br /> \n \n**SQL:**  <font color=\"#000080\">%s</font><br /> \n' % (
                    plist_session['project'], plist_session['id'], plist_session['user'], plist_session['host'],
                    plist_session['db'],
                    plist_session['command'],
                    plist_session['state'], plist_session['exec_stime'], plist_session['time'], plist_session['info'])
                dingding_robot(ding_md)
                plist.pop(index)

        elif plist:
            for index, plist_session in enumerate(plist):
                for i in range(len(result)):
                    if plist_session['vsha'] == result[i]['vsha']:
                        break
                    else:
                        if plist_session['vsha'] != result[i]['vsha'] and i == len(result) - 1:
                            ding_md = '# <font face=\"微软雅黑\">SLOW SQL执行完成</font> \n **InstanceName:**  <font font-weight=700 color=\"#000080\">%s</font><br /> \n \n **ThreadID:**  <font color=\"#000080\">%s</font><br /> \n \n  **User:**  <font color=\"#000080\">%s</font><br /> \n \n **Host:**  <font color=\"#000080\">%s</font><br /> \n \n**DB:**  <font color=\"#000080\">%s</font><br /> \n \n **Command:**  <font color=\"#000080\">%s</font><br /> \n \n **State:**  <font color=\"#000080\">%s</font><br /> \n \n **ExecStartTime:**  <font color=\"#000080\">%s</font><br /> \n \n **ExecTime(s):**  <font color=\"#32CD32\">%s</font><br /> \n \n**SQL:**  <font color=\"#000080\">%s</font><br /> \n' % (
                                plist_session['project'], plist_session['id'], plist_session['user'],
                                plist_session['host'], plist_session['db'],
                                plist_session['command'],
                                plist_session['state'], plist_session['exec_stime'], plist_session['time'],
                                plist_session['info'])
                            dingding_robot(ding_md)
                            plist.pop(index)
                        else:
                            continue
        else:
            pass

    except Exception as e:
        print("Error by checkthred %s" % e)
        exit(-1)


def dingding_robot(content):
    # 替换钉钉 access_token
    webhook = "https://oapi.dingtalk.com/robot/send?access_token=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    headers = {'content-type': 'application/json'}
    data = {"msgtype": "markdown", "markdown": {"title": "SLOW SQL监控", "text": content}}
    r = requests.post(webhook, headers=headers, data=json.dumps(data))
    r.encoding = 'utf-8'
    return (r.text)


def main():
    global result
    result = []
    for db in DBLIST_INFO:
        result = result + connectdb(**db)

    checkthred(plist, result)


if __name__ == "__main__":
    global plist
    plist = []

    # 指定session的执行时间，默认5秒
    parser = argparse.ArgumentParser(usage='%(prog)s [options]')
    parser.add_argument('-t', default='5', metavar='--time', help='time active threads run')
    args = parser.parse_args()

    # main()

    ## 定时任务，每秒执行一次,sleep 一秒
    schedule.every(1).seconds.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)
