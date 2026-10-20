# -*- coding: utf-8 -*-
"""模拟延时关单消息丢失：下单 → 把 order_time 改到 20 分钟前 → 观察对账任务补偿关单、座位与余票只释放一次。
之后原始延时消息（10 分钟后）到达时，应被 CAS 拒绝，余票不再变化（由 reconcile_late.py 检查）。"""
import json, io, time, random, socket, subprocess, urllib.request, urllib.parse

GW = 'http://127.0.0.1:9000'
DOCKER = 'docker'
KEY = 'index12306-ticket-service:train_station_remaining_ticket:1_北京南_杭州东'


def call(method, path, body=None, token=None, params=None):
    url = GW + path + ('?' + urllib.parse.urlencode(params) if params else '')
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    h = {'Content-Type': 'application/json'}
    if token:
        h['Authorization'] = token
    return json.load(urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h, method=method), timeout=30))


def sql(q):
    return subprocess.run([DOCKER, 'exec', 'mysql8', 'mysql', '-uroot', '-proot', '-N', '12306', '-e', q],
                          capture_output=True, text=True, encoding='utf-8').stdout.strip()


def remaining():
    s = socket.create_connection(('127.0.0.1', 6380))
    s.sendall(b'*2\r\n$4\r\nAUTH\r\n$6\r\n123456\r\n'); s.recv(64)
    k = KEY.encode()
    s.sendall(b'*3\r\n$4\r\nHGET\r\n$%d\r\n%s\r\n$1\r\n0\r\n' % (len(k), k)); time.sleep(0.05)
    r = s.recv(256).decode(); s.close()
    return int(r.split('\r\n')[1])


def idcard():
    base = '11010119900101' + str(random.randint(100, 999))
    w = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
    return base + '10X98765432'[sum(int(a) * b for a, b in zip(base, w)) % 11]


def order_table(osn):
    for i in range(16):
        if sql(f"select count(*) from t_order_{i} where order_sn='{osn}'") == '1':
            return f't_order_{i}'


uname = 'recon' + str(int(time.time() * 1000))[-8:]
ic, ph = idcard(), '137' + str(random.randint(10000000, 99999999))
call('POST', '/api/user-service/register', {'username': uname, 'password': 'Recon@123', 'realName': '对账测试', 'idType': 0,
                                            'idCard': ic, 'phone': ph, 'mail': uname + '@t.com', 'userType': 0})
token = call('POST', '/api/user-service/v1/login', {'usernameOrMailOrPhone': uname, 'password': 'Recon@123'})['data']['accessToken']
call('POST', '/api/user-service/passenger/save', {'realName': '对账测试', 'idType': 0, 'idCard': ic, 'discountType': 0, 'phone': ph}, token)
pid = call('GET', '/api/user-service/passenger/query', token=token)['data'][0]['id']

before_buy = remaining()
buy = call('POST', '/api/ticket-service/ticket/purchase/v2', {'trainId': '1', 'departure': '北京南', 'arrival': '杭州东',
                                                              'passengers': [{'passengerId': pid, 'seatType': 0}], 'chooseSeats': []}, token)
osn = buy['data']['orderSn']
seat = buy['data']['ticketOrderDetails'][0]
after_buy = remaining()
tbl = order_table(osn)
sql(f"update {tbl} set order_time = date_sub(now(), interval 20 minute) where order_sn='{osn}'")
t0 = time.time()
status = None
while time.time() - t0 < 60:
    status = sql(f"select status from {tbl} where order_sn='{osn}'")
    if status == '30':
        break
    time.sleep(1)
closed_after = round(time.time() - t0, 1)
time.sleep(2)
after_close = remaining()
locked = sql(f"select count(*) from t_seat where train_id=1 and carriage_number='{seat['carriageNumber']}' "
             f"and seat_number='{seat['seatNumber']}' and seat_status<>0")
res = dict(orderSn=osn, table=tbl, seat=f"{seat['carriageNumber']}车{seat['seatNumber']}", purchased_at=time.strftime('%H:%M:%S'),
           remaining_before_buy=before_buy, remaining_after_buy=after_buy, remaining_after_reconcile=after_close,
           final_status=status, closed_within_seconds=closed_after, seat_rows_still_locked=int(locked),
           passed=(status == '30' and after_close == before_buy and locked == '0'))
io.open('reconcile_result.json', 'w', encoding='utf-8').write(json.dumps(res, ensure_ascii=False, indent=1))
print('passed', res['passed'], 'closed_within_s', closed_after, 'remaining', before_buy, after_buy, after_close, 'locked', locked)
