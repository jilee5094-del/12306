# -*- coding: utf-8 -*-
"""并发取消同一订单 N 次，统计成功次数与 Redis 余票增量。用法：python race_cancel.py <标签> [N] [轮数]
期望（正确实现）：每轮恰好 1 次成功，余票增量 = 乘车人数(1)。"""
import json, io, sys, time, random, threading, socket, urllib.request, urllib.parse, datetime
from concurrent.futures import ThreadPoolExecutor

GW = 'http://127.0.0.1:9000'
TAG = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 20
ROUNDS = int(sys.argv[3]) if len(sys.argv) > 3 else 10
KEY = 'index12306-ticket-service:train_station_remaining_ticket:1_北京南_杭州东'


def call(method, path, body=None, token=None, params=None):
    url = GW + path + ('?' + urllib.parse.urlencode(params) if params else '')
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    h = {'Content-Type': 'application/json'}
    if token:
        h['Authorization'] = token
    return json.load(urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h, method=method), timeout=30))


def redis(*args):
    s = socket.create_connection(('127.0.0.1', 6380))
    def send(*a):
        m = f'*{len(a)}\r\n' + ''.join(f'${len(x.encode())}\r\n{x}\r\n' for x in a)
        s.sendall(m.encode()); time.sleep(0.05); return s.recv(65536).decode('utf-8', 'replace')
    send('AUTH', '123456')
    r = send(*args); s.close(); return r


def remaining(seat_type='0'):
    r = redis('HGET', KEY, seat_type)
    return int(r.split('\r\n')[1]) if r.startswith('$') and not r.startswith('$-1') else None


def idcard():
    base = '11010119900101' + str(random.randint(100, 999))
    w = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
    return base + '10X98765432'[sum(int(a) * b for a, b in zip(base, w)) % 11]


uname = 'race' + str(int(time.time() * 1000))[-8:]
ic = idcard()
ph = '139' + str(random.randint(10000000, 99999999))
call('POST', '/api/user-service/register', {'username': uname, 'password': 'Race@123', 'realName': '并发测试', 'idType': 0,
                                            'idCard': ic, 'phone': ph,
                                            'mail': uname + '@t.com', 'userType': 0})
token = call('POST', '/api/user-service/v1/login', {'usernameOrMailOrPhone': uname, 'password': 'Race@123'})['data']['accessToken']
call('POST', '/api/user-service/passenger/save', {'realName': '并发测试', 'idType': 0, 'idCard': ic, 'discountType': 0, 'phone': ph}, token)
pid = call('GET', '/api/user-service/passenger/query', token=token)['data'][0]['id']

rounds = []
for _ in range(ROUNDS):
    buy = call('POST', '/api/ticket-service/ticket/purchase/v2', {'trainId': '1', 'departure': '北京南', 'arrival': '杭州东',
                                                                  'passengers': [{'passengerId': pid, 'seatType': 0}], 'chooseSeats': []}, token)
    osn = (buy.get('data') or {}).get('orderSn')
    if not osn:
        rounds.append({'error': buy.get('message')}); break
    before = remaining()
    barrier = threading.Barrier(N)

    def one(_):
        barrier.wait()
        try:
            return call('POST', '/api/ticket-service/ticket/cancel', {'orderSn': osn}, token).get('code')
        except Exception as e:
            return 'EXC'
    with ThreadPoolExecutor(N) as ex:
        codes = list(ex.map(one, range(N)))
    time.sleep(0.5)
    after = remaining()
    rounds.append({'orderSn': osn, 'success': codes.count('0'), 'remaining_before': before, 'remaining_after': after,
                   'delta': (after - before) if None not in (before, after) else None})

summary = {'tag': TAG, 'concurrency': N, 'rounds': len(rounds),
           'rounds_with_multi_success': sum(1 for r in rounds if r.get('success', 0) > 1),
           'rounds_with_over_release': sum(1 for r in rounds if (r.get('delta') or 0) > 1),
           'total_over_released_tickets': sum(max((r.get('delta') or 0) - 1, 0) for r in rounds),
           'detail': rounds}
io.open(f'race_{TAG}.json', 'w', encoding='utf-8').write(json.dumps(summary, ensure_ascii=False, indent=1))
print(TAG, 'multi_success_rounds=', summary['rounds_with_multi_success'], 'over_release_rounds=', summary['rounds_with_over_release'],
      'over_released_tickets=', summary['total_over_released_tickets'])
