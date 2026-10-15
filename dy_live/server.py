import argparse
import contextlib
import gzip
import hashlib
import importlib
import math
import os
import shutil
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import Enum
from io import TextIOWrapper
from os import path
from pathlib import Path
from typing import Callable
from urllib.parse import urlencode
from loguru import logger

import websocket

from google.protobuf.message import Message
from dy_live.protobuf.douyin.bizIm import live_pb2 as live
from dy_live.protobuf.douyin.bizIm.webcast import data_pb2 as data
from dy_live.protobuf.douyin.bizIm.webcast import im_pb2 as im
from dy_live.protobuf.douyin.transport.webcast import im_pb2 as transport
from dy_live.message_types import *

from dy_live.pk import PK_MESSAGES, PKMessageHandler, print_pk_event
from dy_apis.douyin_api import LIVE_HOST, DouyinAPI, check_risk_response
from builder.auth import DouyinAuth
from builder.header import HeaderBuilder, HeaderType
from builder.params import Params
from utils import common_util
from utils.dy_util import generate_signature

from dy_live.utils import *
from dy_live.user_db import UserDB, gatherUsers
from utils.fingerprint import get_profile
from utils.http_client import _Response

ZWSP = '\u200b'  # Zero Width Space


# Windows 控制台是 GBK，弹幕含 emoji 会 UnicodeEncodeError，按 UTF-8 输出
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def raise_for_status_code(r: _Response, error=''):
    r.raise_for_status()
    if 'application/json' not in r.headers.get('Content-Type', ''):
        return
    data = r.json()
    if data['status_code'] != 0:
        raise Exception(error or f"Non-zero status_code={data['status_code']}. response = {r.text}")
    return data


class DouyinLive:
    def __init__(
            self,
            live_id: str,
            auth: DouyinAuth,
            on_pk_event=None,
            record=False,
            record_unknown=False,
            dump_json=False,
            keep_uncompressed=False,
            print_pk_events=False,
            **kwargs,
    ):
        """
        on_pk_event: 收到 PKMESSAGES 列表中的消息之一时的回调函数
        record: 将消息的简要内容写到 live.<live_id>.<YYYY-MM-DD_HHMMSS>.log
        record_unknown: 未知消息转为JSON格式写到 live.*.log
        dump_json: 将消息转为JSON格式写到 live.*.json
        keep_uncompressed: 压缩完成后保留原始文件
        print_pk_events: 命令行打印 PK 相关消息
        """
        self.auth = auth
        self.live_id = live_id
        self.ws: websocket.WebSocketApp = None
        # Optional structured PK event consumer
        self.on_pk_event = on_pk_event or (lambda _: True)
        self.pk_handler = PKMessageHandler()

        # lock on auth.request()
        self.request_lock = threading.Lock()

        self.douyin_proto = {
            "im": importlib.import_module("dy_live.protobuf.douyin.bizIm.webcast.im_pb2"),
            "data": importlib.import_module("dy_live.protobuf.douyin.bizIm.webcast.data_pb2"),
            "transport": importlib.import_module("dy_live.protobuf.douyin.transport.webcast.im_pb2"),
            "live": importlib.import_module("dy_live.protobuf.douyin.bizIm.live_pb2"),
        }
        self._log_txt = record
        self._log_unknown = record_unknown
        self._log_json = dump_json
        self._keep_uncompressed = keep_uncompressed
        self._print_pk_events = print_pk_events
        self.__dict__.update({'_' + k: v for k, v in kwargs.items()})

        self.live_room_id = ''
        self.user_db: UserDB = None
        self.cond_stopped = threading.Condition()

        self.muted_messages: list[str] = []
        names = [l.split('#')[0] for l in self.read_file_if_exist('dy_live/muted_messages').splitlines()]
        self.muted_messages = [n.strip().removeprefix('Webcast') for n in names if len(n) > 0]

        # message name -> function
        self.message_readers: dict[str, Callable[[Message], bool]] = {
            n.removeprefix('on_'): getattr(self, n) for n in dir(self)
            if n.startswith('on_') and n[3].isupper() and callable(getattr(self, n))
        }
        self.executor: ThreadPoolExecutor = None
        self.executor_mt: ThreadPoolExecutor = None

        self._fast_reconnect = False

    def queue(self, fn, /, *args, **kwargs):
        if self.executor:  # None if replay()
            fn = print_exception(name_thread(fn, 'queue-'))
            return self.executor.submit(fn, *args, **kwargs)

    def schedule(self, fn, /, *args, **kwargs):
        if self.executor_mt:  # None if replay()
            fn = print_exception(name_thread(fn, 'pool-'))
            return self.executor_mt.submit(fn, *args, **kwargs)

    def schedule_after(self, delay, fn, /, *args, **kwargs):
        def delayed():
            nonlocal fn
            if self.wait_until_stop(delay):
                return
            fn = print_exception(name_thread(fn, 'pool-'))
            return fn(*args, **kwargs)
        return self.executor_mt.submit(name_thread(delayed, f'pool-{fn.__name__}-'))

    def _init_or_shutdown_thread_pools(self, shutdown=False, init=True):
        if shutdown:
            self.executor.shutdown(wait=False, cancel_futures=True)
            self.executor_mt.shutdown(wait=False, cancel_futures=True)
        if init:
            self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='queue')
            self.executor_mt = ThreadPoolExecutor(max_workers=4, thread_name_prefix='pool')

    def _init_live(self, room_info, live_info):
        if self.live_room_id:
            return

        self._init_or_shutdown_thread_pools()

        self.live_room_id = room_info['room_id']
        self.live_name = f"live.{self.live_id}.{self.live_room_id}"
        self.live_start = datetime.now()
        self.live_finished = False
        if self._log_json:
            self.json_log_file = open(self.live_name + '.json', mode='at', encoding='utf8', buffering=1)
        if self._log_txt:
            self.txt_log_file = open(self.live_name + '.log', mode='at', encoding='utf8', buffering=1)

        self.user_db = UserDB(self.live_name + '.user')

        ts = self.live_start.strftime('%Y%m%d%H%M%S')
        self.request_id = ts + hashlib.sha1(ts.encode()).hexdigest()[:20].upper()

        # biz specific stuff

        anchor = live_info['roomStore']['roomInfo']['anchor']
        self.live_anchor_id = int(anchor['id_str'] or anchor['id'])
        self.user_db.updateFromDict(anchor)

        # user info of myself
        me = raise_for_status_code(self.request(
            'GET', '/webcast/user/me/',
            endpoint_params={'room_id': 0},
        ))['data']
        self.my_user_id: int = me['id']
        self.my_user_unique_id: int = live_info['userStore']['odin']['user_unique_id']
        self.user_db.updateFromDict(me)

        self.__joined_lotteries = []
        self.__joined_luckyboxes = []

    def _archive_live_files(self, prefix, new_prefix='', async_=False):
        '''
        Archive files:
            live.<live_id>.<room_id>.log
            live.<live_id>.<room_id>.json
            live.<live_id>.<room_id>.user/
        Into:
            live.<live_id>.<live_start_time>.tar.zstd
        '''
        if not new_prefix:
            with open(f'{prefix}.log', mode='rt', encoding='utf8') as f:
                first_timestamp = f.readline()
            first_timestamp = parse_readable_time(first_timestamp)
            first_timestamp = datetime.fromtimestamp(first_timestamp).strftime("%Y%m%d_%H%M%S")
            new_prefix = f'live.{self.live_id}.{first_timestamp}'

        def archive_live_files():
            files = []
            for suffix in ['log', 'json', 'user']:
                name, new_name = f'{prefix}.{suffix}', f'{new_prefix}.{suffix}'
                if path.exists(name):
                    shutil.move(name, new_name)
                    files.append(new_name)

            logger.info(f"正在归档文件 {', '.join(files)}")
            archive_compress(f'{new_prefix}.tar.zstd', files, level=11)

            if not self._keep_uncompressed:
                if path.exists(f'{new_prefix}.json'):
                    os.remove(f'{new_prefix}.json')
                if path.exists(f'{new_prefix}.user'):
                    shutil.rmtree(f'{new_prefix}.user')
            logger.info(f"    归档完成 {new_prefix}.tar.zstd")

        if async_:
            threading.Thread(target=archive_live_files).start()
        else:
            archive_live_files()

    def _finish_live(self):
        if self.live_room_id is None:
            return

        self.user_db = None  # close database before archiving it

        if self._log_json:
            self.json_log_file.close()
        if self._log_txt:
            self.txt_log_file.close()

        if self.live_finished:
            self._archive_live_files(
                async_=True,
                prefix=self.live_name,
                new_prefix=f'live.{self.live_id}.{self.live_start.strftime("%Y%m%d_%H%M%S")}'
            )

        self._init_or_shutdown_thread_pools(shutdown=True, init=False)

        self.live_name = None
        self.live_room_id = None

    def read_file_if_exist(self, path_) -> str:
        path = Path(path_)
        if path.is_file():
            logger.debug(f"读取文件 '{path_}'")
            return path.read_text(encoding="utf-8")
        return ''

    def log_txt(self, *values: object, sep: str | None = " ", end: str | None = "\n"):
        if self._log_txt:
            print(*values, sep=sep, end=end, file=self.txt_log_file)

    def log_json(self, *values: object, sep: str | None = " ", end: str | None = "\n"):
        if self._log_json:
            print(*values, sep=sep, end=end, file=self.json_log_file)

    def nickname_or_id(self, id_: int | str) -> str:
        u = self.user_db[int(id_)]
        return u.nickname if u else f'ID:{str(id_)}'

    def notify_stop(self):
        with self.cond_stopped:
            self.cond_stopped.notify_all()

    def wait_until_stop(self, timeout=None):
        with self.cond_stopped:
            return self.cond_stopped.wait(timeout)

    def ping(self, ws: websocket.WebSocket):
        while True:
            if self.wait_until_stop(timeout=60):
                break
            try:
                frame = transport.PushFrame(payload_type="hb")
                ws.send(frame.SerializeToString(), websocket.ABNF.OPCODE_PING)
            except Exception as e:
                logger.warning(f"心跳包错误: {str(e)}")
                ws.close()
                break
        logger.debug("结束心跳线程")

    def on_open(self, ws: websocket.WebSocket):
        logger.debug("WebSocket 已连接")
        # A reconnect may have missed an entire battle or a channel change.
        self.pk_handler = PKMessageHandler()
        self.auth.live_websocket_connected = True
        self.schedule(self.ping, ws)
        # biz stuff
        self.queue(self.check_and_join_lottery)
        self.queue(self.check_and_join_luckyboxes)
        self.do_fansclub_tasks_future = self.schedule(self.do_fansclub_tasks)

    def on_error(self, ws, error):
        if isinstance(error, KeyboardInterrupt):
            return
        logger.error(f"WebSocket error: {type(error)}: {error}")
        logger.error(traceback.format_exc())
        # we don't call `self.ws.run_forever(reconnect=1)` because `on_open()` will be
        # called multiple times on each reconnect
        self._fast_reconnect = True

    def on_close(self, ws, close_status_code, close_msg):
        self.notify_stop()
        logger.debug(f"WebSocket closed. status_code: {close_status_code}, close_msg: {close_msg}")
        if sys.exc_info()[0] is KeyboardInterrupt:
            logger.info("要退出程序，请再次按下 Ctrl+C")

    def on_message(self, ws: websocket.WebSocket, data):

        try:
            frame = transport.PushFrame.FromString(data)
            if frame.payload_type in ('hb', 'ack') or not frame.payload:
                return
            headers = {h.key: h.value for h in frame.headers}
            payload = frame.payload
            if 'gzip' == headers.get('compress_type'):
                payload = gzip.decompress(payload)
            response = transport.Response.FromString(payload)
        except Exception as e:
            logger.error(traceback.format_exc())
            return

        # A successful protobuf parse proves the websocket payload
        # path, whereas on_open alone only proves the handshake.
        self.auth.live_websocket_verified = True

        if response.need_ack:
            ack = transport.PushFrame(
                LogID=frame.LogID,
                payload_type='ack',
                # payload = frame.headers[1].value.encode('utf-8'),
                payload=response.internal_ext.encode('utf-8')
            )
            logger.trace(f"Sending ack message with LogID={frame.LogID}")
            ws.send(ack.SerializeToString(), websocket.ABNF.OPCODE_BINARY)

        # print time of the messages
        if len(response.messages) > 0:
            self.log_json(format_readable_time(response.now, True))
            msgs = [m for m in response.messages if m.method.removeprefix('Webcast') not in self.muted_messages]
            if len(msgs) > 0:
                self.log_txt(format_readable_time(response.now, True))

        for msg in response.messages:
            method: str = msg.method.removeprefix('Webcast')
            # Dump the message in JSON
            try:
                json_ = self._try_dump_json(method, msg.payload)
                self.log_json(json_)
            except BaseException:
                json_ = message_to_json(msg)
                self.log_json(json_)
                continue
            # Actually handle it
            try:
                m = self._parse_from_string(method, msg.payload)
                self._gather_users(method, m)
                if method not in self.muted_messages:
                    r = self.message_readers.get(method, self.on_unknown_message)(m)
                    # handler returned False explicitly
                    if (r == False) and self._log_unknown:
                        self.log_txt(json_)

                if method in PK_MESSAGES:
                    try:
                        event = self.pk_handler.handle(method, m, msg.msg_id)
                        if event:
                            if self._print_pk_events:
                                print_pk_event(event)
                            self.on_pk_event(event)
                    except Exception as e:
                        logger.error(f'PK 消息处理失败：method={method} msg_id={msg.msg_id}: {e}')
            except Exception as e:
                if method not in self.muted_messages:
                    logger.error(traceback.format_exc())

    def start_ws(self, room_info=None):
        """
        连接直播间消息接口，并处理接收到的消息
        :param room_info: `DouyinAPI.get_live_info` 的返回结果
        :return: 直播间ID, 用户ID, ttwid
        """
        if room_info is None:
            room_info, _ = DouyinAPI.get_live_info(self.auth, self.live_id)

        room_id = str(room_info['room_id'])
        user_id = str(room_info['user_id'])
        ttwid = room_info['ttwid']

        res = DouyinAPI.get_webcast_detail(self.auth, user_id, room_id, f"https://{LIVE_HOST}/{self.live_id}")
        response = transport.Response.FromString(res)
        params = Params({
            'app_name': 'douyin_web',
            'version_code': '180800',
            'webcast_sdk_version': '1.0.15',
            'update_version_code': '1.0.15',
            'compress': 'gzip',
            'device_platform': 'web',
            'cookie_enabled': 'true',
            'screen_width': '1707',
            'screen_height': '960',
            'browser_language': 'zh-CN',
            'browser_platform': 'Win32',
            'browser_name': 'Mozilla',
            'browser_version': HeaderBuilder.ua.split('Mozilla/')[-1],
            'browser_online': 'true',
            'tz_name': 'Etc/GMT-8',
            'cursor': str(response.cursor),
            'internal_ext': response.internal_ext,
            'host': 'https://live.douyin.com',
            'aid': '6383',
            'live_id': '1',
            'did_rule': '3',
            'endpoint': 'live_pc',
            'support_wrds': '1',
            'user_unique_id': user_id,
            'im_path': '/webcast/im/fetch/',
            'identity': 'audience',
            'need_persist_msg_count': '15',
            'insert_task_id': '',
            'live_reason': '',
            'room_id': room_id,
            'heartbeatDuration': '0',
            'signature': generate_signature(room_id, user_id),
        })
        self.ws = websocket.WebSocketApp(
            url=f"wss://webcast100-ws-web-hl.douyin.com/webcast/im/push/v2/?{urlencode(params.get())}",
            header={
                'Pragma': 'no-cache',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                'User-Agent': HeaderBuilder.ua,
                'Upgrade': 'websocket',
                'Cache-Control': 'no-cache',
                'Connection': 'Upgrade',
            },
            cookie=self.auth.cookie_str,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        # for try_ in range(100) :
        #     try:
        #         _ = DouyinAPI.get_live_room_enter(self.auth, self.live_id)
        #         break
        #     except Exception as e:
        #         if '接口返回空响应' in str(e):
        #             logger.debug(str(e))
        #             time.sleep(1)
        #         else:
        #             raise
        try:
            self.ws.run_forever(origin=f'https://{LIVE_HOST}')
        except Exception as e:
            logger.error(traceback.format_exc())
            self.ws.close()

    def run_forever(self, poll_interval=10):

        def run(room_info, data):
            try:
                self._init_live(room_info, data)
                now = format_readable_time(time.time())
                self.log_json(now)
                self.log_json(json.dumps(data, ensure_ascii=False))

                if data:
                    room = data["roomStore"]["roomInfo"]["room"]
                    display_text = '{} 正在直播：{} │ {}'.format(
                        room["owner"]["nickname"] + ZWSP,
                        room["title"],
                        room["room_view_stats"]["display_long_anchor"],
                    )
                    self.user_db.updateFromDict(room["owner"])
                else:
                    display_text = '{} 正在直播：{}'.format(
                        room_info["anchor_id"], room_info["room_title"]
                    )
                self.log_txt(now)
                self.log_txt(f'【直播间】{display_text}')
                logger.info(display_text)  # also to CLI

                self.start_ws(room_info)
            finally:
                self._finish_live()

        while True:
            try:
                room_info, data = DouyinAPI.get_live_info(self.auth, self.live_id)
                room_status = room_info.get('room_status')
                room_id = room_info.get('room_id')
                if room_status == 2:
                    run(room_info, data)
                elif room_status == 4:
                    logger.info('未开播或直播已结束')
                    prefix = f'live.{self.live_id}.{room_id}'
                    if room_id and path.exists(f'{prefix}.log') and path.exists(f'{prefix}.user'):
                        self._archive_live_files(async_=False, prefix=prefix)
                else:
                    logger.info(f'直播间状态未知 status={room_status}')

                if not self._fast_reconnect:
                    time.sleep(poll_interval)
                self._fast_reconnect = False
            except KeyboardInterrupt:
                return
            except BaseException:
                logger.error(traceback.format_exc())

    def replay(self, prefix):

        self._log_json = False
        self._log_txt = True
        self.txt_log_file = sys.stdout

        json_log_file, user_db_path = f'{prefix}.json', f'{prefix}.user'

        if path.exists(f'{prefix}.tar.zstd'):
            os.makedirs('replay_tmp', exist_ok=True)
            if os.stat(f'{prefix}.tar.zstd').st_size > 10 * 1024 * 1024:
                logger.info(f"Decompressing archive '{prefix}.tar.zstd'")
            archive_decompress(f'{prefix}.tar.zstd', 'replay_tmp')
            basename = path.basename(prefix)
            json_log_file, user_db_path = f'replay_tmp/{basename}.json', f'replay_tmp/{basename}.user'

        if not (path.exists(json_log_file) and path.exists(user_db_path)):
            raise Exception(
                f"Both JSON file '{json_log_file}' and user database '{user_db_path}' directory should exist.")

        self.user_db = UserDB(user_db_path)
        f = open(json_log_file, 'rt', encoding='utf8')

        try:
            with contextlib.suppress(KeyboardInterrupt):
                self._replay(f)
        finally:
            self.user_db = None
            f.close()
            if path.exists('replay_tmp'):
                shutil.rmtree('replay_tmp')

    def _replay(self, json_log_file: TextIOWrapper):
        import base64

        while True:
            line = json_log_file.readline()  # Returns an empty string if EOF is hit immediately.
            if len(line) == 0:
                break

            if line.startswith('{'):  # a message JSON
                json_ = line
                o = json.loads(json_)
                if ('common' not in o):
                    if ('payload' in o) and self._log_unknown:
                        self.log_txt(f"Unknown message. Decoding the payload as raw UTF-8 string:\n" +
                                     base64.b64decode(o['payload']).decode('utf8', errors='replace'))
                    continue

                method: str = o['common']['method'].removeprefix('Webcast')
                class_ = self._try_get_class(im, method)

                if method not in self.muted_messages:
                    m = json_format.ParseDict(o, class_())
                    r = self.message_readers.get(method, self.on_unknown_message)(m)
                    if (r == False) and self._log_unknown:
                        self.log_txt(json_)

            elif line[:4].isdigit():  # time stamp
                self.log_txt(line.rstrip())

    def _try_get_class(self, module, method: str):
        known_relation = {
            # 0.0.5
            # "LinkMicArmiesMethod": "LinkMicArmies",
            # "LinkMicBattleFinishMethod": "LinkMicBattleFinish",
            # "LinkMicBattleMethod": "LinkMicBattle",
            # "LinkMicBattlePunishMethod": "LinkMicBattlePunish",
            # "RoomNotifyMessage": "NotifyMessage",
            # 2026.4.23-beta.12
            "LinkMicArmiesMethod": "LinkMicArmies",
            "LinkMicBattleFinishMethod": "LinkMicBattleFinish",
            "LinkMicBattleMethod": "LinkMicBattle",
            "LinkMicBattlePunishMethod": "LinkMicBattlePunish",
            "RoomNotifyMessage": "NotifyMessage",
            #
            "DecorationModifyMethod": "DecorationModifyMessage",
        }
        if hasattr(module, method):
            pass
        elif method in known_relation and hasattr(module, known_relation[method]):
            method = known_relation[method]
        else:
            return None
        return getattr(module, method)

    def _try_dump_json(self, method: str, payload: bytes):
        tb_list = []
        for module in self.douyin_proto.values():
            class_ = self._try_get_class(module, method)
            if class_:
                try:
                    m = class_.FromString(payload)
                    return message_to_json(m)
                except BaseException:
                    tb_list.append(traceback.format_exc())
        tb_hint = ''
        if tb_list:
            tb_hint = ' Exceptions while trying to dump the Message:\n' + '------------\n'.join(tb_list)
        raise Exception(f"Unknown method '{method}'.{tb_hint}")

    def _gather_users(self, method: str, m: Message):
        gatherUsers(self.user_db, method, m)
        if time.time() - getattr(self, '_last_gather_users_report', 0) > 5 * 60:
            self._last_gather_users_report = time.time()
            logger.debug(f'Gathered {self.user_db.count()} user objects.')

    def _parse_from_string(self, method: str, data: bytes, module=im) -> Message:
        class_ = self._try_get_class(module, method)
        if class_:
            return class_.FromString(data)
        else:
            raise Exception(f"Unknown method '{method}'. Decoding data as raw UTF8 string:\n" +
                            data.decode('utf8', errors='replace'))

    def request(
            self,
            method: str, api: str, endpoint_params: dict[str, str] = {},
            enter_from: str = 'web_homepage_follow',
            with_verify_fp=False,
            with_bd_ticket=False,
            with_csrf: bool | str = False,
            **kwargs
    ) -> _Response:
        """直播间请求。共用登录会话、部分参数。
        Args:
          Webcast API Args
            method：         HTTP 请求方式
            endpoint_params: 该 API 独有的参数
            enter_from:      取值 'web_homepage_hot' | 'web_homepage_follow' | 'link_share'
            with_verify_fp:  加 verifyFp/fp URL参数
            with_bd_ticket:  加 bd-ticket-guard-* 请求头
            with_csrf:       加 x-secsdk-csrf-token 请求头。取值 'DOWNGRADE' | True | False
          General Args
            kwargs:          透传给 curl_cffi.request 函数
        """
        if not getattr(self, 'live_room_id', None):
            raise Exception("Not in live")

        headers = HeaderBuilder().build(HeaderType(method).value)
        headers.set_header("Origin", DouyinAPI.live_url)

        if with_bd_ticket:
            headers.with_bd(api, auth, origin=DouyinAPI.live_url)

        if with_csrf == 'DOWNGRADE':
            headers.set_header('x-secsdk-csrf-token', with_csrf)
        elif with_csrf and isinstance(with_csrf, bool):
            headers.with_csrf(auth.cookie_str)

        headers.set_referer(f'{DouyinAPI.live_url}/{self.live_room_id}')

        params = Params({
            'aid': '6383',
            'app_name': 'douyin_web',
            'live_id': '1',
            'device_platform': 'web',
            'language': 'zh-CN',
            'enter_from': enter_from,
            'cookie_enabled': 'true',
            'screen_width': get_profile()['screen_width'],
            'screen_height': get_profile()['screen_height'],
            'browser_language': 'zh-CN',
            'browser_platform': get_profile()['platform'],
            'browser_name': get_profile()['browser_name'],
            'browser_version': get_profile()['browser_version'],
            'os_name': 'Windows',
            'os_version': '10',
        })
        params.update_params({
            k: v for k, v in endpoint_params.items() if v is not None
        })
        params.add_param('msToken', auth.msToken)
        params.with_a_bogus(host=LIVE_HOST)
        if with_verify_fp:
            params.with_verify_fp(self.auth)

        url = f'{DouyinAPI.live_url}{api}'
        request_kwargs = {
            'headers': headers.get(),
            'params': params.get(),
            'verify': False,
        }
        request_kwargs.update(kwargs)
        request_kwargs.setdefault('timeout', 30)  # after `.update(kwargs)`

        with self.request_lock:
            resp = auth.request(method, url, **request_kwargs)
        check_risk_response(resp)
        if self._dump_http:
            excessive_body_hint = f'...({len(resp.content)} bytes of body)' if len(resp.content) > 512 else ''
            logger.debug(
                'HTTP dump:'
                + f'\n>> {method} ' + resp.request.url
                + f'\n>> ' + '\n>> '.join(f'{k.title()}: {v}' for k, v in resp.request.headers.items())
                + f'\n>>'
                + f'\n>> ' + (resp.request.body or b'').decode()
                + f'\n* ({resp.request_size} bytes)'
                + f'\n<< HTTP/{resp.http_version} {resp.status_code} {resp.reason}'
                + f'\n<< ' + '\n<< '.join(f'{k.title()}: {v}' for k, v in resp.headers.items())
                + f'\n<<'
                + f'\n<< ' + resp.text[:512] + excessive_body_hint
                + f'\n* ({resp.response_size} bytes)'
            )
        return resp

    def send_gift(self, gift_id: int, extra='', tags_extra=''):
        extra = extra or '{"total_count":1}'
        tags_extra = tags_extra or json_dumps_compact({
            "gift_source": "gift_panel",
            "enter_from_merge": "web_homepage_follow",
            "enter_method": "web_live_cell",
            "is_client": False
        })
        webid = self.auth.webid
        device_id = webid
        data = {
            "buff_level": 0,
            # "combo_position": "gift_panel",
            "count": 1,
            "extra": extra,
            "gift_id": gift_id,
            "gift_source": 0,
            # "is_first_combo": True,
            # "is_hotkey": 0,
            "room_anchor_id": self.live_anchor_id,
            "room_id": self.live_room_id,
            "send_scene": 1,
            "send_type": 4,
            "tags_extra": tags_extra,
            "to_episode_id": 0,
            "to_room_id": self.live_room_id,
            "to_user_id": self.live_anchor_id,
            "to_user_ids": "",
        }
        return raise_for_status_code(self.request(
            'POST', '/webcast/gift/send/',
            endpoint_params={
                'browser_online': 'true',
                'engine_name': 'Blink',
                'engine_version': get_profile()['engine_version'],
                'cpu_core_num': get_profile()['cpu_core_num'],
                'device_memory': get_profile()['device_memory'],
                'platform': 'PC',
                'downlink': '10',
                'effective_type': '4g',
                'round_trip_time': '50',
                'channel': 'channel_pc_web',
                'webid': webid,
                'user_agent': get_profile()['ua'],
                # 'fp': '',
                'did': '0',
                'referer': '',
                'target': '',
                'device_id': device_id,
            },
            with_verify_fp=True,
            with_bd_ticket=True,
            with_csrf='DOWNGRADE',
            data=data,
        ))

    def do_fansclub_tasks(self):
        logger.info('检查粉丝团任务')

        def request_fansclub_homepage():
            return self.request(
                'GET', '/webcast/fansclub/homepage/',
                endpoint_params={
                    'anchor_id': self.live_anchor_id,
                    'request_scene': '1',
                    'action': '1',
                    'anchor_open_id': '',
                    'source': '6',
                },
                with_verify_fp=True,
            ).json()

        data = request_fansclub_homepage()
        while data and data['status_code'] == 0:
            data = data['data']
            recheck_after = 0
            for t in data['user_task']['items']:
                if t['task_type'] == 1 and t['status'] != 3:  # 送灯牌
                    logger.info(t['title'])
                    self.send_gift(
                        t['button']['gift_id'],
                        extra=json_dumps_compact({
                            "growth_task": {"send_source": "fansClub"},
                            "task_type": 1,
                            "release": "1.0.0.1545",
                            "fansclub_send_gift": data['extra']['fansclub_send_gift'],
                            "total_count": 1
                        }),
                        tags_extra=json_dumps_compact({
                            "gift_source": "fansClub",
                            "enter_from_merge": "web_homepage_follow",
                            "enter_method": "web_live_cell",
                            "is_client": False
                        }),
                    )
                elif t['task_type'] == 2 and t['status'] != 3:  # 观看直播
                    logger.info(
                        t['title']
                        + f" │ 进度 {t['completed_num']}/{t['total_num']}"
                        + f" │ {t['button']['content']}"
                    )
                    recheck_after = 3 * 60
                elif t['task_type'] == 11 and t['status'] != 3:  # 点亮粉丝团恢复勋章
                    logger.info(t['title'])
                    self.send_gift(
                        t['button']['gift_id'],
                        extra=json_dumps_compact({
                            "release": "1.0.0.1545",
                            "task_type": 11,
                            "fansclub_source": 2,
                            "total_count": 1
                        }),
                        tags_extra=json_dumps_compact({
                            "gift_source": "fansClub",
                            "enter_from_merge": "web_homepage_follow",
                            "enter_method": "web_live_cell",
                            "is_client": False
                        }),
                    )
                    time.sleep(1.3)
                    data = request_fansclub_homepage()
                    continue

            user_info = data['user_info']
            logger.info(
                f"粉丝团亲密度：差{user_info['next_level_intimacy'] - user_info['intimacy']}"
                f"({user_info['intimacy']}/{user_info['next_level_intimacy']})亲密度升级至LV.{user_info['level'] + 1}")

            data = None  # no more tasks
            if recheck_after:
                self.schedule_after(recheck_after, self.do_fansclub_tasks)
                logger.info(f'{recheck_after//60}分钟后再检查一次粉丝团任务')
            else:
                now = int(time.time())
                tomorrow = datetime.fromtimestamp(now) + timedelta(days=1)
                tomorrow = tomorrow.replace(hour=0, minute=0, second=1, microsecond=0).timestamp()  # as type float
                self.schedule_after(tomorrow - now, self.do_fansclub_tasks)
                logger.info(f'粉丝团任务完成，明天（{format_readable_time(tomorrow)}）再次检查')

    def check_and_join_luckyboxes(self):
        me = self.user_db[self.my_user_id]
        anchor = self.user_db[self.live_anchor_id]
        data = raise_for_status_code(self.request(
            'GET', '/webcast/luckybox/box/list/',
            endpoint_params={
                'room_id': self.live_room_id,
                'user_id': me.id,
                'sec_user_id': me.sec_uid,
                'anchor_id': self.live_anchor_id,
                'anchor_id_str': self.live_anchor_id,
                'sec_anchor_id': anchor.sec_uid,
                'enter_from_merge': 'web_homepage_follow',
                'video_id': self.live_room_id,
                'is_ad': '0',
                'request_id': self.request_id,
                'enter_method': 'web_live_cell',
                'user_unique_id': self.my_user_unique_id,
                'user_agent': get_profile()['ua'],
                'action_type': 'click',
                'pc_client_type': '1',
                'pc_client_version': '',
            },
        ))
        if not data.get('data', None):  # data['data'] can be empty object/array
            logger.info('无红包')
            return
        for box in data['data']:
            self.queue(self.join_luckybox, box['box_id'])

    def check_and_join_lottery(self):
        data = raise_for_status_code(self.request(
            'GET', '/webcast/lottery/melon/lottery_info/',
            endpoint_params={
                'room_id': self.live_room_id,
                'query_from': 1,
            },
            with_verify_fp=True,
        ))
        if not data.get('data', None):
            logger.info('无福袋')
            return
        self.queue(self.join_lottery, data['data']['lottery_info']['lottery_id'])

    def join_luckybox(self, box_id: int):
        if box_id in self.__joined_luckyboxes:
            return
        me = self.user_db[self.my_user_id]
        anchor = self.user_db[self.live_anchor_id]
        raise_for_status_code(self.request(
            'POST', '/webcast/luckybox/join/',
            endpoint_params={
                'box_id': box_id,
                'room_id': self.live_room_id,
                'anchor_id': self.live_anchor_id,
                'user_id': self.my_user_id,
                'sec_user_id': me.sec_uid,
                'anchor_id_str': self.live_anchor_id,
                'sec_anchor_id': anchor.sec_uid,
                'enter_from_merge': 'web_homepage_follow',
                'is_ad': '0',
                'request_id': self.request_id,
                'enter_method': 'web_live_head',
                'user_unique_id': self.my_user_unique_id,
                'user_agent': get_profile()['ua'],
                'action_type': 'click',
                'pc_client_type': '1',
                'pc_client_version': '',
                'version_code': '170400',
                'update_version_code': '170400',
                'version_name': '17.4.0',
                'platform': 'PC',
            },
            enter_from='web_homepage_follow',
            with_csrf=True,
        ))

        logger.info(f'Joined lucky box {box_id}')
        self.__joined_luckyboxes.append(box_id)

    def join_lottery(self, lottery_id, conditions: Message | object = None):
        if lottery_id in self.__joined_lotteries:
            return

        if conditions is None:
            resp = self.request(
                'GET', '/webcast/lottery/melon/check_user_right/',
                endpoint_params={
                    'lottery_id': lottery_id,
                    'room_id': self.live_room_id,
                    'anchor_id': self.live_anchor_id,
                },
                with_verify_fp=True,
            )
            raise_for_status_code(resp)
            conditions = json_loads_object(resp.content).data.lottery_info.conditions

        for c in conditions:
            if c.type == LotteryCondition.默认参与.value:
                logger.debug(f"Lottery {lottery_id}. Update user condition '{LotteryCondition(c.type).name}'")
                raise_for_status_code(self.request(
                    'POST', '/webcast/lottery/melon/update_user_condition/',
                    enter_from='web_homepage_follow',
                    with_verify_fp=True,
                    with_csrf='DOWNGRADE',
                    data={'condition_type': c.type, 'lottery_id': lottery_id},
                ))
            elif c.type == LotteryCondition.发送评论.value:
                logger.debug(f"Lottery {lottery_id}. Trying to send special comment '{c.content}'")
                raise_for_status_code(self.request(
                    'GET', f'/webcast/room/special_chat/{self.live_room_id}',
                    endpoint_params={
                        'type': '9',
                        'room_id': self.live_room_id,
                        'content': c.content,
                    },
                    enter_from='web_homepage_follow',
                    with_verify_fp=True,
                ))
            elif c.type == LotteryCondition.点亮粉丝团.value:
                description = getattr(c, 'description', LotteryCondition(c.type).name)
                logger.debug(f"Lottery {lottery_id}. Checking condition '{description}' handled by async task.")
                self.do_fansclub_tasks_future.result()

        logger.info(f'Joined lottery {lottery_id}')
        self.__joined_lotteries.append(lottery_id)

    def on_unknown_message(self, _):
        return False

    def on_ChatMessage(self, m: Message):
        """聊天消息"""
        u = m.user
        # ZWSP 是无宽度空格，避免用户昵称中的特殊字符导致后续文字显示错乱。
        self.log_txt(f"【聊天】({str_public_area_badges(u)}) {u.nickname}{ZWSP}：{m.content}")

    def on_GiftMessage(self, m: Message):
        """礼物消息"""
        u = m.user
        repeat_end_hint = '（连击结束）' if (m.repeat_end and m.combo_count > 1) else ''
        self.log_txt(
            f"【礼物】({str_public_area_badges(u)}) {u.nickname}{ZWSP} "
            f"送出了 {m.gift.name}({m.gift.diamond_count}) ×{m.combo_count}{repeat_end_hint}")

    def on_BindingGiftMessage(self, m: Message):
        """礼物消息"""
        self.log_txt(f"【礼物】{m.common.describe}")

    def on_LikeMessage(self, m: Message):
        """点赞消息"""
        self.log_txt(f"【点赞】{m.user.nickname}{ZWSP} 点了{m.count}个赞。点赞总数{m.total}。")

    def on_MemberMessage(self, m: Message):
        """成员消息，比如用户进入直播间"""
        class Action(Enum):
            ENTER = 1
            LEAVE = 2
            SET_SILENCE = 3
            CANCEL_SILENCE = 4
            SET_ADMIN = 5
            CANCEL_ADMIN = 6
            KICK_OUT = 7
            SHARE = 8
            MANAGER_SET_SILENCE = 9
            MANAGER_CANCEL_SILENCE = 10
            BLOCK = 11
            FOLLOW = 20

        u = m.user
        gender = {1: '男', 2: '女'}.get(u.gender, 'X')
        # user_profile_str = f"[{gender}]" + \
        #     f"({u.pay_grade.level},{u.fans_club.data.level}," + \
        #     f"{u.follow_info.following_count},{u.follow_info.follower_count})"

        if m.action == Action.ENTER.value:
            # “来了”、“通过 分享 来了”、“通过用户推荐来了”
            self.log_txt(f"【观众】{gender} {render_text(m.common.display_text)}")
        elif m.action == Action.LEAVE.value:
            self.log_txt(f"【观众离开】{render_text(m.common.display_text)}")
        else:
            self.log_txt(f"【观众 action={m.action}】{render_text(m.common.display_text)} JSON={message_to_json(m)}")

    def on_SocialMessage(self, m: Message):
        """社交消息。关注、分享等"""
        follow_count_hint = f'。主播当前粉丝 {m.follow_count}' if m.action == 1 else ''
        self.log_txt(f"【社交】{render_text(m.common.display_text)}{follow_count_hint}")

    def on_RoomUserSeqMessage(self, m: Message):
        """直播间统计"""
        self.log_txt(
            f"【统计】在线 {m.total_str}({m.total})，场观 {m.total_pv_for_anchor}({m.total_user})")

    def on_FansclubMessage(self, m: Message):
        """粉丝团消息"""
        if m.content:
            self.log_txt(f"【粉丝团】{m.content}")

    def on_EmojiChatMessage(self, m: Message):
        """聊天表情包消息"""
        self.log_txt(f"【表情包】 {m.user.nickname}{ZWSP}: {render_text(m.emoji_content)}")
        self.schedule(download_file, './', m.emoji_content.pieces[0].image_value.image.url_list[0])

    def on_ExhibitionChatMessage(self, m: Message):
        """冠名展馆"""
        self.log_txt(f"【展馆】 {render_text(m.display_text)}")

    def on_RoomStatsMessage(self, m: Message):
        """直播间统计信息"""
        self.log_txt(f"【直播间统计】在线观众 {m.display_middle}({m.total})")

    def on_RoomRankMessage(self, m: Message):
        """直播间排行榜信息"""
        ranks = [f'{r.user.nickname}{ZWSP}({r.score})' for r in m.audience_ranks]
        self.log_txt(f"【直播间排行榜】{' │ ' .join(ranks)}")

    def on_RoomIntroMessage(self, m: Message):
        """直播间介绍"""
        self.log_txt(f"【直播间介绍】{m.intro}")

    def on_ControlMessage(self, m: Message):
        """直播间状态控制消息"""
        class Action(Enum):
            PAUSE = 1
            RESUME = 2
            FINISH = 3
            FINISH_BY_ADMIN = 4
            CHANGE_NODE = 5
            ROOM_FINISH_BY_SWITCH = 6
            PING_TIMEOUT = 7

        if m.action in [Action.PAUSE.value, Action.RESUME.value]:  # 主播暂时离开、主播回来
            self.log_txt(f'{render_text(m.common.display_text)}')
        elif m.action in [Action.FINISH.value, Action.FINISH_BY_ADMIN.value, Action.ROOM_FINISH_BY_SWITCH.value]:
            tips = m.tips or '直播已结束'
            self.log_txt(tips)
            logger.info(tips)
            self.live_finished = True
            if self.ws:  # in replay() mode, `self.ws` is None
                self.ws.close()
        else:
            return False

    def on_RoomStreamAdaptationMessage(self, m: Message):
        self.log_txt(f'直播间adaptation: {m.adaptation_type}')

    def on_LuckyBoxMessage(self, m: Message):
        """红包"""
        self.log_txt(f"【红包】{m.user.nickname}{ZWSP} 送出红包，价值 {m.diamond_count}，标题：{m.title}")
        self.queue(self.join_luckybox, m.box_id)

    def on_PreviewCjRpMessage(self, m: Message):
        """红包"""
        cjrp = m.cjrp
        num = f'{cjrp.left_num}/{cjrp.total_num}'
        amount = f'{cjrp.left_amount}/{cjrp.total_amount}'
        currency = '钻' if cjrp.currency == 'Diamond' else cjrp.currency
        timing = f'时间 {format_readable_time(cjrp.send_time)} ~ {format_readable_time(cjrp.expire_time)}'
        self.log_txt(f"【红包】{cjrp.text} 剩余 {num}个，价值 {amount}({currency})，{timing}")

    def on_LuckyBoxRewardMessage(self, m: Message):
        """抽奖结果"""
        l: list[str] = m.rewarded_user_id
        self.log_txt(f"【红包中奖结果】{len(l)}人中奖。中奖用户（或ID）：{' │ '.join([self.nickname_or_id(i) for i in l])}")

    def on_LotteryEventNewMessage(self, m: Message):
        """福袋"""
        conditions_detail = m.conditions_detail
        if len(conditions_detail) > 1:
            conditions_detail = filter(lambda c: c.type != LotteryCondition.默认参与.value, conditions_detail)

        conditions = []
        for c in conditions_detail:
            if c.type == LotteryCondition.粉丝团等级.value:
                conditions.append(f'{LotteryCondition(c.type).name} {c.min_fans_level}')
            elif c.type == LotteryCondition.发送评论.value:
                conditions.append(f'{LotteryCondition(c.type).name} "{c.content}"')
            else:
                conditions.append(LotteryCondition(c.type).name)

        self.log_txt(
            f"【福袋】{m.lucky_count}个，{m.prize_count}{m.award_name}，"
            f"开奖时间 {format_readable_time(m.lottery_draw_time)}。参与条件：{' │ '.join(conditions)}")

        self.queue(self.join_lottery, m.lottery_id, m.conditions_detail)

    def on_LotteryCandidateEventMessage(self, m: Message):
        """参与抽奖提示"""
        if m.participate_success:
            self.log_txt(f"【福袋】成功参与福袋 {m.lottery_id}")
        else:
            return False

    def on_LotteryDrawResultEventMessage(self, m: Message):
        """福袋抽奖结果"""
        candidate_hint = ''
        try:
            candidate_num = json.loads(m.extra)['candidate_num']
            candidate_hint = f'，{candidate_num}人参与'
        except BaseException:
            pass
        self.log_txt(
            f"【福袋抽奖结果】{len(m.user_ids)}人中奖{candidate_hint}。"
            f"中奖用户（或ID）：{' │ '.join([self.nickname_or_id(i) for i in m.user_ids])}")

    def on_LinkMessage(self, m: Message):
        """连线信息"""
        oneof = m.WhichOneof('content')
        if not oneof:
            return False

        def print_linked_users(fmt: str, linked_users: list) -> tuple[int, str]:
            users: list[str] = []
            for e in linked_users:
                users.append(f'{e.user.nickname}{ZWSP}({e.user.id})')
                if e.content.linkmic_content.host_name:
                    users[-1] += ' ' + e.content.linkmic_content.host_name
                if e.link_type == 2:
                    users[-1] += ' 语音连线'
                if e.silence_status == 1:
                    users[-1] += ' 静音'
            # return len(linked_users), ' │ ' .join(users)
            if len(linked_users) > 0:
                self.log_txt(fmt.format(n_users=len(linked_users), s_users=' │ ' .join(users)))

        content = getattr(m, oneof)
        if oneof == 'linked_list_change_content':
            print_linked_users("【连线 变化】{n_users}位连线主播：{s_users}", content.linked_users)
        elif oneof == 'switch_scene_content':
            print_linked_users("【连线 切换场景】{n_users}位连线主播：{s_users}", content.switch_scene_data.linked_users)
        elif oneof == 'enter_content':
            users = [u for m in content.linker_content_map.values() for u in m.linked_users]
            print_linked_users("【连线 入场】{n_users}位入场主播：{s_users}", users)
            print_linked_users("【连线 变化】{n_users}位连线主播：{s_users}", content.linked_users)
        elif oneof == 'update_user_content':
            print_linked_users("【连线 变化】{n_users}位连线用户：{s_users}", content.linked_users)
        elif oneof == 'leave_content':
            # users = [u for m in content.linker_content_map.values() for u in m.linked_users]
            # print_linked_users("【连线 离场】{n_users}位离场主播：{s_users}", users)
            print_linked_users("【连线 离场】{n_users}位离场主播：{s_users}", content.linked_users)
        elif oneof == 'audience_waiting_list_change':
            self.log_txt(f"【连线 等待连线】{content.total_waiting_cnt}位观众等待连线")
        else:
            self.log_txt(f"【连线 {oneof}】content: {message_to_json(content)}")

    def on_ScreenChatMessage(self, m: Message):
        """飘屏消息"""
        self.log_txt(f"【飘屏 {m.screen_chat_type}】{m.user.nickname}{ZWSP}：{m.content}")

    def on_PrivilegeScreenChatMessage(self, m: Message):
        """高级飘屏"""
        self.log_txt(f"【飘屏 样式等级{m.style}】{m.user.nickname}{ZWSP}：{m.content}")

    def on_AudioChatMessage(self, m: Message):
        """语音消息"""
        self.log_txt(
            f"【语音 {math.ceil(m.audio_duration/1000)}s】{m.user.nickname}{ZWSP}：{m.content} ({m.audio_url})")

    def on_ToastMessage(self, m: Message):
        self.log_txt(f"【弹出提示】{m.content}")

    def on_CommonToastMessage(self, m: Message):
        """弹出提示，比如全员放大提示"""
        self.log_txt(f"【弹出提示】{render_text(m.common.display_text)}")

    def on_RoomDataSyncMessage(self, m: Message):
        """观众连线、福袋、红包、置顶评论、双倍点赞"""
        payload = self._parse_from_string(m.syncKey, m.payload)
        self.log_json(f'  {m.syncKey}={message_to_json(payload)}')

        ignored_types = ['InteractEffectSyncData', 'RoomLinkmicMicDisplayInfoSyncData']
        handled = False

        if m.syncKey in ignored_types:
            handled = True
        elif m.syncKey == 'RoomLinkMicSyncData':
            link_type_map = {1: '视频连线', 2: '语音连线'}
            users = []
            for e in payload.linked_users:
                users.append(f'{e.user.nickname}{ZWSP}({e.user.id})')
                if e.link_type in link_type_map:
                    users[-1] += ' ' + link_type_map[e.link_type]
                if e.silence_status == 1:
                    users[-1] += ' 静音'
            self.log_txt(f"【连线状态同步】{len(users)}位连线用户：{' │ ' .join(users)}")
            handled = True
        elif m.syncKey == 'LotteryInfoSyncData':
            l = payload
            if l.lottery_type == 1:
                timing = f'时间 {format_readable_time(l.start_time)} ~ {format_readable_time(l.draw_time)}'
                exipred_hint = ' （已过期）' if l.draw_time + 1 <= time.time() else ''
                self.log_txt(
                    f"【福袋】{l.lucky_count}个，{l.prize_count}钻，{l.candidate_total_count}人参与，{timing}{exipred_hint}")
                if not exipred_hint:
                    self.queue(self.join_lottery, l.lottery_id)
                handled = True
        elif m.syncKey == 'HighlightContainerSyncData':
            all_parsed = True
            for item in payload.highlight_items:
                if item.data_type == 4:  # HighlightDataComment 104
                    if item.end_time < time.time():
                        continue
                    self.log_txt(f'【置顶评论】{item.comment_data.nick_name}：{item.comment_data.content}')
                else:
                    all_parsed = False
            handled = all_parsed
        elif m.syncKey == 'DoubleLikeSyncData':
            self.log_txt(f"【双倍点赞】{render_text(payload.normal_display_text)}")
            handled = True
        elif m.syncKey == 'PreviewPromotionSyncData':
            if payload.type == 2:
                if time.time() < payload.lucky_money.display_end_at + 10:
                    self.log_txt(f"【红包信息】{payload.lucky_money.text}")
                    self.queue(self.join_luckybox, payload.lucky_money.box_id)
                handled = True

        if not handled:
            self.log_txt(f'【RoomDataSyncMessage】  {m.syncKey}={self._try_dump_json(m.syncKey, m.payload)}')

    def on_RoomCommentTopicMessage(self, m: Message):
        """话题"""
        l = m.comment_topic_chat_list
        s = ' │ '.join([f"{e.guide_text}：{e.featured_chat}" for e in l])
        self.log_txt(f'【话题】{s}')

    def on_HotChatMessage(self, m: Message):
        """热聊话题"""
        text = f'{m.title}："{m.content}" ×{m.num[-1]}'
        self.log_txt(f'【热聊话题】{text}')

    def on_AnchorLinkmicSilenceMessage(self, m: Message):
        """连线静音"""
        silence_action = {1: '静音', 2: '取消静音'}.get(m.silence_action)
        self.log_txt(
            f'【静音】{self.nickname_or_id(m.from_user_id)}{ZWSP} 将 {self.nickname_or_id(m.to_user_id)}{ZWSP} {silence_action} 了')

    def on_RoomNotifyMessage(self, m: Message):
        """直播间公告"""
        self.log_txt('【直播间公告】' + render_text(m.common.display_text))

    def on_RoomMessage(self, m: Message):
        """直播间信息"""
        if m.common.display_text.default_pattern:
            self.log_txt('【直播间消息】' + render_text(m.common.display_text))
        elif m.content:
            self.log_txt('【直播间消息】' + m.content)
        else:
            return False

    def on_NotifyEffectMessage(self, m: Message):
        """特效公告"""
        display_item = find_if(m.text_v2.display_items, lambda e: e.display_item_type == 2)
        if display_item and len(m.text_v2.display_items) == 1:
            self.log_txt('【特效公告】' + render_text(display_item.text_item.text))
        else:
            return False

    def on_InRoomBannerMessage(self, m: Message):
        """播间横幅内容，比较杂，含挑战榜、百强榜、观众充能、升级派对、礼物心愿单"""
        self.log_json(f'  extra={m.extra}')

        extra = json.loads(m.extra)
        handled = False
        if 'wishList' in extra:
            wish_list = json.loads(extra['wishList'])
            wishes: list[str] = []
            for wish in wish_list['wish_banner_data']['banner_wish_list']:
                infos: list[str] = []
                for info in wish['wish_info_list']:
                    # logger.info(info)
                    infos.append(
                        f"{info['wish_info_extra']['gift_alias']}({info['wish_info_extra']['diamond_count']}钻)"
                        f" {info.get('current_progress',0)}/{info['target_progress']}")
                wishes.append(wish['wish_name'] + '：' + ' │ '.join(infos))
            self.log_txt('【横幅 心愿单】' + ' ┃ '.join(wishes))
            handled = True

        if not handled:
            # 25v_11_ai_gift, 26v_9_task
            b = re.search(r'\d{2}v_\d+', repr([*extra.keys()])) or \
                ('gift_flower' in extra) or ('accompany_indicator' in extra) or ('fansclub_party' in extra) or \
                ('fansclub_clublevel_banner_v2' in extra) or ('cube_pkweek' in extra) or \
                ('giftwall_mission_group_live' in extra)
            return b

    def on_BattleStatusMessage(self, m: Message):
        """PK 开始、惩罚、结束"""
        status = {1: '开始', 2: '惩罚', 3: '结束'}.get(m.status, f'状态 {m.status}')
        duration = (int(m.end_time_ms) - int(m.start_time_ms)) // 1000
        punish_hint = f'，惩罚时长{m.punish_duration}秒' if m.status == 2 else ''
        self.log_txt(
            f'【PK {status}】时间{duration}秒，{format_readable_time(m.start_time_ms[:-3])} ~ {format_readable_time(m.end_time_ms[:-3])}{punish_hint}')

    def on_LinkMicMethod(self, m: Message):
        """连麦分数及PK排名"""
        # 100 无可用信息
        # 202 PK分数更新
        if m.message_type in (100, 202):
            return True
        return False

    def on_LinkMicArmiesMethod(self, m: Message):
        """PK 战队，即榜前三"""
        user_armies_list = []
        for user_id, user_armies in m.user_armies_map.items():
            armies = [f'{u.nickname}{ZWSP}({u.score})' for u in user_armies.user_armies]
            user_armies_list.append(
                f'\n    {self.nickname_or_id(user_id)}{ZWSP}：' + ' │ '.join(armies or ['(空)'])
            )
        self.log_txt('【PK 战队】' + ''.join(user_armies_list))  # ' ┃ '.join(user_armies_list)

    def on_TaskCenterEntranceMessage(self, m: Message):
        "未知"
        self.log_json(f'  extra={m.extra}')

        extra = json.loads(m.extra)
        if 'popularity_egg_panel' in extra:
            return

        self.log_txt(message_to_json(m))
        self.log_txt(f'  extra={m.extra}')

    def on_RoomIndicatorMessage(self, m: Message):
        """加热中"""
        if m.biz_type in [3, 9]:
            return True
        # unknown types
        return False

    def on_BattleAuxiliaryMessage(self, m: Message):
        """PK 玩法"""
        if m.type == 1:
            self.log_txt(f'【PK 玩法】{m.reply_content.reply_string} │ 规则：{m.reply_content.auxiliary_data.rule_content}')
        elif m.type == 3:
            self.log_txt(f'【PK 玩法】{m.close_content.close_content}')
        else:
            return False

    def on_LinkMicBattleMethod(self, m: Message):
        "PK 开始，详细信息"
        def pk_user(o):
            segs = [o.user_img_flip_info.pk_stage_desc]
            c = o.consecutive_record
            if c.consecutive_count > 1:
                consecutive_record_type = {1: '连胜', 2: '连败'}[c.battle_result_type]
                segs.append(f'{consecutive_record_type}×{c.consecutive_count}')
            s = ' '.join([s for s in segs if s])
            s = f'({s})' if s else ''
            return f'{o.user.nick_name}{s}'

        self.log_txt(
            '【PK】' +
            ' │ '.join([pk_user(info) for info in m.user_infos.values()]) +
            f' 由{self.nickname_or_id(m.battle_settings.initiator_id)}{ZWSP}发起'
        )

    def on_LinkMicBattleFinishMethod(self, m: Message):
        """PK 结束"""
        self.log_txt(
            '【PK 结束分数】' +
            ' │ '.join([
                f'{self.nickname_or_id(o.user_id)}{ZWSP}: {o.score}' for o in m.battle_scores
            ]))

        if '获得音浪' in m.battle_settings.lynx_data:
            lynx_data = json.loads(m.battle_settings.lynx_data)
            battle_finish_data = lynx_data.get('battle_finish_data', {})

            self.log_txt(
                '【PK 结束音浪】' +
                ' │ '.join([
                    f'{self.nickname_or_id(user_id)}{ZWSP}: ' + data.get('summary_value', '（未知）')
                    for user_id, data in battle_finish_data.items()
                ]))

    def on_ProfileViewMessage(self, m: Message):
        """亲密度"""
        self.log_txt(f'【亲密度】{render_text(m.title)}{render_text(m.sub_title)}')

    def on_ItemShareMessage(self, m: Message):
        """分享内容"""
        self.log_txt(f'【分享内容】{render_text(m.share_text)} {m.item_style.name} ({m.item_style.icon.url_list[0]})')

    def on_HighlightComment(self, m: Message):
        """置顶评论"""
        if m.action_type == 1:
            self.log_txt(f'【置顶评论】{m.operator_nickname}{ZWSP}：{m.content}')
        elif m.action_type == 2:
            self.log_txt(f'【置顶评论】{m.operator_nickname}{ZWSP} 取消了置顶评论')
        else:
            return False

    def on_GroupLiveMemberChangeMessage(self, m: Message):
        """团播"""
        users = [f'{o.user.nickname}{ZWSP}({o.user.id}) {o.score}' for o in m.members]
        self.log_txt(f"【团播】主播成员 {' │ '.join(users)}")

    def on_GroupLiveGiftRecipientRecommendMessage(self, m: Message):
        """团播"""
        self.log_txt(f'【团播】赠礼建议主播 {self.nickname_or_id(m.recipient_user_id)}')

    def on_GroupLiveContainerChangeMessage(self, m: Message):
        """团播"""
        for o in m.data:
            if o.type == 9:
                try:
                    p = json_loads_object(o.container_payload)
                except BaseException:
                    print(o.container_payload)
                    continue
                users = [f'{i.user.nickname}{ZWSP}({i.score})' for i in p.challenge_user_infos]
                self.log_txt(f"【团播】{p.title} 总进度：{p.total_score}/{p.target_score} {' │ '.join(users)}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="python -m dy_live.server", description="一个简单的douyin直播间消息解析例程")
    parser.add_argument('live_id', nargs='?', type=int, help="直播间ID，即直播间链接 'https://live.douyin.com/123456789' 里面的数字")
    parser.add_argument('--interval', type=int, default=10, help="轮询直播间状态的时间间隔，单位秒")
    parser.add_argument('--record', action=argparse.BooleanOptionalAction, default=True,
                        help="将已识别消息的简要内容写到 'live.<live_id>.<YYYY-MM-DD_HHMMSS>.log'")
    parser.add_argument('--record-unknown', action=argparse.BooleanOptionalAction,
                        default=True, help="将未识别消息转为JSON格式写到 'live.*.log'")
    parser.add_argument('--record-json', action=argparse.BooleanOptionalAction,
                        default=True, help="将消息转为JSON格式写到 'live.*.json'")
    parser.add_argument('--keep-uncompressed', action=argparse.BooleanOptionalAction,
                        default=False, help="压缩完成后保留原始文件（文件夹）")
    parser.add_argument('--print-pk-events', action=argparse.BooleanOptionalAction,
                        default=False, help="命令行打印 PK 相关消息")
    parser.add_argument('--replay', dest='prefix',
                        help="重新解读 <PREFIX>.tar.zstd 文件中消息的简要内容。"
                        "<PREFIX> 既可以对应一个 .tar.zstd 归档包，也可以对应 .json 文件 + .user 目录")
    parser.add_argument('--dump_http', action='store_true')
    args = parser.parse_args()

    # Live REST/WebSocket uses the same Auth session as the main-site APIs;
    # an explicit DY_LIVE_COOKIES remains an opt-in legacy override handled by
    # load_env().
    common_util.load_env()
    auth = common_util.get_live_auth()

    # Run in Chrome web console, and manually save clipboard text as 'localStorage.json':
    #   copy(JSON.stringify(localStorage));
    if path.exists('localStorage.json'):
        local_storage = json.loads(Path('localStorage.json').read_text(encoding='utf8'))
        auth.perepare_auth(
            os.getenv('DY_COOKIES'),
            local_storage["security-sdk/s_sdk_sign_data_key/web_protect"],
            local_storage["security-sdk/s_sdk_crypt_sdk"]
        )
        (auth.cookie or {}).pop('bd_ticket_guard_ts_sign_id')

    live = DouyinLive(
        str(args.live_id),
        auth=auth,
        record=args.record,
        record_unknown=args.record_unknown,
        dump_json=args.record_json,
        keep_uncompressed=args.keep_uncompressed,
        print_pk_events=args.print_pk_events,
        dump_http=args.dump_http,
    )

    # info, _ = DouyinAPI.get_live_info(auth, str(args.live_id))
    # live.live_room_id = info['room_id']
    # logger.info(f'Trying to send special comment "11111"')
    # resp = live.request(
    #     'GET', '/webcast/room/chat/',
    #     endpoint_params={
    #         'room_id': live.live_room_id,
    #         'content': '11111',
    #         'type': '0',
    #         'rtf_content': '',
    #         'paste_edit_method': 'non_paste',
    #     },
    #     enter_from='web_homepage_follow',
    #     with_verify_fp=True,
    # )
    # print(auth.cookie)
    # print(resp.json()['status_code'])
    # print(auth.cookie)
    # sys.exit(0)

    if args.live_id:
        live.run_forever(poll_interval=args.interval)
    elif args.prefix:
        live.replay(args.prefix)
    else:
        parser.error("应当指定 live_id 或者 --replay 参数")
