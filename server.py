"""Media Overlay jam server: who is online, rooms, a shared queue, and when
to play. Every app streams the music itself; no audio passes through here.

Run:  python server.py            (listens on $PORT, default 8765, path /ws)
Health check:  GET /health -> ok

All state is in memory. Logs hold counts and errors only.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import math
import os
import random
import secrets
import time
from collections import deque
from http import HTTPStatus
from urllib.parse import urlparse

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("jam")

VERSION = 1
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
CODE_LEN = 6
NAME_MAX = 24
MAX_MESSAGE = 64 * 1024
TRACKS_PER_ADD = 50
RATE_PER_S = 20
RATE_CLOSE_AFTER_MS = 3000
RESTART_AFTER_S = 3.0
TRACK_MAX_BYTES = 1200  # one queue entry, as sent to every member
OUTBOX_MAX = 64  # messages waiting for one slow member before it is dropped
SEND_TIMEOUT_S = 10.0
ENDED_WINDOW_S = 5.0  # "ended" only counts this close to the end of the song
MAX_POSITION_S = 86_400.0

YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com",
                 "youtu.be"}
SOUNDCLOUD_HOSTS = {"soundcloud.com", "www.soundcloud.com", "m.soundcloud.com",
                    "on.soundcloud.com"}
_TEXT_LIMITS = {"id": 64, "title": 200, "artist": 120, "album": 120, "lyrics_title": 200,
                "lyrics_artist": 120}


# ---- validation ------------------------------------------------------------------------
def encode(msg: dict) -> str:
    return json.dumps(msg, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _clean_text(value, limit: int) -> str:
    """Printable text without markup characters (other apps may show it in
    places that understand HTML)."""
    if not isinstance(value, str):
        return ""
    text = "".join(ch for ch in value if ch.isprintable() and ch not in "<>")
    return " ".join(text.split())[:limit]


def clean_name(value) -> str | None:
    name = _clean_text(value, 10_000)[:NAME_MAX].strip()
    return name or None


def _https_host(url) -> str | None:
    if not isinstance(url, str) or len(url) > 500:
        return None
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    if parsed.scheme != "https" or parsed.username or parsed.password:
        return None
    return (parsed.hostname or "").lower() or None


def validate_track(raw) -> dict | None:
    """A cleaned copy of a track another member sent, or None if unsafe."""
    if not isinstance(raw, dict):
        return None
    source = raw.get("source")
    host = _https_host(raw.get("url"))
    if source == "youtube":
        if host not in YOUTUBE_HOSTS:
            return None
    elif source == "soundcloud":
        if host not in SOUNDCLOUD_HOSTS:
            return None
    else:
        return None
    track = {"source": source, "url": raw["url"]}
    for field, limit in _TEXT_LIMITS.items():
        track[field] = _clean_text(raw.get(field), limit)
    if not track["id"] or not track["title"]:
        return None
    try:
        duration = float(raw.get("duration_s") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    track["duration_s"] = duration if 0 <= duration <= 86_400 else 0.0
    thumb = raw.get("thumbnail_url")
    thumb_host = _https_host(thumb)
    track["thumbnail_url"] = thumb if thumb_host and (
        thumb_host == "i.ytimg.com" or thumb_host.endswith(".sndcdn.com")) else ""
    track["is_video"] = bool(raw.get("is_video"))
    # Keep every entry small: the whole queue goes to every member on changes.
    while len(encode(track).encode("utf-8")) > TRACK_MAX_BYTES:
        longest = max(_TEXT_LIMITS, key=lambda f: len(track[f]))
        if not track[longest]:
            return None
        track[longest] = track[longest][:len(track[longest]) // 2]
    if not track["id"] or not track["title"]:
        return None
    return track


def client_ip(headers, remote, trust_proxy: bool) -> str:
    """The person's address; behind Fly/Render the proxy tells us."""
    if trust_proxy and headers is not None:
        for key in ("Fly-Client-IP", "X-Forwarded-For"):
            value = headers.get(key)
            if value:
                return value.split(",")[0].strip()
    if isinstance(remote, (tuple, list)) and remote:
        return str(remote[0])
    return "?"


def _finite(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def new_code(rng=None) -> str:
    pick = (rng or secrets.SystemRandom()).choice
    return "JAM-" + "".join(pick(CODE_ALPHABET) for _ in range(CODE_LEN))


def normalize_code(value) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip().upper()
    if text.startswith("JAM-"):
        text = text[4:]
    if len(text) != CODE_LEN or any(ch not in CODE_ALPHABET for ch in text):
        return None
    return "JAM-" + text


# ---- state -------------------------------------------------------------------------------
class Member:
    _ids = itertools.count(1)

    def __init__(self, ws):
        self.id = f"m{next(self._ids)}"
        self.ws = ws
        self.name: str | None = None
        self.room: Room | None = None
        self.sent = deque()  # message times (ms) in the last second
        self.excess_since: int | None = None
        self.last_rate_error = 0
        self.ip = "?"
        self.failed_joins = 0
        self.outbox: asyncio.Queue | None = None
        self.writer: asyncio.Task | None = None


class Room:
    def __init__(self, code: str, public: bool, control: str, host: Member, now: int):
        self.code = code
        self.public = public
        self.control = control
        self.host = host
        self.members: list[Member] = []
        self.queue: list[dict] = []  # {"uid", "track", "by"}
        self.current: str | None = None
        self.playing = False
        self.anchor_pos = 0.0
        self.anchor_s = now
        self.waiting = False
        self.wait_for: set[str] = set()
        self.ready: set[str] = set()
        self.wait_task: asyncio.Task | None = None
        self.created = now
        self._uids = itertools.count(1)

    def new_uid(self) -> str:
        return f"q{next(self._uids)}"

    def index_of(self, uid) -> int:
        for i, item in enumerate(self.queue):
            if item["uid"] == uid:
                return i
        return -1

    def current_item(self) -> dict | None:
        i = self.index_of(self.current)
        return self.queue[i] if i >= 0 else None

    def duration(self) -> float:
        item = self.current_item()
        return float(item["track"].get("duration_s") or 0) if item else 0.0

    def position(self, now: int) -> float:
        pos = self.anchor_pos
        if self.playing:
            pos += max(0, now - self.anchor_s) / 1000.0
        d = self.duration()
        return max(0.0, min(pos, d) if d > 0 else pos)

    def state(self) -> dict:
        return {
            "t": "room", "code": self.code, "public": self.public, "control": self.control,
            "host": self.host.id,
            "members": [{"id": m.id, "name": m.name} for m in self.members],
            "queue": self.queue, "current": self.current, "playing": self.playing,
            "anchor_pos": self.anchor_pos, "anchor_s": self.anchor_s, "waiting": self.waiting,
        }

    def summary(self) -> dict:
        item = self.current_item()
        return {"code": self.code, "host": self.host.name, "members": len(self.members),
                "now": item["track"]["title"] if item else None}


class Server:
    def __init__(self, clock=None, start_delay_ms: int = 500, ready_timeout_s: float = 6.0,
                 online_interval: float = 2.0):
        self.now_ms = clock or (lambda: int(time.time() * 1000))
        self.start_delay_ms = start_delay_ms
        self.ready_timeout_s = ready_timeout_s
        self.online_interval = online_interval
        self.max_members = 25
        self.max_queue = 500
        self.max_rooms = 300
        self.max_connections = 2000
        self.max_per_ip = 20
        self.max_rooms_per_ip = 3
        self.max_failed_joins = 10
        self.hello_timeout_s = 10.0
        self.trust_proxy = os.environ.get("TRUST_PROXY", "1") == "1"
        self.members: dict[str, Member] = {}
        self.rooms: dict[str, Room] = {}
        self._online_task: asyncio.Task | None = None
        self._last_online_sent = -1
        self._rng = secrets.SystemRandom()

    # ---- serving ---------------------------------------------------------------------
    async def serve(self, host: str, port: int):
        self._online_task = asyncio.create_task(self._online_loop())
        return await serve(self.handler, host, port, max_size=MAX_MESSAGE,
                           process_request=self._process_request, ping_interval=20,
                           ping_timeout=20)

    async def shutdown(self) -> None:
        if self._online_task:
            self._online_task.cancel()
        for room in list(self.rooms.values()):
            if room.wait_task:
                room.wait_task.cancel()

    @staticmethod
    def _process_request(connection, request):
        path = request.path.split("?", 1)[0]
        if path == "/health":
            return connection.respond(HTTPStatus.OK, "ok\n")
        if path != "/ws":
            return connection.respond(HTTPStatus.NOT_FOUND, "not found\n")
        return None

    def online_count(self) -> int:
        return sum(1 for m in self.members.values() if m.name)

    async def _online_loop(self) -> None:
        while True:
            await asyncio.sleep(self.online_interval)
            count = self.online_count()
            if count != self._last_online_sent:
                self._last_online_sent = count
                await self._broadcast_all({"t": "online", "count": count})

    async def handler(self, ws) -> None:
        request = getattr(ws, "request", None)
        ip = client_ip(getattr(request, "headers", None), ws.remote_address, self.trust_proxy)
        same_ip = sum(1 for m in self.members.values() if m.ip == ip)
        if len(self.members) >= self.max_connections or same_ip >= self.max_per_ip:
            await ws.close(1013, "too many connections")
            return
        member = Member(ws)
        member.ip = ip
        member.outbox = asyncio.Queue(maxsize=OUTBOX_MAX)
        member.writer = asyncio.create_task(self._writer(member))
        self.members[member.id] = member
        hello_deadline = asyncio.create_task(self._hello_deadline(member))
        try:
            async for raw in ws:
                if not self._rate_ok(member):
                    continue
                try:
                    msg = json.loads(raw)
                    if not isinstance(msg, dict) or not isinstance(msg.get("t"), str):
                        raise ValueError
                except (ValueError, RecursionError):
                    await self._error(member, "bad_message", "That message was not understood.")
                    continue
                await self._dispatch(member, msg)
        except ConnectionClosed:
            pass
        except Exception:
            log.exception("connection failed")
        finally:
            hello_deadline.cancel()
            self.members.pop(member.id, None)
            if member.room is not None:
                await self._leave(member, notify=False)
            member.writer.cancel()

    async def _hello_deadline(self, member: Member) -> None:
        await asyncio.sleep(self.hello_timeout_s)
        if not member.name:
            await member.ws.close(1008, "say hello")

    async def _writer(self, member: Member) -> None:
        """Sends one member's messages, so a slow member never holds up others."""
        try:
            while True:
                data = await member.outbox.get()
                await asyncio.wait_for(member.ws.send(data), SEND_TIMEOUT_S)
        except (ConnectionClosed, asyncio.CancelledError):
            pass
        except asyncio.TimeoutError:
            await member.ws.close(1008, "too slow")
        except Exception:
            log.exception("send failed")

    def _rate_ok(self, member: Member) -> bool:
        now = self.now_ms()
        sent = member.sent
        sent.append(now)
        while sent and now - sent[0] > 1000:
            sent.popleft()
        if len(sent) <= RATE_PER_S:
            member.excess_since = None
            return True
        if member.excess_since is None:
            member.excess_since = now
        elif now - member.excess_since > RATE_CLOSE_AFTER_MS:
            asyncio.ensure_future(member.ws.close(1008, "too many messages"))
            return False
        if now - member.last_rate_error > 1000:
            member.last_rate_error = now
            asyncio.ensure_future(self._error(member, "rate", "Slow down a little."))
        return False

    # ---- sending ------------------------------------------------------------------------
    def _queue_out(self, member: Member, data: str) -> None:
        """Hand a message to the member's writer; never waits for the network."""
        if member.outbox is None:
            return
        try:
            member.outbox.put_nowait(data)
        except asyncio.QueueFull:
            # This member stopped reading: drop them instead of everyone waiting.
            member.outbox = None
            asyncio.ensure_future(member.ws.close(1008, "too slow"))

    async def _send(self, member: Member, msg: dict) -> None:
        self._queue_out(member, encode(msg))

    async def _broadcast_all(self, msg: dict) -> None:
        data = encode(msg)
        for m in list(self.members.values()):
            if m.name:
                self._queue_out(m, data)

    async def _broadcast_room(self, room: Room, msg: dict | None = None) -> None:
        data = encode(msg or room.state())
        for m in list(room.members):
            self._queue_out(m, data)

    async def _event(self, room: Room, text: str) -> None:
        await self._broadcast_room(room, {"t": "event", "text": text})

    async def _error(self, member: Member, code: str, message: str) -> None:
        await self._send(member, {"t": "error", "code": code, "message": message})

    # ---- dispatch -----------------------------------------------------------------------
    async def _dispatch(self, member: Member, msg: dict) -> None:
        t = msg["t"]
        if t == "hello":
            await self._hello(member, msg)
            return
        if not member.name:
            await self._error(member, "hello_first", "Say hello first.")
            return
        handler = getattr(self, f"_on_{t}", None)
        if handler is None:
            await self._error(member, "bad_message", "That message was not understood.")
            return
        await handler(member, msg)

    async def _hello(self, member: Member, msg: dict) -> None:
        if msg.get("v") != VERSION:
            # Sent directly (not queued): the connection closes right after.
            try:
                await asyncio.wait_for(member.ws.send(encode(
                    {"t": "error", "code": "version",
                     "message": "This app version can't use this jam server. Please update."})), 2)
            except Exception:
                pass
            await member.ws.close(1008, "version")
            return
        name = clean_name(msg.get("name"))
        if not name:
            await self._error(member, "name", "Please choose a name.")
            return
        member.name = name
        await self._send(member, {"t": "welcome", "v": VERSION, "you": member.id,
                                  "online": self.online_count()})

    async def _on_ping(self, member: Member, msg: dict) -> None:
        await self._send(member, {"t": "pong", "id": msg.get("id"), "c": msg.get("c"),
                                  "s": self.now_ms()})

    async def _on_rename(self, member: Member, msg: dict) -> None:
        name = clean_name(msg.get("name"))
        if not name:
            await self._error(member, "name", "Please choose a name.")
            return
        member.name = name
        if member.room:
            await self._broadcast_room(member.room)

    async def _on_list(self, member: Member, msg: dict) -> None:
        rooms = sorted((r for r in self.rooms.values() if r.public),
                       key=lambda r: r.created, reverse=True)[:50]
        await self._send(member, {"t": "rooms", "rooms": [r.summary() for r in rooms]})

    async def _on_create(self, member: Member, msg: dict) -> None:
        if member.room:
            await self._leave(member)
        hosted_here = sum(1 for r in self.rooms.values() if r.host.ip == member.ip)
        if len(self.rooms) >= self.max_rooms or hosted_here >= self.max_rooms_per_ip:
            await self._error(member, "busy", "The jam server is full right now.")
            return
        code = new_code(self._rng)
        while code in self.rooms:
            code = new_code(self._rng)
        control = "host" if msg.get("control") == "host" else "all"
        room = Room(code, bool(msg.get("public")), control, member, self.now_ms())
        self.rooms[code] = room
        room.members.append(member)
        member.room = room
        await self._broadcast_room(room)

    async def _on_join(self, member: Member, msg: dict) -> None:
        code = normalize_code(msg.get("code"))
        room = self.rooms.get(code) if code else None
        if room is None:
            member.failed_joins += 1
            if member.failed_joins > self.max_failed_joins:
                await member.ws.close(1008, "too many wrong codes")
                return
            await self._error(member, "no_room", "There is no jam with that code.")
            return
        if member.room is room:
            await self._send(member, room.state())
            return
        if len(room.members) >= self.max_members:
            await self._error(member, "room_full", "That jam is full.")
            return
        if member.room:
            await self._leave(member)
            # The room may have changed while we were leaving the old one.
            if self.rooms.get(code) is not room:
                await self._error(member, "no_room", "There is no jam with that code.")
                return
            if len(room.members) >= self.max_members:
                await self._error(member, "room_full", "That jam is full.")
                return
        room.members.append(member)
        member.room = room
        await self._broadcast_room(room)
        await self._event(room, f"{member.name} joined")

    async def _on_leave(self, member: Member, msg: dict) -> None:
        if member.room:
            await self._leave(member)

    async def _leave(self, member: Member, notify: bool = True) -> None:
        room = member.room
        member.room = None
        if room is None:
            return
        # Change all state before awaiting anything: other members may leave
        # (or messages arrive) while we are sending.
        if member in room.members:
            room.members.remove(member)
        empty = not room.members
        new_host = None
        start_now = False
        if empty:
            if room.wait_task:
                room.wait_task.cancel()
                room.wait_task = None
            self.rooms.pop(room.code, None)
        else:
            if room.host is member:
                room.host = new_host = room.members[0]
            if room.waiting:
                room.wait_for.discard(member.id)
                start_now = bool(room.wait_for) and room.wait_for <= room.ready
        if notify:
            await self._send(member, {"t": "left"})
        if empty:
            return
        await self._event(room, f"{member.name} left")
        if new_host is not None:
            await self._event(room, f"{new_host.name} is now host")
        if start_now:
            await self._begin(room, room.current)
        else:
            await self._broadcast_room(room)

    # ---- room commands ---------------------------------------------------------------------
    async def _room_for(self, member: Member, needs_control: bool) -> Room | None:
        room = member.room
        if room is None:
            await self._error(member, "no_room", "You are not in a jam.")
            return None
        if needs_control and room.control == "host" and room.host is not member:
            await self._error(member, "not_allowed", "Only the host controls this jam.")
            return None
        return room

    async def _on_set_control(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, False)
        if room is None:
            return
        if room.host is not member:
            await self._error(member, "not_allowed", "Only the host can change this.")
            return
        room.control = "host" if msg.get("control") == "host" else "all"
        await self._broadcast_room(room)

    async def _on_add(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, False)
        if room is None:
            return
        raw = msg.get("tracks")
        tracks = [validate_track(t) for t in (raw if isinstance(raw, list) else [])[:TRACKS_PER_ADD]]
        tracks = [t for t in tracks if t]
        if not tracks:
            await self._error(member, "invalid_track", "Only YouTube and SoundCloud songs can be shared.")
            return
        room_left = self.max_queue - len(room.queue)
        if room_left <= 0:
            await self._error(member, "queue_full", "The jam queue is full.")
            return
        if len(tracks) > room_left:
            tracks = tracks[:room_left]
            await self._error(member, "queue_full", "The jam queue is full; some songs were left out.")
        finished = self._finished(room)
        items = [{"uid": room.new_uid(), "track": t, "by": member.name} for t in tracks]
        at = room.index_of(room.current) + 1 if (msg.get("next") and room.current) else len(room.queue)
        room.queue[at:at] = items
        what = items[0]["track"]["title"] if len(items) == 1 else f"{len(items)} songs"
        play_now = bool(msg.get("play")) and (room.control == "all" or room.host is member)
        if room.current is None or play_now or finished:
            await self._start(room, items[0]["uid"])
        else:
            await self._broadcast_room(room)
        await self._event(room, f"{member.name} added {what}")

    def _finished(self, room: Room) -> bool:
        """Stopped at the end of the last song: the next song added should play."""
        if room.playing or room.waiting or room.current is None:
            return False
        last = room.queue[-1]["uid"] if room.queue else None
        d = room.duration()
        return room.current == last and d > 0 and room.anchor_pos >= d - 0.5

    async def _start(self, room: Room, uid: str | None) -> None:
        """Make uid current and wait (briefly) for every member to have it loaded."""
        if room.wait_task:
            room.wait_task.cancel()
            room.wait_task = None
        room.current = uid
        room.playing = False
        room.anchor_pos = 0.0
        room.anchor_s = self.now_ms()
        if uid is None:
            room.waiting = False
            await self._broadcast_room(room)
            return
        room.waiting = True
        room.wait_for = {m.id for m in room.members}
        room.ready = set()
        room.wait_task = asyncio.create_task(self._wait_timeout(room, uid))
        await self._broadcast_room(room)

    async def _wait_timeout(self, room: Room, uid: str) -> None:
        try:
            await asyncio.sleep(self.ready_timeout_s)
        except asyncio.CancelledError:
            return
        room.wait_task = None
        if room.waiting and room.current == uid:
            await self._begin(room, uid)

    async def _begin(self, room: Room, uid: str | None) -> None:
        """Everyone starts uid at the same moment, a little in the future."""
        if room.current != uid or uid is None:
            return
        if room.wait_task:
            room.wait_task.cancel()
            room.wait_task = None
        room.waiting = False
        room.playing = True
        room.anchor_s = self.now_ms() + self.start_delay_ms
        await self._broadcast_room(room)

    async def _on_ready(self, member: Member, msg: dict) -> None:
        room = member.room
        if room is None or not room.waiting or msg.get("uid") != room.current:
            return
        room.ready.add(member.id)
        if room.wait_for <= room.ready:
            await self._begin(room, room.current)

    async def _on_play(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, True)
        if room is None:
            return
        if room.current is None:
            if room.queue:
                await self._start(room, room.queue[0]["uid"])
                await self._event(room, f"{member.name} played")
            return
        if room.waiting:
            await self._begin(room, room.current)
        elif not room.playing:
            if room.duration() > 0 and room.anchor_pos >= room.duration() - 0.5:
                await self._start(room, room.current)  # finished: from the top
            else:
                room.playing = True
                room.anchor_s = self.now_ms() + self.start_delay_ms
                await self._broadcast_room(room)
        await self._event(room, f"{member.name} played")

    async def _on_pause(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, True)
        if room is None or room.current is None:
            return
        now = self.now_ms()
        if room.wait_task:
            room.wait_task.cancel()
            room.wait_task = None
        room.anchor_pos = room.position(now)
        room.anchor_s = now
        room.playing = False
        room.waiting = False
        await self._broadcast_room(room)
        await self._event(room, f"{member.name} paused")

    async def _on_seek(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, True)
        if room is None or room.current is None:
            return
        pos = _finite(msg.get("pos"))
        if pos is None:
            return
        pos = max(0.0, min(pos, MAX_POSITION_S))
        d = room.duration()
        room.anchor_pos = max(0.0, min(pos, d) if d > 0 else pos)
        room.anchor_s = self.now_ms() + (self.start_delay_ms if room.playing else 0)
        await self._broadcast_room(room)

    async def _advance(self, room: Room) -> None:
        i = room.index_of(room.current)
        if 0 <= i + 1 < len(room.queue):
            await self._start(room, room.queue[i + 1]["uid"])
            return
        # End of the queue: stay on the last song, stopped at its end.
        if room.wait_task:
            room.wait_task.cancel()
            room.wait_task = None
        room.playing = False
        room.waiting = False
        room.anchor_pos = room.duration()
        room.anchor_s = self.now_ms()
        await self._broadcast_room(room)

    async def _on_next(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, True)
        if room is None:
            return
        await self._advance(room)
        await self._event(room, f"{member.name} skipped")

    async def _on_prev(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, True)
        if room is None or room.current is None:
            return
        i = room.index_of(room.current)
        if room.position(self.now_ms()) > RESTART_AFTER_S or i <= 0:
            await self._start(room, room.current)
        else:
            await self._start(room, room.queue[i - 1]["uid"])

    async def _on_play_uid(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, True)
        if room is None:
            return
        if room.index_of(msg.get("uid")) < 0:
            return
        await self._start(room, msg.get("uid"))

    async def _on_remove(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, True)
        if room is None:
            return
        i = room.index_of(msg.get("uid"))
        if i < 0:
            return
        was_current = room.queue[i]["uid"] == room.current
        room.queue.pop(i)
        if was_current:
            nxt = room.queue[i]["uid"] if i < len(room.queue) else None
            if nxt is None:
                room.current = None
            await self._start(room, nxt)
        else:
            await self._broadcast_room(room)

    async def _on_move(self, member: Member, msg: dict) -> None:
        room = await self._room_for(member, True)
        if room is None:
            return
        i = room.index_of(msg.get("uid"))
        to = msg.get("to")
        if i < 0 or not isinstance(to, int) or not 0 <= to < len(room.queue) or to == i:
            return
        room.queue.insert(to, room.queue.pop(i))
        await self._broadcast_room(room)

    async def _on_ended(self, member: Member, msg: dict) -> None:
        room = member.room
        if room is None or room.waiting or msg.get("uid") != room.current or not room.playing:
            return
        d = room.duration()
        if d > 0 and room.position(self.now_ms()) < d - ENDED_WINDOW_S:
            return  # nowhere near the end: not a real "ended" (or someone skipping)
        await self._advance(room)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    port = int(os.environ.get("PORT", "8765"))

    async def run():
        server = Server()
        ws_server = await server.serve("0.0.0.0", port)
        log.info("jam server listening on port %d", port)
        async with ws_server:
            await ws_server.serve_forever()

    asyncio.run(run())


if __name__ == "__main__":
    main()
