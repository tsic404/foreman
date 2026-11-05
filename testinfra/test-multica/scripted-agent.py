#!/usr/bin/env python3
# The scripted agent backend: a served model endpoint whose task keeps its
# agent in flight for a declared window, so the user-perspective cases have a
# claim to cancel, a run to read the in-flight gauge from, an agent to carry
# across a fault window and two tasks to run at once.
#
#   scripted-agent.py serve [HOST:PORT]   # default 127.0.0.1:18123; port 0 prints the bound address
#   scripted-agent.py decide REQUEST.json # one decision line: call <command> | content
#
# Why it exists. A model that answers in prose settles a task in seconds, and
# every window becomes unreachable speculation. This source states the window
# itself: turn 0 carries the warmup call when one is declared, turns 1 through
# HOLD_TURNS ask the agent's shell tool to `sleep`, and turn HOLD_TURNS+1
# answers DONE — the warmup never eats a hold turn, or HOLD_TURNS=1 would hold
# for no time at all. The length is the fixture's
# (SCRIPTED_SLEEP, SCRIPTED_HOLD_TURNS), not the prompt's — the daemon's first
# turn hands the agent a runtime brief rather than the issue body, so a prompt-
# stated command is not something the model can rely on reading.
#
# This is the model side of such a source, nothing more. It is served over the
# network like any deployed endpoint (`TEST_MULTICA_MODEL_BASE_URL` /
# `TEST_MULTICA_MODEL_ID`, dialect `TEST_MULTICA_MODEL_API`), the Job's agent
# resolves it through the same registration as any other endpoint, and the
# cluster facts — claim, Job, pod, terminal projection — stay what they are.
# It does not run the command itself: the agent runs it inside the Job pod.
#
# Protocol: chat completions (`POST /v1/chat/completions`) in the plain and the
# streamed form — the dialect the endpoint registration declares — plus
# `GET /v1/models`. The decision is a function of the request only (how many
# assistant turns the conversation already carries), so a retried request
# decides the same way and nothing is stored.

import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL_ID = os.environ.get("SCRIPTED_MODEL_ID", "fixture:deterministic")
# The scripted window: HOLD_TURNS turns of `sleep SLEEP`, then the final answer.
# The turn count is what a task sees, not the wall clock: omp's shell tool
# returns partial output for a long command, so the next turn arrives while the
# sleep is still running and the task stays in flight across the whole hold.
SLEEP = os.environ.get("SCRIPTED_SLEEP", "60")
HOLD_TURNS = int(os.environ.get("SCRIPTED_HOLD_TURNS", "3"))
# The first turn's command, declared here rather than read from the prompt: a
# task that makes one Multica API call of its own is what moves the agent-api
# passthrough counter (G2 has no other observable face), and the daemon's first
# turn carries a runtime brief instead of the issue body.
WARMUP = os.environ.get("SCRIPTED_WARMUP_COMMAND", "multica issue list --output json")


NOTICE_PREFIX = "<system-notice>"


def is_delivery_notice(message):
    """Whether a user message is a runtime notice, not the task's brief.

    A background job's completion is delivered as a user message whose text
    starts with `<system-notice>`. Those arrive after the model's final answer
    and start another turn; treating the final answer as the task boundary
    would restart the window (warmup, sleeps, answer) on every notice, and the
    task would never settle.
    """
    content = message.get("content")
    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        text = "".join(
            part.get("text", "")
            for part in content
            if isinstance(part, dict) and part.get("type") == "text"
        )
    else:
        text = ""
    return text.lstrip().startswith(NOTICE_PREFIX)


def assistant_turns(request):
    """The current task's assistant turns.

    A resumed session carries the earlier tasks' transcript, so the turns of
    the previous task must not count. The task boundary is the last user
    message that is not a background-job delivery notice: the daemon's first
    turn hands the agent the task brief, and every later user message in one
    task is such a notice. Counting from a final answer instead would restart
    the window whenever a notice arrives after that answer.
    """
    messages = request.get("messages") or []
    start = 0
    for index, message in enumerate(messages):
        if not isinstance(message, dict) or message.get("role") != "user":
            continue
        if is_delivery_notice(message):
            continue
        start = index + 1
    return sum(
        1
        for message in messages[start:]
        if isinstance(message, dict) and message.get("role") == "assistant"
    )


def offered_tools(request):
    names = []
    for tool in request.get("tools") or []:
        function = tool.get("function") if isinstance(tool, dict) else None
        name = (function or tool).get("name") if (function or tool) else None
        if name:
            names.append(name)
    return names


def choose_tool(request):
    names = offered_tools(request)
    for preferred in ("bash", "shell", "exec", "run"):
        if preferred in names:
            return preferred
    return names[0] if names else ""


def tool_parameters(request, name):
    for tool in request.get("tools") or []:
        function = tool.get("function") if isinstance(tool, dict) else tool
        if isinstance(function, dict) and function.get("name") == name:
            return function.get("parameters") or {}
    return {}


def tool_arguments(request, name, command):
    schema = tool_parameters(request, name)
    properties = schema.get("properties") or {}
    arguments = {}
    if "command" in properties or not properties:
        arguments["command"] = command
    if "i" in properties:
        arguments["i"] = "hold the scripted window"
    for key in schema.get("required") or []:
        if key not in arguments:
            arguments[key] = "scripted"
    return arguments


def decision(request):
    if not isinstance(request.get("messages"), list):
        return {"kind": "invalid"}
    tool = choose_tool(request)
    turns = assistant_turns(request)
    if not tool:
        return {"kind": "content", "text": "DONE"}
    if turns == 0 and WARMUP:
        command = WARMUP
    else:
        # The warmup is turn 0, so the sleeps are turns 1..HOLD_TURNS: counting
        # it as a hold turn would shorten the window by one, and HOLD_TURNS=1
        # (the recovery round's 900s window) would hold for no time at all.
        sleeps = turns - (1 if WARMUP else 0)
        if sleeps >= HOLD_TURNS:
            return {"kind": "content", "text": "DONE"}
        command = "sleep " + SLEEP
    return {
        "kind": "call",
        "tool": tool,
        "arguments": tool_arguments(request, tool, command),
    }


def usage():
    return {"prompt_tokens": 11, "completion_tokens": 3, "total_tokens": 14}


def completion(request, decided, stream):
    created = int(time.time())
    if decided["kind"] == "call":
        message = {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call_1",
                    "type": "function",
                    "function": {
                        "name": decided["tool"],
                        "arguments": json.dumps(decided["arguments"]),
                    },
                }
            ],
        }
        finish = "tool_calls"
    else:
        message = {"role": "assistant", "content": decided.get("text", "DONE")}
        finish = "stop"
    base = {
        "id": "chatcmpl-scripted",
        "object": "chat.completion",
        "created": created,
        "model": request.get("model") or MODEL_ID,
    }
    if not stream:
        body = dict(base, choices=[{"index": 0, "message": message, "finish_reason": finish}], usage=usage())
        return json.dumps(body).encode()
    chunks = []
    chunk = dict(base, object="chat.completion.chunk")
    if finish == "tool_calls":
        delta = {
            "role": "assistant",
            "tool_calls": [
                {
                    "index": 0,
                    "id": message["tool_calls"][0]["id"],
                    "type": "function",
                    "function": message["tool_calls"][0]["function"],
                }
            ],
        }
    else:
        delta = {"role": "assistant", "content": message["content"]}
    chunks.append(dict(chunk, choices=[{"index": 0, "delta": delta, "finish_reason": None}]))
    chunks.append(dict(chunk, choices=[{"index": 0, "delta": {}, "finish_reason": finish}], usage=usage()))
    events = "".join("data: " + json.dumps(item) + "\n\n" for item in chunks)
    return (events + "data: [DONE]\n\n").encode()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("scripted-model: %s\n" % (fmt % args))

    def send_body(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_stream(self, body):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/v1/models", "/models"):
            self.send_body(
                200,
                json.dumps(
                    {"object": "list", "data": [{"id": MODEL_ID, "object": "model"}]}
                ).encode(),
                "application/json",
            )
            return
        if path == "/healthz":
            self.send_body(200, b"ok", "text/plain")
            return
        self.send_body(404, b"not found", "text/plain")

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            request = json.loads(raw)
        except ValueError:
            self.send_body(400, b'{"error":"invalid json"}', "application/json")
            return
        decided = decision(request)
        stream = bool(request.get("stream"))
        if decided["kind"] == "invalid":
            self.send_body(400, b'{"error":"not a chat request"}', "application/json")
            return
        sys.stderr.write(
            "scripted-model: turn kind=%s tool=%s stream=%s\n"
            % (decided["kind"], decided.get("tool", ""), stream)
        )
        body = completion(request, decided, stream)
        if stream:
            self.send_stream(body)
            return
        self.send_body(200, body, "application/json")


def serve(address):
    host, _, port = address.rpartition(":")
    host = host or "127.0.0.1"
    server = ThreadingHTTPServer((host, int(port)), Handler)
    sys.stdout.write("listening %s:%d\n" % (host, server.server_address[1]))
    sys.stdout.flush()
    server.serve_forever()


def main(argv):
    if len(argv) < 2:
        sys.stderr.write(__doc__ if __doc__ else "")
        return 2
    action = argv[1]
    if action == "serve":
        serve(argv[2] if len(argv) > 2 else "127.0.0.1:18123")
        return 0
    if action == "decide":
        with open(argv[2], encoding="utf-8") as handle:
            decided = decision(json.load(handle))
        if decided["kind"] == "call":
            print("call " + decided["arguments"].get("command", ""))
        else:
            print("content " + decided.get("text", "DONE"))
        return 0
    sys.stderr.write("unknown action: %s\n" % action)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
