#!/bin/bash

SERVER_SESSION='dtao-server'
PROXY_SESSION='dtao-proxy'
CLIENT_SESSION='dtao-client'

declare -A pane_map

# send a command to a pane in a session and create them if necessary
_tmux_process_command() {
  local pane term cmd

  session_type=$1
  pane=$2
  term=$3
  cmd=$4

  # create the session if it doesn't exist
  if ! tmux has-session -t "$session_type" > /dev/null 2>&1; then
    pane_map[$session_type.$pane]=$(tmux new-session -d -s "$session_type" -n 0 -P -F "#{pane_id}")
    sleep 0.5
  fi

  # create the pane if it doesn't exist
  if ! [[ -v "pane_map[$session_type.$pane]" ]]; then
    pane_map[$session.$pane]=$(tmux split-window -h -t "$session_type:0.0" -P -F "#{pane_id}")
    tmux select-layout even-horizontal
    # give me something to ctrl-c or ctrl-d out of
    tmux send-keys -t "$session_type:0.${pane_map[$session_type.$pane]}" 'cat' 'C-m'
    sleep 0.1
  fi

  pane_id=${pane_map[$session_type.$pane]}

  tmux send-keys -t "$session_type:0.$pane_id" "$term"
  sleep 0.02
  tmux send-keys -t "$session_type:0.$pane_id" "$cmd" 'C-m'
}

set -e

#if [[ $* == *-k* ]]; then
tmux kill-session -t $SERVER_SESSION > /dev/null 2>&1 || true
tmux kill-session -t $PROXY_SESSION > /dev/null 2>&1 || true
tmux kill-session -t $CLIENT_SESSION > /dev/null 2>&1 || true
#fi

(cd target &&

# launch servers
_tmux_process_command $SERVER_SESSION 0 'C-c' "java -cp ../lib/commons-math3-3.6.1.jar:../lib/guava-19.0.jar:TaoServer-1.0-SNAPSHOT.jar TaoServer.TaoServer --unit 0"
_tmux_process_command $SERVER_SESSION 1 'C-c' "java -cp ../lib/commons-math3-3.6.1.jar:../lib/guava-19.0.jar:TaoServer-1.0-SNAPSHOT.jar TaoServer.TaoServer --unit 1"
_tmux_process_command $SERVER_SESSION 2 'C-c' "java -cp ../lib/commons-math3-3.6.1.jar:../lib/guava-19.0.jar:TaoServer-1.0-SNAPSHOT.jar TaoServer.TaoServer --unit 2"

echo 'Servers launched.'

# launch proxies
_tmux_process_command $PROXY_SESSION 0 'C-c' "java -cp ../lib/commons-math3-3.6.1.jar:../lib/guava-19.0.jar:TaoServer-1.0-SNAPSHOT.jar TaoProxy.TaoProxy --unit 0"
_tmux_process_command $PROXY_SESSION 1 'C-c' "java -cp ../lib/commons-math3-3.6.1.jar:../lib/guava-19.0.jar:TaoServer-1.0-SNAPSHOT.jar TaoProxy.TaoProxy --unit 1"
_tmux_process_command $PROXY_SESSION 2 'C-c' "java -cp ../lib/commons-math3-3.6.1.jar:../lib/guava-19.0.jar:TaoServer-1.0-SNAPSHOT.jar TaoProxy.TaoProxy --unit 2"

echo 'Proxies launched.'

# launch client
num_clients=10
num_warmup=500
loadtest_length=$((10*60*1000))

_tmux_process_command $CLIENT_SESSION 0 'C-c' "java -cp ../lib/commons-math3-3.6.1.jar:../lib/guava-19.0.jar:TaoServer-1.0-SNAPSHOT.jar TaoClient.TaoClient --runType load_test --clients $num_clients --warmup_operations $num_warmup --load_test_length $loadtest_length"

echo 'Client launched.')
