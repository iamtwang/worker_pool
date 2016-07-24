%%%-------------------------------------------------------------------
%%% @author Tao
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% This is an example project.
%%% @end
%%% Created : 21. Jul 2016 22:23
%%%-------------------------------------------------------------------
-module(worker_pool).
-export([
  init/0,
  add_job/1,
  worker/0,
  controller/0

]).

-define(TIMEOUT, 6000).
-define(POOL, pool).
-define(MAX, 3).

%% init the server
init() ->
  ets:new(?POOL, [public, named_table, ordered_set]),
  Pid = spawn(?MODULE, controller, []),
  register(?MODULE, Pid).

%% add job to pool by sending message.
add_job(Value) ->
  ?MODULE ! {new_worker, Value}.

%% trigger a process.
worker() ->
  receive
    {do_work, Value} ->
      example(Value);
    _ ->
      exit(no_valid)
  after ?TIMEOUT ->
    io:format("I (worker ~p) will die now ...~n", [self()]),
    exit(no_activity)
  end.

%% create pool of process and trigger it by message.
controller() ->
  receive
    {new_worker, Value} ->
      Pid = insert_job(),
      Pid ! {do_work, Value},
      controller();
    {'DOWN', _Ref, process, Pid, Reason} ->
      io:format(" worker ~p died (~p)~n", [Pid, Reason]),
      delete_job(Pid),
      controller();
    Msg ->
      io:format(" Controller ~p ~n", [Msg]),
      controller()
  end.


%% internal functions.
example(Value) ->
  timer:sleep(2000),
  io:format("sleep [~p] done ~n", [Value]).

%% insert to ets table, ordered by time.
insert_job() ->
  Jobs = ets:info(?POOL, size),
  insert_job(Jobs, ?MAX).
insert_job(Jobs, Max) when Jobs < Max ->
  {Pid, _} = spawn_monitor(fun() -> worker() end),
  ets:insert(?POOL, {{erlang:monotonic_time(seconds), pid_to_list(Pid)}, []}),
  Pid;
insert_job(_Jobs, _Max) ->
  First = ets:first(?POOL),
  {_, Key} = First,
  exit(list_to_pid(Key), cancelled),
  ets:delete(?POOL, First),
  {Pid, _} = spawn_monitor(fun() -> worker() end),
  ets:insert(?POOL, {{erlang:monotonic_time(seconds), pid_to_list(Pid)}, []}),
  Pid.

%% delete job from the pool if it was finished.
delete_job(Pid) ->
  Pid_list = pid_to_list(Pid),
  ets:match_delete(?POOL, {{'_', Pid_list}, '_'}).