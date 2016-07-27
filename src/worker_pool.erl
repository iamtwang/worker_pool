%%%-------------------------------------------------------------------
%%% @author Tao
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% This is an example project.
%%% http://codereview.stackexchange.com/questions/135767/erlang-process-pool
%%% @end
%%% Created : 21. Jul 2016 22:23
%%%-------------------------------------------------------------------
-module(worker_pool).
-export([
  init/0,
  add_job/2,
  worker/0,
  controller/0,
  insert_job/0,
  delete_job/1

]).

-define(TIMEOUT, 10000).
-define(POOL, pool).
-define(MAX, 100).

%% ----------------------------------------------------------------------
%% @doc
%% init the server
%% @end
%% ----------------------------------------------------------------------
init() ->
  ets:new(?POOL, [
    public,
    named_table,
    ordered_set,
    {write_concurrency, true},
    {read_concurrency, true}]),
  Pid = spawn(?MODULE, controller, []),
  register(?MODULE, Pid).

%% ----------------------------------------------------------------------
%% @doc
%% add jobs to pool by sending message.
%% @end
%% ----------------------------------------------------------------------
add_job(Fun, Value) ->
  ?MODULE ! {new_worker, Fun, Value}.

%% ----------------------------------------------------------------------
%% @doc
%% running the jobs. triggered by message.
%% @end
%% ----------------------------------------------------------------------
worker() ->
  receive
    {do_work, Fun, Value} ->
      Fun(Value);
    _ ->
      exit(no_valid)
  after ?TIMEOUT ->
    io:format("I (worker ~p) will die now ...~n", [self()]),
    exit(no_activity)
  end.

%% ----------------------------------------------------------------------
%% @doc
%% create a process tool and trigger it by message.
%% @end
%% ----------------------------------------------------------------------
controller() ->
  receive
    {new_worker, Fun, Value} ->
      Pid = insert_job(),
      Pid ! {do_work, Fun, Value},
      controller();
    {'DOWN', _Ref, process, Pid, Reason} ->
      io:format(" worker ~p died (~p)~n", [Pid, Reason]),
      delete_job(Pid),
      controller();
    Msg ->
      io:format(" Controller ~p ~n", [Msg]),
      controller()
  end.

%% ------------------------------------------------------------------------
%% internal functions.

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
