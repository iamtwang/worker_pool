%%%-------------------------------------------------------------------
%%% @author Tao
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Aug 2016 21:48
%%%-------------------------------------------------------------------
-module(worker_pool).
-export([
  init/0,
  add_job/2
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

  register(?MODULE, spawn(fun() -> controller() end)).

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
  ets:insert(?POOL, {{erlang:monotonic_time(seconds), Pid}, []}),
  Pid;
insert_job(_Jobs, _Max) ->
  First = ets:first(?POOL),
  {_, Key} = First,
  exit(Key, cancelled),
  ets:delete(?POOL, First),
  {Pid, _} = spawn_monitor(fun() -> worker() end),
  ets:insert(?POOL, {{erlang:monotonic_time(seconds), Pid}, []}),
  Pid.

%% delete job from the pool if it was finished.
delete_job(Pid) ->
  ets:match_delete(?POOL, {{'_', Pid}, '_'}).
