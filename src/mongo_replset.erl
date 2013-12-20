%@doc Get connection to appropriate server in a replica set
-module (mongo_replset).

-export_type ([replset/0, rs_connection/0]).
-export ([connect/1, connect/2, ssl_connect/1, ssl_connect/2, ssl_connect/3, primary/1, secondary_ok/1, close/1, is_closed/1]). % API

-type maybe(A) :: {A} | {}.
-type err_or(A) :: {ok, A} | {error, reason()}.
-type reason() :: any().

% -spec find (fun ((A) -> boolean()), [A]) -> maybe(A).
% return first element in list that satisfies predicate
% find (Pred, []) -> {};
% find (Pred, [A | Tail]) -> case Pred (A) of true -> {A}; false -> find (Pred, Tail) end.

-spec until_success ([A], fun ((A) -> B)) -> B. % EIO, fun EIO
%@doc Apply fun on each element until one doesn't fail. Fail if all fail or list is empty
until_success ([], _Fun) -> throw ([]);
until_success ([A | Tail], Fun) -> try Fun (A)
	catch Reason -> try until_success (Tail, Fun)
		catch Reasons -> throw ([Reason | Reasons]) end end.

-spec rotate (integer(), [A]) -> [A].
%@doc Move first N element of list to back of list
rotate (N, List) ->
	{Front, Back} = lists:split (N, List),
	Back ++ Front.

-type host() :: mongo_connect:host().
-type connection() :: mongo_connect:connection().

-type replset() :: {rs_name(), [host()]}.
% Identify replset. Hosts is just seed list, not necessarily all hosts in replica set
-type rs_name() :: bson:utf8().

-spec connect (replset()) -> rs_connection(). % IO
%@doc Create new cache of connections to replica set members starting with seed members. No connection attempted until primary or secondary_ok called.
connect (ReplSet) -> connect (ReplSet, infinity).

-spec connect (replset(), timeout()) -> rs_connection(). % IO
%@doc Create new cache of connections to replica set members starting with seed members. No connection attempted until primary or secondary_ok called. Timeout used for initial connection and every call.
connect ({ReplName, Hosts}, TimeoutMS) ->
	connect({ReplName, Hosts, undefined, undefined, undefined}, TimeoutMS);
connect ({ReplName, Hosts, DB, User, Pass}, TimeoutMS) ->
	List = lists:map (fun (Host) -> {mongo_connect:host_port (Host), {undefined, DB, User, Pass}} end, Hosts),
	Dict = dict:from_list (List),
	{rs_connection, ReplName, mvar:new (Dict), TimeoutMS, false}.

-spec ssl_connect(replset()) -> rs_connection().
%@doc Create new cache of connections with SSL support.
ssl_connect(ReplSet) -> ssl_connect(ReplSet, infinity, []).

-spec ssl_connect(replset(), timeout()) -> rs_connection().
%@doc Create new cache of connections with SSL support.
ssl_connect(ReplSet, TimeoutMS) -> ssl_connect(ReplSet, TimeoutMS, []).

-spec ssl_connect(replset(), timeout(), ssl:ssloption()) -> rs_connection().
%@doc Create new cache of connections with SSL support.
ssl_connect({ReplName, Hosts}, TimeoutMS, SslOptions) ->
	Dict = dict:from_list (lists:map (fun (Host) -> {mongo_connect:host_port (Host), {}} end, Hosts)),
	{rs_connection, ReplName, mvar:new (Dict), TimeoutMS, SslOptions}.

%% Note(superbobry): having an 'opaque' type here causes 'dialyzer' to
%% crash (bug reported).
-type rs_connection() :: {rs_connection, rs_name(), mvar:mvar(connections()), timeout(), maybe(list())}.
% Maintains set of connections to some if not all of the replica set members. Opaque except to mongo:connect_mode
% Type not opaque to mongo:connection_mode/2
-type connections() :: dict:dictionary (host(), maybe(connection())).
% All hosts listed in last member_info fetched are keys in dict. Value is {} if no attempt to connect to that host yet

-spec primary (rs_connection()) -> err_or(connection()). % IO
%@doc Return connection to current primary in replica set
primary (ReplConn) -> try
		MemberInfo = fetch_member_info (ReplConn),
		primary_conn (2, ReplConn, MemberInfo)
	of Conn -> {ok, Conn}
	catch Reason -> {error, Reason} end.

-spec secondary_ok (rs_connection()) -> err_or(connection()). % IO
%@doc Return connection to a current secondary in replica set or primary if none
secondary_ok (ReplConn) -> try
		{_Conn, Info} = fetch_member_info (ReplConn),
		Hosts = lists:map (fun mongo_connect:read_host/1, bson:at (hosts, Info)),
		R = random:uniform (length (Hosts)) - 1,
		secondary_ok_conn (ReplConn, rotate (R, Hosts))
	of Conn -> {ok, Conn}
	catch Reason -> {error, Reason} end.

-spec close (rs_connection()) -> ok. % IO
%@doc Close replset connection
close ({rs_connection, _, VConns, _, _}) ->
	CloseConn = fun (_, MCon, _) ->
		case MCon of
			{undefined, _DB, _User, _Pass} -> ok;
			{Con, _DB, _User, _Pass} -> mongo_connect:close (Con)
		end
	end,
	mvar:with (VConns, fun (Dict) -> dict:fold (CloseConn, ok, Dict) end),
	mvar:terminate (VConns).

-spec is_closed (rs_connection()) -> boolean(). % IO
%@doc Has replset connection been closed?
is_closed ({rs_connection, _, VConns, _, _}) -> mvar:is_terminated (VConns).

% EIO = IO that may throw error of any type

-type member_info() :: {connection(), bson:document()}.
% Result of isMaster query on a server connnection. Returned fields are: setName, ismaster, secondary, hosts, [primary]. primary only present when ismaster = false

-spec primary_conn (integer(), rs_connection(), member_info()) -> connection(). % EIO
%@doc Return connection to primary designated in member_info. Only chase primary pointer N times.
primary_conn (0, _ReplConn, MemberInfo) -> throw ({false_primary, MemberInfo});
primary_conn (Tries, ReplConn, {Conn, Info}) -> case bson:at (ismaster, Info) of
	true -> Conn;
	false -> case bson:lookup (primary, Info) of
		{HostString} ->
			MemberInfo = connect_member (ReplConn, mongo_connect:read_host (HostString)),
			primary_conn (Tries - 1, ReplConn, MemberInfo);
		{} -> throw ({no_primary, {Conn, Info}}) end end.

-spec secondary_ok_conn (rs_connection(), [host()]) -> connection(). % EIO
%@doc Return connection to a live secondaries in replica set, or primary if none
secondary_ok_conn (ReplConn, Hosts) -> try
		until_success (Hosts, fun (Host) ->
			{Conn, Info} = connect_member (ReplConn, Host),
			case bson:at (secondary, Info) of true -> Conn; false -> throw (not_secondary) end end)
	catch _ -> primary_conn (2, ReplConn, fetch_member_info (ReplConn)) end.

-spec fetch_member_info (rs_connection()) -> member_info(). % EIO
%@doc Retrieve isMaster info from a current known member in replica set. Update known list of members from fetched info.
fetch_member_info (ReplConn = {rs_connection, _ReplName, VConns, _, _}) ->
	Dict0 = mvar:read (VConns),
	{DB, User, Pass} = get_creds(Dict0),
	OldHosts_ = dict:fetch_keys (Dict0),
	{Conn, Info} = until_success (OldHosts_, fun (Host) -> connect_member (ReplConn, Host) end),
	OldHosts = sets:from_list (OldHosts_),
	NewHosts = sets:from_list (lists:map (fun mongo_connect:read_host/1, bson:at (hosts, Info))),
	RemovedHosts = sets:subtract (OldHosts, NewHosts),
	AddedHosts = sets:subtract (NewHosts, OldHosts),
	mvar:modify_ (VConns, fun (Dict) ->
		Dict1 = sets:fold (fun remove_host/2, Dict, RemovedHosts),
		{Dict2, _, _, _} = sets:fold (fun add_host/2, {Dict1, DB, User, Pass}, AddedHosts),
		Dict2 end),
	case sets:is_element (mongo_connect:conn_host (Conn), RemovedHosts) of
		false -> {Conn, Info};
		true -> % Conn connected to member but under wrong name (eg. localhost instead of 127.0.0.1) so it was closed and removed because it did not match a host in isMaster info. Reconnect using correct name.
			Hosts = dict:fetch_keys (mvar:read (VConns)),
			until_success (Hosts, fun (Host) -> connect_member (ReplConn, Host) end) end.

get_creds(Dict) ->
	get_creds(dict:fetch_keys(Dict), Dict).
get_creds([], _Dict) ->
	{undefined, undefined, undefined};
get_creds([Key | _], Dict) ->
	{_, DB, User, Pass} = dict:fetch(Key, Dict),
	{DB, User, Pass}.
add_host (Host, {Dict, DB, User, Pass}) ->
	Dict1 = dict:store (Host, {undefined, DB, User, Pass}, Dict),
	{Dict1, DB, User, Pass}.

remove_host (Host, Dict) ->
	case dict:is_key(Host, Dict) of
		true ->
			MConn = dict:fetch (Host, Dict),
			Dict1 = dict:erase (Host, Dict),
			case MConn of
				{undefined, _DB, _User, _Pass} -> ok;
				{Conn, _DB, _User, _Pass} -> mongo_connect:close (Conn)
			end,
			Dict1;
		_ ->
			Dict
	end.

-spec connect_member (rs_connection(), host()) -> member_info(). % EIO
%@doc Connect to host and verify membership. Cache connection in rs_connection
connect_member ({rs_connection, ReplName, VConns, TimeoutMS, SslOptions}, Host) ->
	Conn = get_connection (VConns, Host, TimeoutMS, SslOptions),
	Info = try get_member_info (Conn) catch _ ->
		mongo_connect:close (Conn),
		Conn1 = get_connection (VConns, Host, TimeoutMS, SslOptions),
		get_member_info (Conn1) end,
	case bson:at (setName, Info) of
		ReplName -> {Conn, Info};
		_ ->
			mongo_connect:close (Conn),
			throw ({not_member, ReplName, Host, Info}) end.

get_connection (VConns, Host, TimeoutMS, SslOptions) ->
	mvar:modify (VConns, fun (Dict) ->
		case dict:find (Host, Dict) of
			{ok, {undefined, DB, User, Pass}} ->
				new_connection (Dict, Host, TimeoutMS, SslOptions, DB, User, Pass);
			{ok, {Conn, DB, User, Pass}} ->
				case mongo_connect:is_closed (Conn) of
					false -> {Dict, Conn};
					true -> new_connection (Dict, Host, TimeoutMS, SslOptions, DB, User, Pass)
				end
		end
	end).

new_connection (Dict, Host, TimeoutMS, false, DB, User, Pass) ->
	case mongo_connect:connect (Host, TimeoutMS) of
		{ok, Conn} -> try_auth(Host, Conn, Dict, DB, User, Pass);
		{error, Reason} -> throw ({cant_connect, Reason})
	end;
new_connection (Dict, Host, TimeoutMS, SslOptions, DB, User, Pass) ->
	case mongo_connect:ssl_connect(Host, TimeoutMS, SslOptions) of
		{ok, Conn} -> try_auth(Host, Conn, Dict, DB, User, Pass);
		{error, Reason} -> throw ({cant_connect, Reason})
	end.

try_auth(Host, Conn, Dict, DB, undefined, undefined) ->
	{dict:store (Host, {Conn, DB, undefined, undefined}, Dict), Conn};
try_auth(Host, Conn, Dict, DB, User, Pass) ->
	Nonce = bson:at(nonce, command({getnonce, 1}, {DB, Conn})),
	Hash = pw_key(Nonce, User, Pass),
	AuthCommand = {authenticate, 1, user, User, nonce, Nonce, key, Hash},
	try command(AuthCommand, {DB, Conn}) of
		_AuthResult ->
			{dict:store (Host, {Conn, DB, User, Pass}, Dict), Conn}
	catch
		_:Error -> throw({auth_failed, Error})
	end.

command(Command, Conn) ->
	mongo_query:command(Conn, Command, true).

pw_key (Nonce, Username, Password) ->
	bson:utf8 (binary_to_hexstr (crypto:md5 ([Nonce, Username, pw_hash (Username, Password)]))).

pw_hash (Username, Password) ->
	bson:utf8 (binary_to_hexstr (crypto:md5 ([Username, <<":mongo:">>, Password]))).

binary_to_hexstr (Bin) ->
	lists:flatten ([io_lib:format ("~2.16.0b", [X]) || X <- binary_to_list (Bin)]).

get_member_info (Conn) -> mongo_query:command ({admin, Conn}, {isMaster, 1}, true).
