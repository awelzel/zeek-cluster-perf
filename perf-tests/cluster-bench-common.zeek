## Configuration
const test_config = getenv("TEST_CONFIG");

@if ( test_config != "lowrate" && test_config != "highrate" )
event zeek_init() {
	Reporter::fatal(fmt("Invalid test_config '%s'", test_config));
	exit(1);
}
@endif


@if ( test_config == "lowrate" )
const tick_interval = 2 msec;
const publishes_per_tick = 3;
const total_publishes = 20000;
@endif

@if ( test_config == "highrate" )
const tick_interval = 2 msec;
const publishes_per_tick = 30;
const total_publishes = 100000;
@endif

const test_backend = getenv("TEST_BACKEND");

@if ( test_backend != "zeromq" && test_backend != "broker" )
event zeek_init() {
	Reporter::fatal(fmt("Invalid test_backend '%s'", test_backend));
	exit(1);
}
@endif

@if ( test_backend == "zeromq" )
@load frameworks/cluster/backend/zeromq/connect
redef Cluster::Backend::ZeroMQ::proxy_io_threads = 2;
@endif

@if ( test_backend == "broker" )
redef Broker::disable_ssl = T;  # SSL makes stuff slow.
redef Broker::peer_buffer_size = 128*1024;
@endif


module Cluster::Bench;

redef Log::default_rotation_interval = 0.0sec;

export {
	## Proc stats
	type BenchProcStats: record {
		real_time: double;
		user_time: double;
		system_time: double;
		max_rss: count;
		user_system_time: double &optional;
		utilization: double &optional;
	};

	type Result: record {
		success: bool;
		message: string &optional;
	};

	type TestStats: record {
		result: Result &optional;
		proc_stats: BenchProcStats &optional;
	};

	# Stats produced by nodes.
	global node_stats: table[string] of TestStats = table();

	# Event raised by a node when it thinks it's ready.
	global ready: event(name: string, id: string);

	## Raised by the manager to start the test.
	global test_start: event();

	global publish_test_done: function();

	global prepare_test_done: hook(test_stats: TestStats);

	## Send by a node when its test part has completed.
	global test_done: event(name: string, test_stats: TestStats);

	## Broadcasted to all nodes when workers
	## completed their task.
	global test_complete: event();

	## Send to all nodes for clean termination.
	global test_terminate: event();

	## Topic for benchmarking communication.
	const topic = "cluster.bench.all" &redef;

	global stats_tick: hook(now_ts: double, last_tick_ts: double, td: double);

	const stats_tick_interval = 3.0sec &redef;

	global loggers_total = 0;
	global workers_total = 0;
	global proxies_total = 0;
}


# Every node in the cluster waits for node_up from all its neighbors.
#
# This is similar to cluster_started(), but without using the Broker
# specific connection hook.
global nodes_up_pending: set[string] = set();
global nodes_down_pending: set[string] = set();
global nodes_ready_pending: set[string] = set();

# For now, workers and proxies.
global nodes_test_done_pending: set[string] = set();

# This is applicable for both, Broker and ZeroMQ in this environment.
#
# Well, it really is: Wait for everyone except nodes of the same type.
global wait_for_map: table[Cluster::NodeType] of set[Cluster::NodeType] = {
	[Cluster::WORKER] = set(Cluster::MANAGER, Cluster::LOGGER, Cluster::PROXY),
	[Cluster::PROXY] = set(Cluster::MANAGER, Cluster::LOGGER, Cluster::WORKER),
	[Cluster::MANAGER] = set(Cluster::LOGGER, Cluster::PROXY, Cluster::WORKER),
	[Cluster::LOGGER] = set(Cluster::MANAGER, Cluster::PROXY, Cluster::WORKER),

};

event zeek_init() &priority=5 {
	Cluster::subscribe(topic);

	local wait_for = wait_for_map[Cluster::local_node_type()];

	for ( name, n in Cluster::nodes ) {

		if ( n$node_type == Cluster::LOGGER )
			++loggers_total;

		if ( n$node_type == Cluster::PROXY )
			++proxies_total;

		if ( n$node_type == Cluster::WORKER )
			++workers_total;

		# Don't wait for ourselves
		if ( name == Cluster::node )
			next;

		local other_node = Cluster::nodes[name];

		if (other_node$node_type !in wait_for )
			next;

		add nodes_down_pending[name];

		add nodes_up_pending[name];

		add nodes_test_done_pending[name];
	}

	# At least for the manager this is correct.
	nodes_ready_pending = copy(nodes_up_pending);
	# print fmt("going to wait for: %s", join_string_set(nodes_up_pending, ","));
}

global ready_sent = F;

event Cluster::node_up(name: string, id: string) {

	delete nodes_up_pending[name];

	if ( ! ready_sent && |nodes_up_pending| == 0 ) {
		Cluster::publish(topic, Cluster::Bench::ready, Cluster::node, Cluster::node_id());
		ready_sent = T;
	}

}

global test_started = F;

event Cluster::Bench::ready(name: string, id: string) {

	# The manager waits for a ready event from all other nodes.
	if ( Cluster::local_node_type() != Cluster::MANAGER )
		return;


	if ( name ! in nodes_ready_pending )
		Reporter::error(fmt("Node '%s' send ready() twice?", name));

	# print fmt("Node '%s' is ready", name);
	delete nodes_ready_pending[name];

	if ( ! test_started && |nodes_ready_pending| == 0 ) {
		print "All nodes ready, go go go!";
		Cluster::publish(topic, Cluster::Bench::test_start);

		# Prepare locally, too.
		event Cluster::Bench::test_start();
		test_started = T;
	}
}

event Cluster::node_down(name: string, id: string) {
	if ( Cluster::local_node_type() != Cluster::MANAGER )
		return;

	delete nodes_down_pending[name];

	if ( |nodes_down_pending| == 0 )
		terminate();
}

event Cluster::Bench::test_terminate() {
	if ( Cluster::local_node_type() != Cluster::MANAGER )
		terminate();
}

global did_publish_test_complete = F;

event Cluster::Bench::test_done(name: string, stats: TestStats) {
	if ( Cluster::local_node_type() != Cluster::MANAGER )
		return;

	if ( name !in nodes_test_done_pending ) {
		Reporter::error(fmt("Node '%s' sent test_done twice", name));
		return;
	}

	delete nodes_test_done_pending[name];

	node_stats[name] = stats;

	if ( ! did_publish_test_complete && |nodes_test_done_pending| == 0 ) {
		Cluster::publish(topic, Cluster::Bench::test_complete);
		did_publish_test_complete = T;
	}

	if ( |nodes_test_done_pending| == 0 ) {

		event Cluster::Bench::test_terminate();
		Cluster::publish(topic, Cluster::Bench::test_terminate);
	}
}

global last_tick_ts = time_to_double(current_time());

# Stats tick can be used by test to output status information.
event do_stats_tick() {
	if ( zeek_is_terminating() )
		return;

	local now_ts = time_to_double(current_time());
	local td = now_ts - last_tick_ts;

	hook stats_tick(now_ts, last_tick_ts, td);

	last_tick_ts = now_ts;

	schedule stats_tick_interval { do_stats_tick() };
}

global proc_stats_start: ProcStats;
event Cluster::Bench::test_start() {
	proc_stats_start = get_proc_stats();

	schedule stats_tick_interval { do_stats_tick() };
}

function diff_bench_proc_stats(end: ProcStats, start: ProcStats): BenchProcStats {
	local r = BenchProcStats(
		$real_time=interval_to_double(end$real_time - start$real_time),
		$user_time=interval_to_double(end$user_time - start$user_time),
		$system_time=interval_to_double(end$system_time - start$system_time),
		$max_rss=end$mem,
	);

	r$user_system_time = r$user_time + r$system_time;
	r$utilization = r$user_system_time / r$real_time;

	return r;
}


event Cluster::Bench::test_terminate() &priority=5 {
	if ( Cluster::local_node_type() != Cluster::MANAGER )
		return;

	local bps = diff_bench_proc_stats(get_proc_stats(), proc_stats_start);

	node_stats[Cluster::node] = TestStats();
	node_stats[Cluster::node]$proc_stats = bps;
}

global did_publish_test_done = F;

function Cluster::Bench::publish_test_done() {

	if ( did_publish_test_done )
		return;

	local stats = TestStats();

	hook Cluster::Bench::prepare_test_done(stats);

	Cluster::publish(topic, Cluster::Bench::test_done, Cluster::node, stats);

	did_publish_test_done = T;
}

hook Cluster::Bench::prepare_test_done(stats: TestStats) {
	local bench_proc_stats = diff_bench_proc_stats(get_proc_stats(), proc_stats_start);
	stats$proc_stats = bench_proc_stats;
}

event zeek_init() &priority=1000 {
	# Disable stdout buffering.
	local f = open("-");
	set_buf(f, F);
}

type JsonResult: record {
	node: string;
	node_type: Cluster::NodeType;
	stats: TestStats;
};

event zeek_done() {
	if ( Cluster::local_node_type() != Cluster::MANAGER )
		return;


	# The manager outputs all the information from other nodes and
	# outputs it in some JSON format.

	local user_by_type: table[Cluster::NodeType] of double &default=0.0;
	local system_by_type: table[Cluster::NodeType] of double &default=0.0;
	local user_system_by_type: table[Cluster::NodeType] of double &default=0.0;
	local max_rss_sum_by_type: table[Cluster::NodeType] of count &default=0;

	for ( name, stats in node_stats ) {
		local nt = Cluster::nodes[name]$node_type;
		print fmt("JSON_RESULT=%s", to_json(JsonResult($node=name, $node_type=nt, $stats=stats)));
		user_system_by_type[nt] += stats$proc_stats$user_system_time;
		user_by_type[nt] += stats$proc_stats$user_time;
		system_by_type[nt] += stats$proc_stats$system_time;
		max_rss_sum_by_type[nt] += stats$proc_stats$max_rss;
	}

	local total_user_system_sec = 0.0;


	for ( typ, user_system_sec in user_system_by_type ) {
		print fmt("SUMMARY [%s] user_system=%.2fs max_rss_sum=%.1f MB",
		          typ, user_system_sec, max_rss_sum_by_type[typ] / 1024.0 / 1024.0);
		total_user_system_sec += user_system_sec;
	}

	local summary = "";

	local mgr_stats = node_stats[Cluster::node];
	if ( mgr_stats?$result )
		summary += cat(mgr_stats$result);
	else
		summary += " <no result>";

	summary += fmt(" duration=%.2fs total_user_system=%.2fs",
	               mgr_stats$proc_stats$real_time, total_user_system_sec);
	print fmt("SUMMARY %s", summary);
}

module GLOBAL;

const dummy_cid = conn_id($orig_h=127.0.0.1, $orig_p=1234/tcp, $resp_h=127.0.0.2, $resp_p=80/tcp, $proto=6);
const dummy_cid2 = conn_id($orig_h=10.0.0.1, $orig_p=12345/udp, $resp_h=10.0.0.2, $resp_p=53/tcp, $proto=17);
