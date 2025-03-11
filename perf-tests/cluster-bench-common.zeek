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
const total_publishes = 100000;
# const total_publishes = 20000;
@endif

@if ( test_config == "highrate" )
const tick_interval = 2 msec;
const publishes_per_tick = 30;
const total_publishes = 100000;
# const total_publishes = 20000;
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

	global workers_total = 0;
	global proxies_total = 0;
}

global nodes_up_pending: set[string] = set();
global nodes_down_pending: set[string] = set();
global nodes_test_done_pending: set[string] = set();

global workers_done = 0;

global test_started = F;

event zeek_init() {
	Cluster::subscribe(topic);

	for ( name, n in Cluster::nodes ) {
		if ( name == Cluster::node )
			next;

		add nodes_up_pending[name];
		add nodes_down_pending[name];

		if ( n$node_type == Cluster::WORKER || n$node_type == Cluster::PROXY ) {
			add nodes_test_done_pending[name];
		}

		if ( n$node_type == Cluster::WORKER )
			++workers_total;

		if ( n$node_type == Cluster::PROXY )
			++proxies_total;
	}
}

event Cluster::node_up(name: string, id: string) {

	if ( Cluster::local_node_type() != Cluster::MANAGER )
		return;

	delete nodes_up_pending[name];

	if ( ! test_started && |nodes_up_pending| == 0 ) {
		print "GO GO GO";
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

	delete nodes_test_done_pending[name];

	node_stats[name] = stats;

	if ( Cluster::nodes[name]$node_type == Cluster::WORKER )
		++workers_done;

	if ( ! did_publish_test_complete && workers_done == workers_total ) {
		Cluster::publish(topic, Cluster::Bench::test_complete);
		did_publish_test_complete = T;
	}

	if ( |nodes_test_done_pending| == 0 ) {

		event Cluster::Bench::test_terminate();
		Cluster::publish(topic, Cluster::Bench::test_terminate);
	}
}

#
global proc_stats_start: ProcStats;

event Cluster::Bench::test_start() {
	proc_stats_start = get_proc_stats();
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


#
#
#
global last_tick_ts = time_to_double(current_time());

event do_stats_tick() {
	if ( zeek_is_terminating() )
		return;

	local now_ts = time_to_double(current_time());
	local td = now_ts - last_tick_ts;

	hook stats_tick(now_ts, last_tick_ts, td);

	last_tick_ts = now_ts;

	schedule stats_tick_interval { do_stats_tick() };
}

event zeek_init() {
	# Disable stdout buffering.
	local f = open("-");
	set_buf(f, F);

	event do_stats_tick();
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
