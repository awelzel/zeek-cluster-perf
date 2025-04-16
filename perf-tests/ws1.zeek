# The manager opens a WebSocket port and workers connect with Python and piped-exec.
@load cluster-bench-common
@load ws-common

redef record Cluster::Bench::TestStats += {
	received: count &optional;
	per_worker: table[string] of count &optional;
};

global ws_topic = "cluster.bench.websocket.ws1";

event zeek_init()
	{
	total_publishes = 10000;
	}

##############
### WORKER ###
##############
@if ( Cluster::local_node_type() == Cluster::WORKER )
global total = 0;

event Cluster::Bench::test_start() {
	local test_tool_dir = getenv("TEST_TOOL_DIR");
	if ( |test_tool_dir| == 0 )
		test_tool_dir = ".";

	local cmd = fmt("%s/ws1.py --total-publishes %s --url %s --topic %s --name %s", test_tool_dir, total_publishes, Cluster::Bench::WebSocket::url, ws_topic, Cluster::node);

	piped_exec(cmd, "");
	print "DONE";
	Cluster::Bench::publish_test_done();
}

global last_total = 0;
hook Cluster::Bench::stats_tick(now_ts: double, last_ts: double, td: double) {
	local diff = total - last_total;
	local per_second = diff / td;

	print fmt("publishes per second: %.3f (%s / %s) total %s", per_second, diff, td, total);
	last_total = total;
}
@endif

#############
### PROXY ###
#############
@if ( Cluster::local_node_type() == Cluster::PROXY )
global workers_test_done_seen = 0;
global last_tick_received = 0;
global received = 0;
global per_worker: table[string] of count &default=0;
global worker_ws_bye: set[string];

event zeek_init()
	{
	Cluster::subscribe(ws_topic);
	}

event Cluster::Bench::WebSocket::ping(i: count, who: string) {
	++per_worker[who];
	++received;
}

function maybe_done() {
	if ( Cluster::Bench::test_started && |worker_ws_bye| == Cluster::Bench::workers_total && workers_test_done_seen == Cluster::Bench::workers_total )
		Cluster::Bench::publish_test_done();
}

event Cluster::Bench::test_done(name: string, stats: Cluster::Bench::TestStats) {
	if (  Cluster::nodes[name]$node_type == Cluster::WORKER )
		++workers_test_done_seen;

	maybe_done();
}

event Cluster::Bench::WebSocket::bye(who: string) {
	print "BYE", who;
	add worker_ws_bye[who];

	maybe_done();
}

event Cluster::Bench::test_start() {
	# We might have seen the workers done even before starting.
	maybe_done();
}

hook Cluster::Bench::prepare_test_done(stats: Cluster::Bench::TestStats) {
	stats$received = received;
	stats$per_worker = per_worker;
}

hook Cluster::Bench::stats_tick(now_ts: double, last_ts: double, td: double) {
	local diff = received - last_tick_received;
	local per_second = diff / td;

	print fmt("events per second: %.3f (%s / %s) total %s", per_second, diff, td, received);
	last_tick_received = received;
}
@endif


##############
### LOGGER ###
##############
@if ( Cluster::local_node_type() == Cluster::LOGGER)
global workers_test_done_seen = 0;

function maybe_done() {
	if ( Cluster::Bench::test_started && workers_test_done_seen == Cluster::Bench::workers_total )
		Cluster::Bench::publish_test_done();
}

event Cluster::Bench::test_start() {
	# We might have seen the workers done even before starting.
	maybe_done();
}

event Cluster::Bench::test_done(name: string, stats: Cluster::Bench::TestStats) {
	if (  Cluster::nodes[name]$node_type == Cluster::WORKER )
		++workers_test_done_seen;

	maybe_done();
}
@endif

###############
### MANAGER ###
###############
@if ( Cluster::local_node_type() == Cluster::MANAGER )
global worker_counts: table[string] of count &default=0;
event zeek_init() {
	Cluster::listen_websocket([
		$listen_host=Cluster::Bench::WebSocket::listen_host,
		$listen_port=Cluster::Bench::WebSocket::listen_port,
	]);
}

event Cluster::Bench::test_terminate() {
	# Sum up all received values.
	local sum_received = 0;
	for ( name, stats in Cluster::Bench::node_stats ) {
		if (stats?$received ) {
			print name, stats$received, stats$per_worker;
			sum_received += stats$received;
		}

	}

	local sum_expected = total_publishes * Cluster::Bench::workers_total * Cluster::Bench::proxies_total;

	local success = sum_received == sum_expected;
	local msg = fmt("Received %s, expected %s", sum_received, sum_expected);

	local result = Cluster::Bench::Result($success=success, $message=msg);
	Cluster::Bench::node_stats[Cluster::node]$result = result;
}

event zeek_init()
	{
	Cluster::subscribe(ws_topic);
	}

global received = 0;

event Cluster::Bench::WebSocket::ping(i: count, who: string) {
	local p = worker_counts[who];
	if ( p != i ) {
		print "ERROR ERROR", i, p, who;
	}

	++worker_counts[who];
}
@endif
