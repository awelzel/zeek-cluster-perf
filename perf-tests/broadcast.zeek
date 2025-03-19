# All workers publish via publish() to all proxies at a fixed rate to proxies.
@load cluster-bench-common

module Scan;

export {
	type conn_info: record {
		cid: conn_id;
		ts: time;
	};

	global potential_scanner: event(ci: conn_info, established: bool, reverse: bool, filtrator: string);
}

const dummy_ci = conn_info($cid=dummy_cid, $ts=current_time());

redef record Cluster::Bench::TestStats += {
	received: count &optional;
};

##############
### WORKER ###
##############
@if ( Cluster::local_node_type() == Cluster::WORKER )
global total = 0;
event do_publish_tick() {
	local i = 0;

	while ( i < publishes_per_tick ) {

		if ( total >= total_publishes )
			return;

		++i;
		++total;
		local subx = Cluster::node + cat(total) + cat(i);
		Cluster::publish(Cluster::proxy_topic, Scan::potential_scanner, dummy_ci, T, F, "filtrator" + subx + cat(total));
	}
}

event tick() {
	if ( total >= total_publishes ) {
		Cluster::Bench::publish_test_done();
		return;
	}

	event do_publish_tick();

	schedule tick_interval { tick() };
}

event Cluster::Bench::test_start() {
	event tick();
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
global received = 0;
global last_tick_received = 0;

# Wait to observe all test_done() from workers before shutting down.
global workers_test_done_seen = 0;

event Cluster::Bench::test_done(name: string, stats: Cluster::Bench::TestStats) {
    if (  Cluster::nodes[name]$node_type == Cluster::WORKER )
        ++workers_test_done_seen;

    if ( workers_test_done_seen == Cluster::Bench::workers_total )
	Cluster::Bench::publish_test_done();
}

hook Cluster::Bench::prepare_test_done(stats: Cluster::Bench::TestStats) {
	stats$received = received;
}

event potential_scanner(ci: conn_info, established: bool, reverse: bool, filtrator: string) {
	++received;
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
event Cluster::Bench::test_done(name: string, stats: Cluster::Bench::TestStats) {
	# Loggers do not do anything, just finish with the first node to complete.
	Cluster::Bench::publish_test_done();
}
@endif

###############
### MANAGER ###
###############
@if ( Cluster::local_node_type() == Cluster::MANAGER )
event Cluster::Bench::test_terminate() {
	# Sum up all received values.
	local sum_received = 0;
	for ( name, stats in Cluster::Bench::node_stats ) {
		if (stats?$received ) {
			print name, stats$received;
			sum_received += stats$received;
		}

	}

	local sum_expected = total_publishes * Cluster::Bench::workers_total * Cluster::Bench::proxies_total;

	local success = sum_received == sum_expected;
	local msg = fmt("Received %s, expected %s", sum_received, sum_expected);

	local result = Cluster::Bench::Result($success=success, $message=msg);
	Cluster::Bench::node_stats[Cluster::node]$result = result;
}
@endif
