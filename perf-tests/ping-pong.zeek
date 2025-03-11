# worker-1 starts, sends ping to proxy-1
# proxy-1 sends to worker-2
# worker-2 to proxy-2 and so forth.
@load cluster-bench-common


module PingPong;

export {
	global ping: event(total: count, widx: count, pidx: count);

	global nodes_with_type: function(node_type: Cluster::NodeType): vector of Cluster::NamedNode &redef;
}

redef record Cluster::Bench::TestStats += {
	received: count &optional;
};


# Hack: Make nodes_with_type available to PingPong.
module Cluster;
redef PingPong::nodes_with_type = Cluster::nodes_with_type;


global workers: vector of Cluster::NamedNode;
global proxies: vector of Cluster::NamedNode;

global received = 0;
global last_tick_received = 0;

event zeek_init()
	{
	workers = Cluster::nodes_with_type(Cluster::WORKER);
	proxies = Cluster::nodes_with_type(Cluster::PROXY);
	}

hook Cluster::Bench::prepare_test_done(stats: Cluster::Bench::TestStats) {
	stats$received = received;
}
##############
### WORKER ###
##############
@if ( Cluster::local_node_type() == Cluster::WORKER )
global done = F;

event PingPong::ping(total: count, widx: count, pidx: count) {
	if ( done )
		return;

	++received;
	++total;
	# print "got ping", total, widx, pidx;
	if ( workers[widx]$name != Cluster::node )
		Reporter::fatal(fmt("unexpected ping %s %s", widx, pidx));

	widx = (widx + 1) % |workers|;
	local topic = Cluster::node_topic(proxies[pidx]$name);
	# print "sending", topic, total, widx, pidx;
	Cluster::publish(topic, PingPong::ping, total, widx, pidx);

	if ( total >= total_publishes ) {
		print "DONE";
		done = T;
		Cluster::Bench::publish_test_done();
	}
}

event Cluster::Bench::test_start() {
	if ( Cluster::node == workers[0]$name ) {
		print "GO GO GO";
		event PingPong::ping(0, 0, 0);
	}
}
@endif

#############
### PROXY ###
#############
@if ( Cluster::local_node_type() == Cluster::PROXY )
event PingPong::ping(total: count, widx: count, pidx: count) {
	++received;
	++total;
	# print "got ping", total, widx, pidx;
	if ( proxies[pidx]$name != Cluster::node )
		Reporter::fatal(fmt("unexpected ping %s %s", widx, pidx));

	pidx = (pidx + 1) % |proxies|;
	local topic = Cluster::node_topic(workers[widx]$name);
	# print "sending", topic, total, widx, pidx;
	Cluster::publish(topic, PingPong::ping, total, widx, pidx);

	if ( total >= total_publishes )
		Cluster::Bench::publish_test_done();
}


hook Cluster::Bench::stats_tick(now_ts: double, last_ts: double, td: double) {

	local diff = received - last_tick_received;
	local per_second = diff / td;

	print fmt("events per second: %.3f (%s / %s) total %s", per_second, diff, td, received);

	last_tick_received = received;
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
			# print "RECV", name, stats$received;
			sum_received += stats$received;
		}

	}

	local sum_expected = total_publishes + |proxies| + |workers| -1;

	local success = sum_received == sum_expected;
	local msg = fmt("Received %s, expected %s", sum_received, sum_expected);

	local result = Cluster::Bench::Result($success=success, $message=msg);
	Cluster::Bench::node_stats[Cluster::node]$result = result;
}
@endif
