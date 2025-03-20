@load cluster-bench-common

module MyLog;

export {
	redef enum Log::ID += { LOG1, LOG2, LOG3, LOG4, LOG5 };

	type Info: record {
		ts: time &log;
		delta: interval &log;
		cid: conn_id &log;
		cid2: conn_id &log;
		msg: string &log;
	};
}

event zeek_init()
	{
	Log::create_stream(LOG1, [$columns=Info, $path="mylog1"]);
	Log::create_stream(LOG2, [$columns=Info, $path="mylog2"]);
	Log::create_stream(LOG3, [$columns=Info, $path="mylog3"]);
	Log::create_stream(LOG4, [$columns=Info, $path="mylog4"]);
	Log::create_stream(LOG5, [$columns=Info, $path="mylog5"]);
	}


redef record Cluster::Bench::TestStats += {
	received: count &optional;
};

##############
### WORKER ###
##############
@if ( Cluster::local_node_type() == Cluster::WORKER )
global start_time = current_time();
global total = 0;
event do_log_tick() {
	local i = 0;

	while ( i < publishes_per_tick ) {

		if ( total >= total_publishes )
			return;

		++i;
		++total;

		local now = current_time();
		local rec = Info(
			$ts=now,
			$delta=now - start_time,
			$cid=dummy_cid,
			$cid2=dummy_cid,
			$msg=fmt("%s %s", Cluster::node, total),
		);

		Log::write(LOG1, rec);
		Log::write(LOG2, rec);
		Log::write(LOG3, rec);
		Log::write(LOG4, rec);
		Log::write(LOG5, rec);
	}
}

event tick() {
	if ( total >= total_publishes ) {
		Log::flush(LOG1);
		Log::flush(LOG2);
		Log::flush(LOG3);
		Log::flush(LOG4);
		Log::flush(LOG5);
		Cluster::Bench::publish_test_done();
		return;
	}

	event do_log_tick();

	schedule tick_interval { tick() };
}

event Cluster::Bench::test_start() {
	event tick();
}

global last_total = 0;
hook Cluster::Bench::stats_tick(now_ts: double, last_ts: double, td: double) {
	local diff = total - last_total;
	local per_second = diff / td;

	print fmt("log writes per second: %.3f (%s / %s) total %s", per_second, diff, td, total);
	last_total = total;
}
@endif

#############
### PROXY ###
#############
@if ( Cluster::local_node_type() == Cluster::PROXY )
global last_tick_received = 0;

# Wait to observe all test_done() from workers before shutting down.
global loggers_test_done_seen = 0;

event Cluster::Bench::test_done(name: string, stats: Cluster::Bench::TestStats) {
	if (  Cluster::nodes[name]$node_type == Cluster::LOGGER )
		++loggers_test_done_seen;

	if ( loggers_test_done_seen == Cluster::Bench::loggers_total ) {
		print "CHECK LOGGER DIRS?";
		Cluster::Bench::publish_test_done();
	}
}
@endif

global workers_test_done_seen = 0;
@if ( Cluster::local_node_type() == Cluster::LOGGER )
event Cluster::Bench::test_done(name: string, stats: Cluster::Bench::TestStats) {
	if (  Cluster::nodes[name]$node_type == Cluster::WORKER )
		++workers_test_done_seen;

	if ( workers_test_done_seen == Cluster::Bench::workers_total ) {
		print "DONE", "FLUSH";
		Log::flush(LOG1);
		Log::flush(LOG2);
		Log::flush(LOG3);
		Log::flush(LOG4);
		Log::flush(LOG5);
		Cluster::Bench::publish_test_done();
	}
}

@endif

###############
### MANAGER ###
###############
@if ( Cluster::local_node_type() == Cluster::MANAGER )
event Cluster::Bench::test_terminate() {
	# Sum up all received values.
	print "CHECK LOGGER DIRECTORY!";

	local sum_received = 0;
	local sum_expected = 0;
	local success = sum_received == sum_expected;
	local msg = fmt("Received %s, expected %s", sum_received, sum_expected);

	local result = Cluster::Bench::Result($success=success, $message=msg);
	Cluster::Bench::node_stats[Cluster::node]$result = result;
}
@endif
