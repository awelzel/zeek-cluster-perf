

module Cluster::Bench::WebSocket;

export {
	global ping: event(i: count, who: string);
	global bye: event(who: string);

	global listen_host = "127.0.0.1";
	global listen_port = 1111/tcp;
	global url = fmt("ws://%s:%s/v1/messages/json", listen_host, port_to_count(listen_port));
}
