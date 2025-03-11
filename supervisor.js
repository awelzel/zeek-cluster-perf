/**
 * A minimal Zeek cluster supervisor.
 */
const fs = require("node:fs");
const util = require("node:util");
const path = require("node:path");
const process = require("node:process");
const { spawn } = require("node:child_process");
const yaml = require("yaml");

const Binaries = {
  zeek: typeof zeek !== "undefined" ? process.execPath : "zeek",
  taskset: "/usr/bin/taskset",
};

const NodeType = {
  Manager: "Cluster::MANAGER",
  Worker: "Cluster::WORKER",
  Proxy: "Cluster::PROXY",
  Logger: "Cluster::LOGGER",
};

const writeClusterLayout = (nodes, path) => {
  let content = "redef Cluster::manager_is_logger = F;\n\n";
  content += "redef Cluster::nodes += {";
  for (const node of nodes) {
    let ca = node.clusterArgs;
    content += "\n";
    content += `\t["${node.name}"] = [$node_type=${node.type}, $ip=${ca.host}`;
    if (ca?.port) {
      content += `, $p=${ca.port}`;
    }
    if (ca?.manager) {
      content += `, $manager="${ca.manager}"`;
    }
    content += "],";
  }
  content += "\n};\n";

  fs.writeFileSync(path, content);
};

class SpawnArgs {
  constructor(command, args, options) {
    this.command = command;
    this.args = args;

    // Options passed to child_process.spawn().
    // Contains env, stdio, etc.
    this.options = options;
  }
}

/**
 * Cluster (broker specific) configuration of a node.
 */
class ClusterArgs {
  constructor(host, port, manager) {
    this.host = host;
    this.port = port;
    this.manager = manager;
  }
}

/**
 * A node in the cluster. Not necessarily a Zeek node.
 */
class Node {
  constructor(name, type, clusterArgs, spawnArgs) {
    this.name = name;
    this.type = type;
    this.clusterArgs = clusterArgs;
    this.spawnArgs = spawnArgs;
  }
}

/**
 * Create a Zeek node that can be starteds.
 *
 * @param {*} cfg
 * @param {*} cfgNode
 * @param {*} type
 * @param {*} name
 * @returns
 */
const makeZeekNode = (cfg, cfgNode, type, name, manager) => {
  let defaultHost = cfg?.zeek?.cluster?.default?.host;
  let defaultCpu = cfg?.zeek?.cluster?.default?.cpu;
  let defaultInterface = cfg?.zeek?.cluster?.default?.interface;
  let ignoreChecksums = cfg?.zeek?.cluster?.default?.ignore_checksums;
  let defaultScripts = cfg?.zeek?.cluster?.default?.scripts || [];
  let spoolDir = cfg?.zeek?.cluster?.config?.spool_dir || ".";

  let host = cfgNode.host || defaultHost;
  let cwd = cfgNode.cwd || path.join(spoolDir, name);

  let env = Object.assign({}, process.env);
  env["CLUSTER_NODE"] = name;
  env["ZEEK_DEFAULT_CONNECT_RETRY"] = 1;

  let spawnOptions = {
    env: env,
    stdio: ["ignore", "pipe", "pipe"],
    cwd: cwd,
  };

  let command = Binaries.zeek;
  let args = [];

  // Worker specific logic.
  if (type == NodeType.Worker) {
    let interface = cfgNode.interface || defaultInterface;
    args = args.concat(["-i", interface]);

    if (ignoreChecksums) args = args.concat(["-C"]);
  }

  // Handle scripts as args.
  args = args.concat(defaultScripts);
  if (cfgNode.scripts) {
    args = args.concat(cfgNode.scripts);
  }

  if (process.env.ZEEK_EXTRA_SCRIPTS) {
    let s = process.env.ZEEK_EXTRA_SCRIPTS;
    let extra_args = s.replace(/,/g, " ").split(/ +/);
    args = args.concat(extra_args);
  }

  let cpu = "";
  if (defaultCpu !== undefined && defaultCpu !== null) {
    cpu = `${defaultCpu}`;
  }

  if (cfgNode?.cpu !== undefined && cfgNode?.cpu !== null) {
    cpu = `${cfgNode.cpu}`;
  }

  if (cpu) {
    command = Binaries.taskset;
    args = ["-c", cpu, Binaries.zeek].concat(args);
  }

  let clusterArgs = new ClusterArgs(host, cfgNode.port, manager);
  let spawnArgs = new SpawnArgs(command, args, spawnOptions);
  return new Node(name, type, clusterArgs, spawnArgs);
};

class Supervisor {
  constructor(clusterLayoutPath) {
    this.clusterLayoutPath = clusterLayoutPath;
    this.nodes = {};
  }

  launchNode(node) {
    let sa = node.spawnArgs;
    console.log(
      `Spawning ${node.name}: ${sa.command} ${JSON.stringify(sa.args)}`
    );

    // Prepare the working directory, linking cluster-layout.zeek into it.
    fs.mkdirSync(sa.options.cwd, { recursive: true });
    fs.rmSync(`${sa.options.cwd}/cluster-layout.zeek`, { force: true });
    fs.symlinkSync(
      this.clusterLayoutPath,
      `${sa.options.cwd}/cluster-layout.zeek`
    );

    let proc = spawn(sa.command, sa.args, sa.options);

    let log = (data) => {
      let lines = data.toString().split("\n");
      for (var i = 0; i < lines.length; i++) {
        let line = lines[i].trim();
        if (i == lines.length - i && line == "") {
          break;
        }

        console.log(`${node.name}: ${line}`);
      }
    };

    proc.stdout.on("data", log);
    proc.stderr.on("data", log);

    proc.on("error", (err) => {
      console.error(`Process ${node.name} caused an error ${err}`);
    });

    proc.on("exit", (code, signal) => {
      if (code != 0 || signal !== null)
        console.error(
          `Process ${node.name} exited with code=${code} signal=${signal}`
        );
    });

    proc.on("close", (code, signal) => {
      if (code != 0 || signal != null)
        console.error(
          `Process ${node.name} closed with code=${code} signal=${signal}`
        );
    });
  }
}

/**
 * Entry point.
 */
const main = () => {
  const cfgFile = process.env.ZEEK_CLUSTER_CONFIG || "cluster-config.yaml";
  let cfg = yaml.parse(fs.readFileSync(cfgFile, "utf8"));

  let autogenDir =
    cfg?.zeek?.cluster?.config?.autogen_dir || "tmp/auto-generated";
  fs.mkdirSync(autogenDir, { recursive: true });

  let nodes = [];

  nodes.push(
    makeZeekNode(cfg, cfg.zeek.cluster.manager, NodeType.Manager, "manager")
  );

  let loggerNum = 1;
  for (const logger of cfg.zeek.cluster.loggers) {
    let name = "";

    if (logger?.name && logger.name) {
      name = `${logger.name}`;
    } else {
      name = `logger-${loggerNum}`;
      ++loggerNum;
    }

    nodes.push(makeZeekNode(cfg, logger, NodeType.Logger, name, "manager"));
  }

  let proxyNum = 1;
  for (const proxy of cfg.zeek.cluster.proxies) {
    let name = "";

    if (proxy?.name && proxy.name) {
      name = `${proxy.name}`;
    } else {
      name = `proxy-${proxyNum}`;
      ++proxyNum;
    }
    nodes.push(makeZeekNode(cfg, proxy, NodeType.Proxy, name, "manager"));
  }

  let workerNum = 1;
  for (const worker of cfg.zeek.cluster.workers) {
    let name = "";

    if (worker?.name && worker.name) {
      name = `${worker.name}`;
    } else {
      name = `worker-${workerNum}`;
      ++workerNum;
    }

    nodes.push(makeZeekNode(cfg, worker, NodeType.Worker, name, "manager"));
  }

  let clusterLayoutPath = path.resolve(
    path.join(autogenDir, "cluster-layout.zeek")
  );
  writeClusterLayout(nodes, clusterLayoutPath);

  let s = new Supervisor(clusterLayoutPath);

  for (const n of nodes) {
    s.launchNode(n);
  }
};

if (typeof zeek !== "undefined") {
  zeek.on("zeek_init", () => {
    main();
  });
} else {
  console.warn("Running without zeek!");
  main();
}
