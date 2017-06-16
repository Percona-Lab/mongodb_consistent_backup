// A script for failure testing the oplog tailer.
// Kills running mongodb_consistent_backup.Oplog.Tailer.TailThread threads.
//
// Usage:
// $ mongo --shell /path/to/mongodb_consistent_backup/scripts/kill_tailer.thread.js
// > mcbKillTailer()
// ...
function mcbKillTailer() {
	return db.currentOp().inprog.forEach(
		function(x) {
			if (x.originatingCommand && x.originatingCommand.comment && x.originatingCommand.comment.startsWith("TailThread:run;")) {
				printjson(x);
				db.killOp(x.opid);
				print("Killed!");
			}
		}
	);
}
