#!/usr/bin/env python3
"""Run the Java Relive protocol probe with bounded local resources and same-run sampling."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jar", type=Path)
    parser.add_argument("classes", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("state", type=Path)
    parser.add_argument("--maintenance", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    args.state.parent.mkdir(parents=True, exist_ok=True)
    if args.state.exists():
        raise ValueError("State must be fresh")
    if shutil.disk_usage(args.state.parent).free < 20 * 1024**3:
        raise RuntimeError("Less than 20 GiB free")
    report = {
        "jar_sha256": hashlib.sha256(args.jar.read_bytes()).hexdigest(),
        "probe_class_sha256": hashlib.sha256((args.classes / "org/relive/PublicationProbe.class").read_bytes()).hexdigest(),
        "maintenance": args.maintenance,
        "heap": "256m per server; 128m driver",
        "cache_condition": "fresh index; OS page cache not flushed",
        "timeout_seconds": 120,
        "state": str(args.state.resolve()),
        "status": "failed",
    }
    (args.output / "configuration.json").write_text(json.dumps(report, indent=2) + "\n")
    processes, streams, samples = [], [], []
    try:
        addresses = []
        for index in range(2):
            log = args.output / f"server-{index}.log"
            stream = log.open("w")
            streams.append(stream)
            process = subprocess.Popen(["java", "-Xmx256m", "-Drelive.memtable.bytes=262144",
                                        "-jar", str(args.jar.resolve()), str(args.state.resolve()), "0"],
                                       stdout=stream, stderr=subprocess.STDOUT)
            processes.append(process)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                ready = [line[10:] for line in log.read_text().splitlines() if line.startswith("LISTENING ")]
                if ready:
                    addresses.append(ready[0])
                    break
                if process.poll() is not None:
                    raise RuntimeError(log.read_text())
                time.sleep(0.02)
            else:
                raise TimeoutError("Server startup exceeded 10 seconds")
        stream = (args.output / "driver.log").open("w")
        streams.append(stream)
        classpath = os.pathsep.join([str(args.jar.resolve()), str(args.classes.resolve())])
        command = ["java", "-Xmx128m", "-cp", classpath, "org.relive.PublicationProbe", *addresses,
                   str((args.output / "probe.json").resolve()), str(args.maintenance).lower()]
        driver = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        processes.append(driver)
        start = time.monotonic()
        ticks = os.sysconf("SC_CLK_TCK")
        while driver.poll() is None:
            elapsed = time.monotonic() - start
            if elapsed > 120:
                raise TimeoutError("Probe execution exceeded 120 seconds")
            pss, cpu = 0, 0.0
            for process in processes[:2]:
                if process.poll() is not None:
                    raise RuntimeError("Server exited before probe completion")
                for line in Path(f"/proc/{process.pid}/smaps_rollup").read_text().splitlines():
                    if line.startswith("Pss:"):
                        pss += int(line.split()[1]) * 1024
                fields = Path(f"/proc/{process.pid}/stat").read_text().split()
                cpu += (int(fields[13]) + int(fields[14])) / ticks
            disk = 0
            for path in args.state.glob("*"):
                try:
                    if path.is_file():
                        disk += path.stat().st_size
                except FileNotFoundError:
                    pass  # Maintenance may unlink an obsolete file during this non-atomic sample.
            if disk > 1024**3:
                raise RuntimeError("Probe state exceeded 1 GiB")
            samples.append({"elapsed_seconds": elapsed, "server_pss_bytes": pss,
                            "server_cpu_seconds": cpu, "state_bytes": disk})
            time.sleep(0.1)
        if driver.returncode != 0:
            raise RuntimeError((args.output / "driver.log").read_text())
        report.update(status="passed", peak_sampled_server_pss_bytes=max(x["server_pss_bytes"] for x in samples),
                      server_cpu_seconds=samples[-1]["server_cpu_seconds"],
                      peak_sampled_state_bytes=max(x["state_bytes"] for x in samples),
                      journal_bytes=(args.state / "journal").stat().st_size)
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for stream in streams:
            stream.close()
        (args.output / "resources.json").write_text(json.dumps(samples, indent=2) + "\n")
        (args.output / "run.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
