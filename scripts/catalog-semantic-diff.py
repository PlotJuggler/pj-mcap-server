#!/usr/bin/env python3
"""Semantic catalog diff: rowid/ordering-independent equivalence check.

Catalog builds are NOT byte-reproducible across runs (topic/schema dictionary
ids assign in file-completion order), so raw-row or byte comparison of two
builds is meaningless. This decodes each DB through its own id dictionaries.

Compared per file: dims+filename identity, etag, size, last_modified_ns,
start/end times, chunk_count, has_error, {(topic, schema, encoding): count},
and effective tags. Plus the full failure set as (s3_key, error_text).
Intentionally ignored: cataloged_at_ns, build_metadata, all rowids and
topic_sets fingerprints (run-order dependent by design).

Usage: catalog-semantic-diff.py A.db B.db   (exit 0 identical / 1 different)
Run against quiescent DBs (no live builder writing them).
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "mcap_catalog"))
from mcap_catalog_builder.varint import decode_counts_blob  # noqa: E402


def canon(path):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        c.execute("BEGIN")  # snapshot: one consistent read transaction
        topics = dict(c.execute("SELECT id, name FROM topic_names"))
        schemas = {r[0]: (r[1], r[2])
                   for r in c.execute("SELECT id, name, encoding FROM schemas")}
        members: dict = {}
        for sid, tid, scid in c.execute(
                "SELECT set_id, topic_id, schema_id FROM topic_set_members "
                "ORDER BY set_id, topic_id"):
            members.setdefault(sid, []).append((tid, scid))
        tags: dict = {}
        for table in ("tags_embedded", "tags_override"):
            for fid, k, v in c.execute(f"SELECT file_id, key, value FROM {table}"):
                tags.setdefault(fid, set()).add((table, k, v))
        out = {}
        for r in c.execute("""
            SELECT cu.name, si.name, ro.name, so.name, f.date, f.filename,
                   f.etag, f.size_bytes, f.last_modified_ns, f.start_time_ns,
                   f.end_time_ns, f.chunk_count, f.has_error, f.topic_set_id,
                   f.topic_counts, f.id
            FROM files f
            JOIN customers cu ON cu.id=f.customer_id JOIN sites si ON si.id=f.site_id
            JOIN robots ro ON ro.id=f.robot_id JOIN sources so ON so.id=f.source_id"""):
            counts = decode_counts_blob(r[14])
            mem = members.get(r[13], [])
            if len(counts) != len(mem):
                print(f"CORRUPT: member/count arity mismatch for {r[:6]}")
                sys.exit(2)
            topicmap = frozenset(
                (topics[tid], schemas[scid][0], schemas[scid][1], n)
                for (tid, scid), n in zip(mem, counts))
            out[tuple(r[:6])] = (*r[6:13], topicmap,
                                 frozenset(tags.get(r[15], set())))
        fails = {(r[0], r[1]) for r in
                 c.execute("SELECT s3_key, error_text FROM catalog_failures")}
        return out, fails
    finally:
        c.close()


fa, xa = canon(sys.argv[1])
fb, xb = canon(sys.argv[2])
diff = [k for k in set(fa) & set(fb) if fa[k] != fb[k]]
only_a, only_b = set(fa) - set(fb), set(fb) - set(fa)
print(f"files: A={len(fa)} B={len(fb)} only-A={len(only_a)} "
      f"only-B={len(only_b)} value-diffs={len(diff)} failures-equal={xa == xb}")
for k in [*only_a, *only_b, *diff][:5]:
    print(" differs:", k)
if xa != xb:
    for row in list(xa ^ xb)[:5]:
        print(" failure-differs:", row)
identical = not (only_a or only_b or diff) and xa == xb
print("VERDICT:", "SEMANTICALLY IDENTICAL" if identical else "DIFFERENT")
sys.exit(0 if identical else 1)
