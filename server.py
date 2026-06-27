"""
Map of Science — Databricks backend
"""

import os
import pathlib
import logging
from functools import lru_cache

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from databricks.sql import connect as databricks_connect
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APP_DIR = pathlib.Path(__file__).parent

DATABRICKS_HOST      = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN     = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_connection():
    return databricks_connect(
        server_hostname=DATABRICKS_HOST.replace("https://", "").rstrip("/"),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
        _socket_timeout=60,
    )


@lru_cache(maxsize=1)
def _load_graph():
    logger.info("Opening Databricks connection...")
    with get_connection() as conn:
        logger.info("Connection established. Querying top 500 nodes per field...")
        with conn.cursor() as cur:

            # Top 500 nodes per field by pagerank
            cur.execute("""
                SELECT id, title, field, domain, pagerank_score
                FROM (
                    SELECT id, title, field, domain, pagerank_score,
                           ROW_NUMBER() OVER (PARTITION BY field ORDER BY pagerank_score DESC) AS rn
                    FROM default.openalex_top_001_nodes
                ) ranked
                WHERE rn <= 500
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            nodes = [dict(zip(cols, r)) for r in rows]
            logger.info(f"Loaded {len(nodes)} nodes. Querying induced edges...")

            # Only edges where both endpoints are in our capped node set
            node_id_set = {n["id"] for n in nodes}
            cur.execute("""
                SELECT e.source, e.target
                FROM default.openalex_top_001_edges e
                JOIN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY field ORDER BY pagerank_score DESC) AS rn
                        FROM default.openalex_top_001_nodes
                    ) r WHERE r.rn <= 500
                ) capped_src ON e.source = capped_src.id
                JOIN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY field ORDER BY pagerank_score DESC) AS rn
                        FROM default.openalex_top_001_nodes
                    ) r WHERE r.rn <= 500
                ) capped_tgt ON e.target = capped_tgt.id
            """)
            rows = cur.fetchall()
            # Also filter in Python for safety
            edges = [
                {"source": r[0], "target": r[1]}
                for r in rows
                if r[0] in node_id_set and r[1] in node_id_set
            ]
            logger.info(f"Loaded {len(edges)} edges.")

    return nodes, edges


@app.get("/api/graph")
def graph():
    try:
        nodes, edges = _load_graph()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    id_to_idx = {n["id"]: i for i, n in enumerate(nodes)}

    edge_list = []
    for e in edges:
        s = id_to_idx.get(e["source"])
        t = id_to_idx.get(e["target"])
        if s is not None and t is not None and s != t:
            edge_list.append({"s": s, "t": t})

    return {
        "nodes": [
            {
                "id":       n["id"],
                "title":    n["title"] or n["id"],
                "field":    n["field"] or "Unknown",
                "domain":   n["domain"] or "",
                "pagerank": float(n["pagerank_score"] or 0),
            }
            for n in nodes
        ],
        "edges": edge_list,
    }


@app.get("/")
def root():
    return FileResponse(str(APP_DIR / "map-of-science.html"))

app.mount("/static", StaticFiles(directory=str(APP_DIR)), name="static")